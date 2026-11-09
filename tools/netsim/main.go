//go:build linux

// Command netsim emulates a network path between two network namespaces, so the
// kernel's own TCP and QUIC stacks see real delay, random loss, bandwidth caps,
// outages, latency spikes and periodic pauses.
//
// It creates the namespaces hk-client (10.200.0.1) and hk-server (10.200.0.2), each
// with a TUN device, and forwards every packet between them; with -id N they are
// hk-client-N and hk-server-N on 10.200.N.0, so several paths can run at once. It
// needs root (or CAP_NET_ADMIN and CAP_SYS_ADMIN with /dev/net/tun). Processes started
// with `ip netns exec hk-client ...` or `ip netns exec hk-server ...` use the path
// until netsim exits, which removes both namespaces.
package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const mtu = 1500

// path names one emulated path's namespaces, devices and addresses.
type path struct {
	clientNS, serverNS, clientDev, serverDev, clientAddr, serverAddr string
}

func newPath(id int) path {
	if id == 0 {
		return path{"hk-client", "hk-server", "hkc", "hks", "10.200.0.1", "10.200.0.2"}
	}
	return path{
		fmt.Sprintf("hk-client-%d", id), fmt.Sprintf("hk-server-%d", id),
		fmt.Sprintf("hkc%d", id), fmt.Sprintf("hks%d", id),
		fmt.Sprintf("10.200.%d.1", id), fmt.Sprintf("10.200.%d.2", id),
	}
}

func main() {
	id := flag.Int("id", 0, "instance number from 0 to 255; each gets its own namespaces and addresses")
	rtt := flag.Duration("rtt", 620*time.Millisecond, "round-trip delay, split evenly between the two directions")
	loss := flag.Float64("loss", 0, "round-trip packet loss in percent, spread over both directions")
	downRate := flag.Float64("down-rate", 0, "bandwidth cap in Mbit/s from server to client (0: none)")
	upRate := flag.Float64("up-rate", 0, "bandwidth cap in Mbit/s from client to server (0: none)")
	queue := flag.Duration("queue", 250*time.Millisecond, "drop-tail queue in front of the bandwidth cap")
	fair := flag.Bool("fq", false, "give each connection its own queue of that length at the bandwidth cap and serve them in turn, like a fair-queueing bottleneck")
	outageEvery := flag.Duration("outage-every", 0, "mean time between outages that drop everything both ways (0: none)")
	outageFor := flag.Duration("outage-for", 0, "mean outage length")
	spikeEvery := flag.Duration("spike-every", 0, "mean time between latency spikes (0: none)")
	spikeFor := flag.Duration("spike-for", 0, "length of each latency spike")
	spikeMin := flag.Duration("spike-min", 0, "smallest extra round-trip delay during a spike")
	spikeMax := flag.Duration("spike-max", 0, "largest extra round-trip delay during a spike")
	pauseEvery := flag.Duration("pause-every", 0, "period of short pauses in which the path sends nothing (0: none)")
	pauseDown := flag.Duration("pause-down", 0, "pause length from server to client")
	pauseUp := flag.Duration("pause-up", 0, "pause length from client to server")
	seed := flag.Uint64("seed", 1, "seed for the loss, outage and spike patterns")
	flag.Parse()
	if *id < 0 || *id > 255 {
		log.Fatalf("-id %d is outside 0-255", *id)
	}
	p := newPath(*id)

	// Losing L of round trips means each direction drops p, where (1-p)^2 = 1-L.
	perDirection := 1 - math.Sqrt(1-*loss/100)
	epoch := time.Now()
	outages := newEvents(*seed, 3, *outageEvery,
		func(r *rand.Rand) time.Duration { return exponential(r, *outageFor) },
		func(*rand.Rand) time.Duration { return 1 })
	spikes := newEvents(*seed, 4, *spikeEvery,
		func(*rand.Rand) time.Duration { return *spikeFor },
		func(r *rand.Rand) time.Duration {
			return *spikeMin + time.Duration(r.Float64()*float64(*spikeMax-*spikeMin))
		})
	link := func(stream uint64, rate float64, pause time.Duration) *link {
		return &link{
			delay:      *rtt / 2,
			loss:       perDirection,
			rate:       rate * 1e6 / 8,
			queue:      *queue,
			fair:       *fair && rate > 0,
			rng:        rand.New(rand.NewPCG(*seed, stream)),
			outages:    outages,
			spikes:     spikes,
			epoch:      epoch,
			pauseEvery: *pauseEvery,
			pause:      pause,
		}
	}

	if err := setup(p); err != nil {
		teardown(p)
		log.Fatal(err)
	}
	clientTUN, err := openTUN(p.clientNS, p.clientDev)
	if err == nil {
		var serverTUN *os.File
		if serverTUN, err = openTUN(p.serverNS, p.serverDev); err == nil {
			if err = configure(p.clientNS, p.clientDev, p.clientAddr, p.serverAddr); err == nil {
				err = configure(p.serverNS, p.serverDev, p.serverAddr, p.clientAddr)
			}
			if err == nil {
				go link(1, *upRate, *pauseUp).forward(clientTUN, serverTUN)
				go link(2, *downRate, *pauseDown).forward(serverTUN, clientTUN)
			}
		}
	}
	if err != nil {
		teardown(p)
		log.Fatal(err)
	}
	fmt.Printf("netsim: rtt %s, %.4f%% loss per round trip (%.4f%% each way), down %s, up %s, %s queue",
		*rtt, *loss, perDirection*100, rateString(*downRate), rateString(*upRate), *queue)
	if *fair {
		fmt.Print(" per connection, served in turn")
	}
	if *outageEvery > 0 {
		fmt.Printf(", outages of ~%s every ~%s", *outageFor, *outageEvery)
	}
	if *spikeEvery > 0 {
		fmt.Printf(", +%s–%s spikes for %s every ~%s", *spikeMin, *spikeMax, *spikeFor, *spikeEvery)
	}
	if *pauseEvery > 0 {
		fmt.Printf(", pauses of %s down / %s up every %s", *pauseDown, *pauseUp, *pauseEvery)
	}
	fmt.Println("; ready")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	teardown(p)
}

func rateString(mbps float64) string {
	if mbps == 0 {
		return "no cap"
	}
	return fmt.Sprintf("%g Mbit/s", mbps)
}

func run(args ...string) error {
	if out, err := exec.Command("ip", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("ip %v: %v: %s", args, err, out)
	}
	return nil
}

func setup(p path) error {
	teardown(p)
	for _, ns := range []string{p.clientNS, p.serverNS} {
		if err := run("netns", "add", ns); err != nil {
			return err
		}
		if err := run("-n", ns, "link", "set", "lo", "up"); err != nil {
			return err
		}
	}
	return nil
}

func teardown(p path) {
	for _, ns := range []string{p.clientNS, p.serverNS} {
		exec.Command("ip", "netns", "del", ns).Run()
	}
}

func configure(ns, dev, local, peer string) error {
	if err := run("-n", ns, "addr", "add", local, "peer", peer, "dev", dev); err != nil {
		return err
	}
	return run("-n", ns, "link", "set", dev, "mtu", fmt.Sprint(mtu), "up")
}

// openTUN creates a TUN device inside the named namespace. The device lives as long
// as the returned file, which this process keeps while it forwards.
func openTUN(ns, name string) (*os.File, error) {
	runtime.LockOSThread()
	orig, err := os.Open("/proc/thread-self/ns/net")
	if err != nil {
		runtime.UnlockOSThread()
		return nil, err
	}
	defer orig.Close()
	target, err := os.Open("/run/netns/" + ns)
	if err != nil {
		runtime.UnlockOSThread()
		return nil, err
	}
	defer target.Close()
	if err := unix.Setns(int(target.Fd()), unix.CLONE_NEWNET); err != nil {
		runtime.UnlockOSThread()
		return nil, fmt.Errorf("enter %s: %w", ns, err)
	}
	f, openErr := createTUN(name)
	// If switching back fails, the thread stays locked so Go retires it instead of
	// reusing it in the wrong namespace.
	if err := unix.Setns(int(orig.Fd()), unix.CLONE_NEWNET); err != nil {
		return nil, fmt.Errorf("leave %s: %w", ns, err)
	}
	runtime.UnlockOSThread()
	return f, openErr
}

func createTUN(name string) (*os.File, error) {
	fd, err := unix.Open("/dev/net/tun", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	ifr, err := unix.NewIfreq(name)
	if err != nil {
		unix.Close(fd)
		return nil, err
	}
	ifr.SetUint16(unix.IFF_TUN | unix.IFF_NO_PI)
	if err := unix.IoctlIfreq(fd, unix.TUNSETIFF, ifr); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("TUNSETIFF %s: %w", name, err)
	}
	return os.NewFile(uintptr(fd), name), nil
}

// link is one direction of the path: outages and random loss, then a bandwidth cap
// with a drop-tail queue that also holds packets through pauses, then the
// propagation delay plus any latency spike.
type link struct {
	delay time.Duration
	loss  float64
	rate  float64 // bytes per second; 0 means no cap
	queue time.Duration
	fair  bool // one queue per connection instead of one shared queue
	rng   *rand.Rand

	outages, spikes *events // shared by both directions
	epoch           time.Time
	pauseEvery      time.Duration
	pause           time.Duration
}

// resume returns when the path can send again if t falls inside a pause.
func (l *link) resume(t time.Time) time.Time {
	if l.pauseEvery == 0 || l.pause == 0 {
		return t
	}
	if into := t.Sub(l.epoch) % l.pauseEvery; into < l.pause {
		return t.Add(l.pause - into)
	}
	return t
}

// events is an on/off process in wall-clock time with exponentially distributed
// gaps, such as outages or latency spikes. Its state is shared by both directions.
type events struct {
	mu      sync.Mutex
	rng     *rand.Rand
	gap     time.Duration // mean time between events; 0 disables them
	length  func(*rand.Rand) time.Duration
	size    func(*rand.Rand) time.Duration
	active  bool
	current time.Duration
	until   time.Time
}

func newEvents(seed, stream uint64, gap time.Duration, length, size func(*rand.Rand) time.Duration) *events {
	return &events{rng: rand.New(rand.NewPCG(seed, stream)), gap: gap, length: length, size: size}
}

func exponential(r *rand.Rand, mean time.Duration) time.Duration {
	return time.Duration(r.ExpFloat64() * float64(mean))
}

// at returns the size of the event in progress at t, or 0 between events.
func (e *events) at(t time.Time) time.Duration {
	if e.gap == 0 {
		return 0
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.until.IsZero() {
		e.until = t.Add(exponential(e.rng, e.gap))
	}
	for !t.Before(e.until) {
		if e.active {
			e.active = false
			e.until = e.until.Add(exponential(e.rng, e.gap))
		} else {
			e.active = true
			e.current = e.size(e.rng)
			e.until = e.until.Add(e.length(e.rng))
		}
	}
	if e.active {
		return e.current
	}
	return 0
}

type packet struct {
	data []byte
	buf  *[]byte
	due  time.Time
}

var buffers = sync.Pool{New: func() any {
	b := make([]byte, mtu+64)
	return &b
}}

func (l *link) forward(src, dst *os.File) {
	// Due times are kept in arrival order, even when a spike ends, so a FIFO is
	// enough to schedule them and the path never reorders packets.
	out := make(chan packet, 1<<16)
	go deliver(dst, out)
	if l.fair {
		l.forwardFair(src, out)
		return
	}
	var busyUntil, lastDue time.Time
	for {
		buf := buffers.Get().(*[]byte)
		n, err := src.Read(*buf)
		if err != nil {
			log.Fatalf("read %s: %v", src.Name(), err)
		}
		now := time.Now()
		if l.outages.at(now) > 0 || (l.loss > 0 && l.rng.Float64() < l.loss) {
			buffers.Put(buf)
			continue
		}
		start := now
		if busyUntil.After(start) {
			start = busyUntil
		}
		start = l.resume(start)
		depart := start
		if l.rate > 0 {
			if start.Sub(now) > l.queue {
				buffers.Put(buf)
				continue
			}
			busyUntil = start.Add(time.Duration(float64(n) / l.rate * float64(time.Second)))
			depart = busyUntil
		}
		due := depart.Add(l.delay + l.spikes.at(now)/2)
		if due.Before(lastDue) {
			due = lastDue
		}
		lastDue = due
		out <- packet{data: (*buf)[:n], buf: buf, due: due}
	}
}

// flow identifies a connection by protocol, addresses and ports; packets that are
// not IPv4 TCP or UDP share the zero flow.
type flow struct {
	proto            byte
	src, dst         [4]byte
	srcPort, dstPort uint16
}

func flowOf(b []byte) flow {
	if len(b) < 20 || b[0]>>4 != 4 {
		return flow{}
	}
	f := flow{proto: b[9], src: [4]byte(b[12:16]), dst: [4]byte(b[16:20])}
	if ihl := int(b[0]&0x0f) * 4; (f.proto == 6 || f.proto == 17) && len(b) >= ihl+4 {
		f.srcPort = uint16(b[ihl])<<8 | uint16(b[ihl+1])
		f.dstPort = uint16(b[ihl+2])<<8 | uint16(b[ihl+3])
	}
	return f
}

// forwardFair queues each connection's packets separately, dropping a packet that
// would take its own connection's queue past l.queue at the bandwidth cap, while a
// sender serves the queues in turn, one packet each. A busy connection then waits
// behind its own packets but not in front of other connections', as at a
// fair-queueing bottleneck such as fq_codel.
func (l *link) forwardFair(src *os.File, out chan<- packet) {
	var mu sync.Mutex
	queues := make(map[flow][]packet)
	queued := make(map[flow]int) // bytes waiting per connection
	var turns []flow             // connections with packets waiting, in serving order
	wake := make(chan struct{}, 1)
	limit := int(l.rate * l.queue.Seconds())
	go func() {
		var busyUntil, lastDue time.Time
		for {
			mu.Lock()
			if len(turns) == 0 {
				mu.Unlock()
				<-wake
				continue
			}
			f := turns[0]
			turns = turns[1:]
			p := queues[f][0]
			queues[f] = queues[f][1:]
			queued[f] -= len(p.data)
			if len(queues[f]) > 0 {
				turns = append(turns, f)
			} else {
				delete(queues, f)
				delete(queued, f)
			}
			mu.Unlock()
			now := time.Now()
			start := now
			if busyUntil.After(start) {
				start = busyUntil
			}
			start = l.resume(start)
			busyUntil = start.Add(time.Duration(float64(len(p.data)) / l.rate * float64(time.Second)))
			// One packet is on the wire at a time, so the next turn is chosen only once
			// this one has left, letting connections that wake meanwhile go next.
			time.Sleep(time.Until(busyUntil))
			due := busyUntil.Add(l.delay + l.spikes.at(now)/2)
			if due.Before(lastDue) {
				due = lastDue
			}
			lastDue = due
			p.due = due
			out <- p
		}
	}()
	for {
		buf := buffers.Get().(*[]byte)
		n, err := src.Read(*buf)
		if err != nil {
			log.Fatalf("read %s: %v", src.Name(), err)
		}
		if l.outages.at(time.Now()) > 0 || (l.loss > 0 && l.rng.Float64() < l.loss) {
			buffers.Put(buf)
			continue
		}
		f := flowOf((*buf)[:n])
		mu.Lock()
		if queued[f]+n > limit {
			mu.Unlock()
			buffers.Put(buf)
			continue
		}
		if len(queues[f]) == 0 {
			turns = append(turns, f)
		}
		queues[f] = append(queues[f], packet{data: (*buf)[:n], buf: buf})
		queued[f] += n
		mu.Unlock()
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

func deliver(dst *os.File, out <-chan packet) {
	for p := range out {
		if wait := time.Until(p.due); wait > 0 {
			time.Sleep(wait)
		}
		if _, err := dst.Write(p.data); err != nil {
			log.Printf("write %s: %v", dst.Name(), err)
		}
		buffers.Put(p.buf)
	}
}
