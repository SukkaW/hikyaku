//go:build linux

// Command netsim emulates a network path between two network namespaces, so the
// kernel's own TCP and QUIC stacks see real delay, random loss and a bandwidth cap.
//
// It creates the namespaces hk-client (10.200.0.1) and hk-server (10.200.0.2), each
// with a TUN device, and forwards every packet between them. It needs root (or
// CAP_NET_ADMIN and CAP_SYS_ADMIN with /dev/net/tun). Processes started with
// `ip netns exec hk-client ...` or `ip netns exec hk-server ...` use the path until
// netsim exits, which removes both namespaces.
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

const (
	clientNS   = "hk-client"
	serverNS   = "hk-server"
	clientAddr = "10.200.0.1"
	serverAddr = "10.200.0.2"
	mtu        = 1500
)

func main() {
	rtt := flag.Duration("rtt", 620*time.Millisecond, "round-trip delay, split evenly between the two directions")
	loss := flag.Float64("loss", 0, "round-trip packet loss in percent, spread over both directions")
	rate := flag.Float64("rate", 0, "bandwidth cap in Mbit/s for each direction (0: none)")
	queue := flag.Duration("queue", 250*time.Millisecond, "drop-tail queue in front of the bandwidth cap")
	seed := flag.Uint64("seed", 1, "seed for the loss pattern")
	flag.Parse()

	// Losing L of round trips means each direction drops p, where (1-p)^2 = 1-L.
	perDirection := 1 - math.Sqrt(1-*loss/100)
	link := func(stream uint64) *link {
		return &link{
			delay: *rtt / 2,
			loss:  perDirection,
			rate:  *rate * 1e6 / 8,
			queue: *queue,
			rng:   rand.New(rand.NewPCG(*seed, stream)),
		}
	}

	if err := setup(); err != nil {
		teardown()
		log.Fatal(err)
	}
	clientTUN, err := openTUN(clientNS, "hkc")
	if err == nil {
		var serverTUN *os.File
		if serverTUN, err = openTUN(serverNS, "hks"); err == nil {
			if err = configure(clientNS, "hkc", clientAddr, serverAddr); err == nil {
				err = configure(serverNS, "hks", serverAddr, clientAddr)
			}
			if err == nil {
				go link(1).forward(clientTUN, serverTUN)
				go link(2).forward(serverTUN, clientTUN)
			}
		}
	}
	if err != nil {
		teardown()
		log.Fatal(err)
	}
	fmt.Printf("netsim: rtt %s, %.2f%% loss per round trip (%.2f%% each way), %s each way, %s queue; ready\n",
		*rtt, *loss, perDirection*100, rateString(*rate), *queue)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	teardown()
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

func setup() error {
	teardown()
	for _, ns := range []string{clientNS, serverNS} {
		if err := run("netns", "add", ns); err != nil {
			return err
		}
		if err := run("-n", ns, "link", "set", "lo", "up"); err != nil {
			return err
		}
	}
	return nil
}

func teardown() {
	for _, ns := range []string{clientNS, serverNS} {
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

// link is one direction of the path: random loss, then a bandwidth cap with a
// drop-tail queue, then a fixed propagation delay.
type link struct {
	delay time.Duration
	loss  float64
	rate  float64 // bytes per second; 0 means no cap
	queue time.Duration
	rng   *rand.Rand
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
	// Departure times only move forward and the delay is fixed, so packets come due
	// in arrival order and a FIFO is enough to schedule them.
	out := make(chan packet, 1<<16)
	go deliver(dst, out)
	var busyUntil time.Time
	for {
		buf := buffers.Get().(*[]byte)
		n, err := src.Read(*buf)
		if err != nil {
			log.Fatalf("read %s: %v", src.Name(), err)
		}
		if l.loss > 0 && l.rng.Float64() < l.loss {
			buffers.Put(buf)
			continue
		}
		now := time.Now()
		depart := now
		if l.rate > 0 {
			start := now
			if busyUntil.After(now) {
				start = busyUntil
			}
			if start.Sub(now) > l.queue {
				buffers.Put(buf)
				continue
			}
			busyUntil = start.Add(time.Duration(float64(n) / l.rate * float64(time.Second)))
			depart = busyUntil
		}
		out <- packet{data: (*buf)[:n], buf: buf, due: depart.Add(l.delay)}
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
