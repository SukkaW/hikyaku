package main

import (
	"errors"
	"io"
	"log"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
	"github.com/xtaci/smux"
)

const (
	dialTimeout  = 5 * time.Second
	minBackoff   = time.Second
	maxBackoff   = 5 * time.Second
	suspectAfter = 5 * time.Second
	healthCheck  = 500 * time.Millisecond
	carrierWait  = 10 * time.Second
	carrierPoll  = 50 * time.Millisecond
	pongTimeout  = 3 * time.Second
	slowMargin   = time.Second
	slowStrikes  = 3
	metricsEvery = 30 * time.Second
	outageAfter  = 3 * time.Second
	probeWait    = 5 * time.Second
)

var (
	errNoCarrier = errors.New("no carrier is ready")
	errBadPong   = errors.New("stream did not start with a pong")
)

type carrier struct {
	slot    int
	session *smux.Session
	conn    *proxy.Conn
	ready   time.Time
	rtt     atomic.Int64 // smoothed pong round trip in nanoseconds; 0 until the first pong

	mu         sync.Mutex
	unanswered map[*replayStream]struct{}
	waiting    map[*carrierStream]time.Time // streams still waiting for their pong
}

// pool opens carriers on demand, up to a fixed number of slots; an opened slot is
// nil while its carrier is down and keeps being redialed. Silent carriers wait on
// standby until their slot has a replacement.
type pool struct {
	server   string
	psk      []byte
	mu       sync.Mutex
	carriers []*carrier
	standby  []*carrier
	opened   int
	probing  atomic.Bool
}

func newPool(server string, psk []byte, carriers int) *pool {
	return &pool{server: server, psk: psk, carriers: make([]*carrier, carriers)}
}

// grow starts maintaining one more slot; the caller holds p.mu.
func (p *pool) grow() {
	if p.opened < len(p.carriers) {
		go p.maintain(p.opened)
		p.opened++
	}
}

func (p *pool) maintain(slot int) {
	var backoff time.Duration
	for {
		c, err := p.dial()
		if err != nil {
			log.Printf("carrier %d: %v", slot, err)
			backoff = min(max(2*backoff, minBackoff), maxBackoff)
			time.Sleep(backoff)
			continue
		}
		backoff = 0
		c.slot = slot
		p.set(slot, c)
		log.Printf("carrier %d: ready", slot)
		v := p.watch(c)
		silent := v == silent || v == noPong
		switch {
		case v == slow:
			p.toStandby(c)
			c.moveUnanswered()
			log.Printf("carrier %d: %s slower than the best other carrier for %d heartbeats after %s, standing by",
				slot, slowMargin, slowStrikes, c.lifetime())
			go p.retire(c)
		case silent && c.conn.Acked():
			p.toStandby(c)
			c.moveUnanswered()
			if v == noPong {
				log.Printf("carrier %d: pong overdue by %s after %s, standing by", slot, pongTimeout, c.lifetime())
			} else {
				log.Printf("carrier %d: silent for %s after %s, standing by", slot, suspectAfter, c.lifetime())
			}
			go p.retire(c)
		case silent:
			// Never acknowledged, so it has not proven it reaches the server; failing
			// its streams now lets apps retry instead of waiting on a dead standby.
			p.set(slot, nil)
			c.session.Close()
			log.Printf("carrier %d: no acknowledgement after %s, closing", slot, c.lifetime())
		default:
			p.set(slot, nil)
			log.Printf("carrier %d: closed after %s: %s", slot, c.lifetime(), c.closeReason())
		}
	}
}

func (p *pool) set(slot int, c *carrier) {
	p.mu.Lock()
	p.carriers[slot] = c
	p.mu.Unlock()
}

func (p *pool) toStandby(c *carrier) {
	p.mu.Lock()
	p.carriers[c.slot] = nil
	p.standby = append(p.standby, c)
	p.mu.Unlock()
}

func (p *pool) replaced(slot int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	c := p.carriers[slot]
	return c != nil && c.conn.Acked()
}

func (p *pool) dial() (*carrier, error) {
	conn, err := net.DialTimeout("tcp", p.server, dialTimeout)
	if err != nil {
		return nil, err
	}
	c, err := proxy.ClientHandshake(conn, p.psk)
	if err != nil {
		conn.Close()
		return nil, err
	}
	session, err := smux.Client(c, proxy.MuxConfig())
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &carrier{session: session, conn: c, ready: time.Now()}, nil
}

func (c *carrier) lifetime() time.Duration {
	return time.Since(c.ready).Round(time.Second)
}

// closeReason explains why the session ended. smux closes the connection itself
// on keepalive timeout, which reaches the reader as net.ErrClosed.
func (c *carrier) closeReason() string {
	if err := c.conn.ReadErr(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err.Error()
	}
	return "keepalive timeout"
}

func (c *carrier) silent() bool {
	return time.Since(c.conn.LastRead()) >= suspectAfter
}

func (c *carrier) pongOverdue() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, opened := range c.waiting {
		if time.Since(opened) >= pongTimeout {
			return true
		}
	}
	return false
}

type verdict int

const (
	closed verdict = iota
	silent
	noPong
	slow
)

// watch reports why a carrier should leave its slot: its session closed, it went
// silent, a stream's pong is overdue, or it stayed clearly slower than the best
// other carrier for slowStrikes heartbeats in a row.
func (p *pool) watch(c *carrier) verdict {
	tick := time.NewTicker(healthCheck)
	defer tick.Stop()
	var strikes int
	lastBeat, lastLog := time.Now(), time.Now()
	for {
		select {
		case <-c.session.CloseChan():
			return closed
		case <-tick.C:
			quiet := p.linkQuiet()
			if quiet {
				p.probeOutage()
			}
			// A link-wide outage looks the same on every carrier, so while it lasts only
			// acknowledged carriers go to standby, keeping their streams; new carriers
			// keep waiting for their acknowledgement and the other rules wait too.
			if c.silent() && (c.conn.Acked() || !quiet) {
				return silent
			}
			if quiet {
				strikes = 0
				continue
			}
			if c.pongOverdue() {
				return noPong
			}
			if time.Since(lastBeat) >= proxy.KeepAliveInterval {
				lastBeat = time.Now()
				if p.slower(c) {
					strikes++
				} else {
					strikes = 0
				}
				if strikes >= slowStrikes {
					return slow
				}
			}
			if time.Since(lastLog) >= metricsEvery {
				lastLog = time.Now()
				log.Printf("carrier %d: rtt %s, heartbeat lag %s, %d streams",
					c.slot, time.Duration(c.rtt.Load()).Round(time.Millisecond),
					c.conn.HeartbeatLag().Round(time.Millisecond), c.session.NumStreams())
			}
		}
	}
}

// slower reports whether c's delay exceeds the best other acknowledged carrier's by
// slowMargin. Delay is the larger of the heartbeat lag and how far the carrier's
// pong round trip sits above the fastest one; with no other carrier, nothing is slow.
func (p *pool) slower(c *carrier) bool {
	if !c.conn.Acked() {
		return false
	}
	p.mu.Lock()
	var others []*carrier
	for _, o := range p.carriers {
		if o != nil && o != c && o.conn.Acked() && !o.session.IsClosed() {
			others = append(others, o)
		}
	}
	p.mu.Unlock()
	if len(others) == 0 {
		return false
	}
	fastest := time.Duration(c.rtt.Load())
	for _, o := range others {
		if rtt := time.Duration(o.rtt.Load()); rtt != 0 && (fastest == 0 || rtt < fastest) {
			fastest = rtt
		}
	}
	delay := func(x *carrier) time.Duration {
		d := x.conn.HeartbeatLag()
		if rtt := time.Duration(x.rtt.Load()); rtt != 0 {
			d = max(d, rtt-fastest)
		}
		return d
	}
	best := delay(others[0])
	for _, o := range others[1:] {
		best = min(best, delay(o))
	}
	return delay(c)-best >= slowMargin
}

// linkQuiet reports a link-wide outage: some carriers have been acknowledged, yet
// none of them has received anything for outageAfter. A carrier that was never
// acknowledged has heard nothing real, so it counts neither way.
func (p *pool) linkQuiet() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	seen := false
	for _, c := range slices.Concat(p.carriers, p.standby) {
		if c == nil || !c.conn.Acked() || c.session.IsClosed() {
			continue
		}
		if time.Since(c.conn.LastRead()) < outageAfter {
			return false
		}
		seen = true
	}
	return seen
}

// probeOutage dials one fresh carrier at a time while the link stays quiet.
// Carriers whose path state was lost may never hear back, so the first acknowledged
// probe is what ends the outage; it then serves from standby until slot 0 has an
// acknowledged carrier again.
func (p *pool) probeOutage() {
	if !p.probing.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer p.probing.Store(false)
		log.Printf("link: nothing received for %s, probing", outageAfter)
		for p.linkQuiet() {
			c, err := p.dial()
			if err != nil {
				time.Sleep(minBackoff)
				continue
			}
			deadline := time.Now().Add(probeWait)
			for !c.conn.Acked() && !c.session.IsClosed() && time.Now().Before(deadline) {
				time.Sleep(carrierPoll)
			}
			if !c.conn.Acked() {
				c.session.Close()
				continue
			}
			p.mu.Lock()
			p.standby = append(p.standby, c)
			p.mu.Unlock()
			log.Printf("link: probe acknowledged, outage over")
			go p.retire(c)
			return
		}
	}()
}

// retire keeps a standby carrier for its existing streams, which survive a short
// outage, and closes it once its slot is replaced and those streams are done; if
// the server never returns, smux's keepalive timeout closes it instead.
func (p *pool) retire(c *carrier) {
	tick := time.NewTicker(healthCheck)
	defer tick.Stop()
	defer func() {
		p.mu.Lock()
		p.standby = slices.DeleteFunc(p.standby, func(s *carrier) bool { return s == c })
		p.mu.Unlock()
	}()
	for {
		select {
		case <-c.session.CloseChan():
			log.Printf("carrier %d: standby closed after %s: %s", c.slot, c.lifetime(), c.closeReason())
			return
		case <-tick.C:
			if c.session.NumStreams() == 0 && p.replaced(c.slot) {
				c.session.Close()
				log.Printf("carrier %d: standby retired after %s", c.slot, c.lifetime())
				return
			}
		}
	}
}

// openStream waits for a usable carrier when none is ready yet, such as right
// after start or an outage, so the stream rides the new carrier's first flight.
func (p *pool) openStream() (*carrier, *carrierStream, error) {
	deadline := time.Now().Add(carrierWait)
	for {
		if c := p.pick(); c != nil {
			s, err := c.session.OpenStream()
			if err != nil {
				return nil, nil, err
			}
			return c, c.newStream(s), nil
		}
		if time.Now().After(deadline) {
			return nil, nil, errNoCarrier
		}
		time.Sleep(carrierPoll)
	}
}

func (p *pool) pick() *carrier {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.opened == 0 {
		p.grow()
	}
	best := fewestStreams(p.carriers, func(c *carrier) bool { return c.conn.Acked() })
	if best != nil && (best.session.NumStreams() > 0 || !p.spare(best)) {
		// Keep an idle carrier in reserve: open another in the background as soon as
		// this one takes a stream, so a request that arrives during a long download
		// gets a carrier of its own instead of queueing behind it.
		p.grow()
	}
	if best == nil {
		// A standby carrier takes new streams only once it is receiving again; that
		// proves more than a new carrier still waiting for its acknowledgement.
		best = fewestStreams(p.standby, func(c *carrier) bool { return !c.silent() })
	}
	if best == nil {
		best = fewestStreams(p.carriers, nil)
	}
	return best
}

// spare reports whether a slot other than c's has an idle carrier or is still
// opening one; the caller holds p.mu.
func (p *pool) spare(c *carrier) bool {
	for slot := range p.opened {
		if slot == c.slot {
			continue
		}
		if o := p.carriers[slot]; o == nil || !o.conn.Acked() || o.session.NumStreams() == 0 {
			return true
		}
	}
	return false
}

func fewestStreams(carriers []*carrier, eligible func(*carrier) bool) *carrier {
	var best *carrier
	for _, c := range carriers {
		if c == nil || c.session.IsClosed() || (eligible != nil && !eligible(c)) {
			continue
		}
		if best == nil || c.session.NumStreams() < best.session.NumStreams() {
			best = c
		}
	}
	return best
}

// carrierStream strips the server's pong from the front of a stream and turns it
// into a round-trip sample for its carrier.
type carrierStream struct {
	*smux.Stream
	c      *carrier
	opened time.Time
	ponged bool // only touched by the single reader
}

func (c *carrier) newStream(s *smux.Stream) *carrierStream {
	cs := &carrierStream{Stream: s, c: c, opened: time.Now()}
	c.mu.Lock()
	if c.waiting == nil {
		c.waiting = make(map[*carrierStream]time.Time)
	}
	c.waiting[cs] = cs.opened
	c.mu.Unlock()
	return cs
}

func (s *carrierStream) Read(b []byte) (int, error) {
	if !s.ponged {
		var p [1]byte
		if _, err := io.ReadFull(s.Stream, p[:]); err != nil {
			return 0, err
		}
		if p[0] != proxy.Pong {
			return 0, errBadPong
		}
		s.ponged = true
		s.c.pong(s, time.Since(s.opened))
	}
	return s.Stream.Read(b)
}

func (s *carrierStream) Close() error {
	s.c.mu.Lock()
	delete(s.c.waiting, s)
	s.c.mu.Unlock()
	return s.Stream.Close()
}

func (c *carrier) pong(s *carrierStream, rtt time.Duration) {
	c.mu.Lock()
	delete(c.waiting, s)
	c.mu.Unlock()
	if old := c.rtt.Load(); old != 0 {
		rtt = time.Duration(old) + (rtt-time.Duration(old))/4
	}
	c.rtt.Store(int64(rtt))
}
