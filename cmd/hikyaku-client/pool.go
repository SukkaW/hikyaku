package main

import (
	"errors"
	"log"
	"net"
	"slices"
	"sync"
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
)

var errNoCarrier = errors.New("no carrier is ready")

type carrier struct {
	slot    int
	session *smux.Session
	conn    *proxy.Conn
	ready   time.Time

	mu         sync.Mutex
	unanswered map[*replayStream]struct{}
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
		silent := c.watch()
		switch {
		case silent && c.conn.Acked():
			p.toStandby(c)
			c.moveUnanswered()
			log.Printf("carrier %d: silent for %s after %s, standing by", slot, suspectAfter, c.lifetime())
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

// watch reports true once the carrier has received nothing for suspectAfter,
// and false if its session closes first.
func (c *carrier) watch() bool {
	tick := time.NewTicker(healthCheck)
	defer tick.Stop()
	for {
		select {
		case <-c.session.CloseChan():
			return false
		case <-tick.C:
			if c.silent() {
				return true
			}
		}
	}
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
func (p *pool) openStream() (*carrier, *smux.Stream, error) {
	deadline := time.Now().Add(carrierWait)
	for {
		if c := p.pick(); c != nil {
			s, err := c.session.OpenStream()
			return c, s, err
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
	if best != nil && best.session.NumStreams() > 0 {
		// No acknowledged carrier is idle: open another in the background for the
		// next request instead of making this one wait for it.
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
