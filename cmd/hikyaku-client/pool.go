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
)

var errNoCarrier = errors.New("no carrier is ready")

type carrier struct {
	slot    int
	session *smux.Session
	conn    *proxy.Conn
	ready   time.Time
}

// pool keeps a fixed set of pre-opened carriers; a slot is nil while its carrier is
// down. Silent carriers wait on standby until their slot has a replacement.
type pool struct {
	server   string
	psk      []byte
	mu       sync.Mutex
	carriers []*carrier
	standby  []*carrier
}

func newPool(server string, psk []byte, carriers int) *pool {
	p := &pool{server: server, psk: psk, carriers: make([]*carrier, carriers)}
	for slot := range p.carriers {
		go p.maintain(slot)
	}
	return p
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
		if silent {
			p.toStandby(c)
			log.Printf("carrier %d: silent for %s after %s, standing by", slot, suspectAfter, c.lifetime())
			go p.retire(c)
		} else {
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
	return p.carriers[slot] != nil
}

func (p *pool) dial() (*carrier, error) {
	conn, err := net.DialTimeout("tcp", p.server, dialTimeout)
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(dialTimeout))
	c, err := proxy.ClientHandshake(conn, p.psk)
	if err != nil {
		conn.Close()
		return nil, err
	}
	conn.SetDeadline(time.Time{})
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

func (p *pool) openStream() (*smux.Stream, error) {
	p.mu.Lock()
	best := fewestStreams(p.carriers, nil)
	if best == nil {
		// A standby carrier takes new streams only once it is receiving again, and
		// only while no active carrier is usable.
		best = fewestStreams(p.standby, func(c *carrier) bool { return !c.silent() })
	}
	p.mu.Unlock()
	if best == nil {
		return nil, errNoCarrier
	}
	return best.session.OpenStream()
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
