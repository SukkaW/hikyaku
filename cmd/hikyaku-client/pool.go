package main

import (
	"errors"
	"log"
	"net"
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
	session *smux.Session
	conn    *proxy.Conn
}

// pool keeps a fixed set of pre-opened carriers; a slot is nil while its carrier is down.
type pool struct {
	server   string
	psk      []byte
	mu       sync.Mutex
	carriers []*carrier
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
		p.set(slot, c)
		log.Printf("carrier %d: ready", slot)
		silent := c.watch()
		p.set(slot, nil)
		if silent {
			log.Printf("carrier %d: silent, replacing", slot)
			go c.drain()
		} else {
			log.Printf("carrier %d: closed", slot)
		}
	}
}

func (p *pool) set(slot int, c *carrier) {
	p.mu.Lock()
	p.carriers[slot] = c
	p.mu.Unlock()
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
	return &carrier{session: session, conn: c}, nil
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
			if time.Since(c.conn.LastRead()) >= suspectAfter {
				return true
			}
		}
	}
}

// drain lets a silent carrier finish its existing streams, which survive a short
// outage; if the server never returns, smux's keepalive timeout closes it instead.
func (c *carrier) drain() {
	tick := time.NewTicker(healthCheck)
	defer tick.Stop()
	for {
		select {
		case <-c.session.CloseChan():
			return
		case <-tick.C:
			if c.session.NumStreams() == 0 {
				c.session.Close()
				return
			}
		}
	}
}

func (p *pool) openStream() (*smux.Stream, error) {
	p.mu.Lock()
	var best *carrier
	for _, c := range p.carriers {
		if c != nil && !c.session.IsClosed() && (best == nil || c.session.NumStreams() < best.session.NumStreams()) {
			best = c
		}
	}
	p.mu.Unlock()
	if best == nil {
		return nil, errNoCarrier
	}
	return best.session.OpenStream()
}
