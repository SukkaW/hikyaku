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
	dialTimeout = 10 * time.Second
	minBackoff  = time.Second
	maxBackoff  = 30 * time.Second
)

var errNoCarrier = errors.New("no carrier is ready")

// pool keeps a fixed set of pre-opened carriers; a slot is nil while its carrier is down.
type pool struct {
	server   string
	psk      []byte
	mu       sync.Mutex
	sessions []*smux.Session
}

func newPool(server string, psk []byte, carriers int) *pool {
	p := &pool{server: server, psk: psk, sessions: make([]*smux.Session, carriers)}
	for slot := range p.sessions {
		go p.maintain(slot)
	}
	return p
}

func (p *pool) maintain(slot int) {
	backoff := minBackoff
	for {
		session, err := p.dial()
		if err != nil {
			log.Printf("carrier %d: %v", slot, err)
			time.Sleep(backoff)
			backoff = min(2*backoff, maxBackoff)
			continue
		}
		backoff = minBackoff
		p.set(slot, session)
		log.Printf("carrier %d: ready", slot)
		<-session.CloseChan()
		p.set(slot, nil)
		log.Printf("carrier %d: closed", slot)
		time.Sleep(backoff)
	}
}

func (p *pool) set(slot int, session *smux.Session) {
	p.mu.Lock()
	p.sessions[slot] = session
	p.mu.Unlock()
}

func (p *pool) dial() (*smux.Session, error) {
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
	return session, nil
}

func (p *pool) openStream() (*smux.Stream, error) {
	p.mu.Lock()
	var best *smux.Session
	for _, s := range p.sessions {
		if s != nil && !s.IsClosed() && (best == nil || s.NumStreams() < best.NumStreams()) {
			best = s
		}
	}
	p.mu.Unlock()
	if best == nil {
		return nil, errNoCarrier
	}
	return best.OpenStream()
}
