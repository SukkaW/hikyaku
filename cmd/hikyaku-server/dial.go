package main

import (
	"container/list"
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
)

const (
	// A race's winner is reused for half its DNS TTL, within these bounds.
	minPin = 3 * time.Second
	maxPin = 300 * time.Second
	// maxPins bounds the cache; the least recently used name goes first.
	maxPins = 4096
	// attemptDelay is how long a pinned address connects alone before every address
	// races it: RFC 8305's recommended Connection Attempt Delay.
	attemptDelay = 250 * time.Millisecond
)

// raceDialer dials every address a name resolves to at once, keeps the first
// connection whose handshake completes and resets the others. The winner is reused
// for half the name's DNS TTL, between minPin and maxPin, before the next race.
type raceDialer struct {
	dialer  net.Dialer
	resolve func(ctx context.Context, host string) ([]netip.Addr, time.Duration, error)

	mu     sync.Mutex
	pinned map[string]*list.Element // of *pin, in lru
	lru    list.List                // most recently used at the front
}

type pin struct {
	host  string
	addr  netip.Addr
	until time.Time
}

type dialResult struct {
	conn net.Conn
	addr netip.Addr
	err  error
}

func newRaceDialer() *raceDialer {
	return &raceDialer{
		dialer:  net.Dialer{Timeout: dialTimeout},
		resolve: lookupOS,
		pinned:  make(map[string]*list.Element),
	}
}

func (d *raceDialer) DialContext(ctx context.Context, dst proxy.Addr) (net.Conn, error) {
	host := dst.Domain
	if host == "" {
		return d.dialer.DialContext(ctx, "tcp", dst.String())
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		return d.dialer.DialContext(ctx, "tcp", netip.AddrPortFrom(ip.Unmap(), dst.Port).String())
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r := &race{ctx: ctx, dialer: &d.dialer, port: dst.Port, results: make(chan dialResult), tried: make(map[netip.Addr]bool)}
	if addr, ok := d.pinnedAddr(host); ok {
		// A pin that fails, or stays silent past attemptDelay, is dropped at once; a
		// pinned address that is only slow keeps its attempt in the race.
		r.start(addr)
		timer := time.NewTimer(attemptDelay)
		select {
		case res := <-r.results:
			timer.Stop()
			r.pending--
			if res.err == nil {
				return res.conn, nil
			}
			r.errs = append(r.errs, res.err)
		case <-timer.C:
		}
		d.unpin(host)
	}
	addrs, ttl, resolveErr := d.resolve(ctx, host)
	if resolveErr != nil && r.pending == 0 {
		return nil, resolveErr
	}
	for _, addr := range addrs {
		if !r.tried[addr] {
			r.start(addr)
		}
	}
	if r.pending == 0 && len(r.errs) == 0 {
		return nil, &net.DNSError{Err: "no addresses", Name: host, IsNotFound: true}
	}
	conn, winner, err := r.wait()
	if err != nil {
		if resolveErr != nil {
			return nil, resolveErr
		}
		return nil, err
	}
	if resolveErr == nil {
		d.pin(host, winner, min(max(ttl/2, minPin), maxPin))
	}
	return conn, nil
}

// race holds one dial's connection attempts; the first to connect wins.
type race struct {
	ctx     context.Context
	dialer  *net.Dialer
	port    uint16
	results chan dialResult
	tried   map[netip.Addr]bool
	pending int
	errs    []error
}

func (r *race) start(addr netip.Addr) {
	r.tried[addr] = true
	r.pending++
	go func() {
		conn, err := r.dialer.DialContext(r.ctx, "tcp", netip.AddrPortFrom(addr, r.port).String())
		r.results <- dialResult{conn, addr, err}
	}()
}

func (r *race) wait() (net.Conn, netip.Addr, error) {
	for ; r.pending > 0; r.pending-- {
		res := <-r.results
		if res.err != nil {
			r.errs = append(r.errs, res.err)
			continue
		}
		go resetLosers(r.results, r.pending-1)
		return res.conn, res.addr, nil
	}
	return nil, netip.Addr{}, errors.Join(r.errs...)
}

// resetLosers closes the connections that completed after a race's winner with an
// RST, so their destinations don't keep them open. Cancelling the race's context
// stops the handshakes still in progress.
func resetLosers(results <-chan dialResult, n int) {
	for range n {
		r := <-results
		if tcp, ok := r.conn.(*net.TCPConn); ok {
			tcp.SetLinger(0)
			tcp.Close()
		}
	}
}

func (d *raceDialer) pinnedAddr(host string) (netip.Addr, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	e, ok := d.pinned[host]
	if !ok {
		return netip.Addr{}, false
	}
	p := e.Value.(*pin)
	if time.Now().After(p.until) {
		d.lru.Remove(e)
		delete(d.pinned, host)
		return netip.Addr{}, false
	}
	d.lru.MoveToFront(e)
	return p.addr, true
}

func (d *raceDialer) pin(host string, addr netip.Addr, ttl time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	p := &pin{host, addr, time.Now().Add(ttl)}
	if e, ok := d.pinned[host]; ok {
		e.Value = p
		d.lru.MoveToFront(e)
		return
	}
	d.pinned[host] = d.lru.PushFront(p)
	if d.lru.Len() > maxPins {
		oldest := d.lru.Back()
		d.lru.Remove(oldest)
		delete(d.pinned, oldest.Value.(*pin).host)
	}
}

func (d *raceDialer) unpin(host string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e, ok := d.pinned[host]; ok {
		d.lru.Remove(e)
		delete(d.pinned, host)
	}
}
