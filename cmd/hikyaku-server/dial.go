package main

import (
	"container/list"
	"context"
	"errors"
	"net"
	"net/netip"
	"slices"
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
	resolve func(ctx context.Context, host string, found func(addrs []netip.Addr, ttl time.Duration)) error

	mu      sync.Mutex
	entries map[string]*list.Element // of *entry, in lru
	lru     list.List                // most recently used at the front
}

// entry caches a name's addresses and the race winner pinned among them. Addresses
// outlive their expiry until the LRU evicts the name: expired ones are raced at once
// while a refresh runs behind them, and dead ones are found by the race.
type entry struct {
	host       string
	addrs      []netip.Addr
	ttl        time.Duration
	expires    time.Time
	refreshing bool
	pin        netip.Addr
	pinUntil   time.Time
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
		entries: make(map[string]*list.Element),
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
	cached := d.cached(host)
	pinned := cached.pin.IsValid() && time.Now().Before(cached.pinUntil)
	if pinned {
		// A pin that fails, or stays silent past attemptDelay, is dropped at once; a
		// pinned address that is only slow keeps its attempt in the race.
		r.start(cached.pin)
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
	var looked <-chan error
	switch {
	case pinned || len(cached.addrs) == 0:
		// Nothing cached, or the pin among the cached addresses failed or stalled: ask
		// DNS now, racing each answer as it arrives alongside the cached addresses.
		looked = d.lookup(ctx, host, r)
	case time.Now().After(cached.expires):
		d.refresh(host)
	}
	r.add(cached.addrs)
	conn, winner, err := r.wait()
	if conn == nil && looked == nil {
		// Every cached address failed, so the name may have moved: ask DNS now.
		looked = d.lookup(ctx, host, r)
		conn, winner, err = r.wait()
	}
	if conn == nil {
		if lookupErr := <-looked; len(r.tried) == 0 {
			if lookupErr != nil {
				return nil, lookupErr
			}
			return nil, &net.DNSError{Err: "no addresses", Name: host, IsNotFound: true}
		}
		return nil, err
	}
	d.pinTo(host, winner)
	return conn, nil
}

// lookup resolves host in the background, caching each answer and adding its
// addresses to r while r waits, and reports the lookup's error once it ends. It
// outlives the dial, up to the dial's deadline, so answers that come after a winner
// still reach the cache.
func (d *raceDialer) lookup(ctx context.Context, host string, r *race) <-chan error {
	more := make(chan []netip.Addr)
	r.more = more
	done := make(chan error, 1)
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(dialTimeout)
	}
	go func() {
		lookupCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
		defer cancel()
		first := true
		err := d.resolve(lookupCtx, host, func(addrs []netip.Addr, ttl time.Duration) {
			d.learn(host, addrs, ttl, first)
			first = false
			select {
			case more <- addrs:
			case <-ctx.Done():
			}
		})
		close(more)
		done <- err
	}()
	return done
}

// race holds one dial's connection attempts; the first to connect wins.
type race struct {
	ctx     context.Context
	dialer  *net.Dialer
	port    uint16
	results chan dialResult
	more    chan []netip.Addr // addresses still to come from a running lookup
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

func (r *race) add(addrs []netip.Addr) {
	for _, addr := range addrs {
		if !r.tried[addr] {
			r.start(addr)
		}
	}
}

// wait returns the first connection to complete, taking in a running lookup's
// addresses as they arrive, or every attempt's error once nothing is left to try.
func (r *race) wait() (net.Conn, netip.Addr, error) {
	for r.pending > 0 || r.more != nil {
		select {
		case res := <-r.results:
			r.pending--
			if res.err != nil {
				r.errs = append(r.errs, res.err)
				continue
			}
			go resetLosers(r.results, r.pending)
			return res.conn, res.addr, nil
		case addrs, ok := <-r.more:
			if !ok {
				r.more = nil
				continue
			}
			r.add(addrs)
		}
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

// cached returns a copy of host's entry, or the zero entry if there is none.
func (d *raceDialer) cached(host string) entry {
	d.mu.Lock()
	defer d.mu.Unlock()
	if el, ok := d.entries[host]; ok {
		d.lru.MoveToFront(el)
		return *el.Value.(*entry)
	}
	return entry{}
}

// entryFor returns host's entry, creating it, and evicting the least recently used
// name if the cache is full. d.mu must be held.
func (d *raceDialer) entryFor(host string) *entry {
	if el, ok := d.entries[host]; ok {
		d.lru.MoveToFront(el)
		return el.Value.(*entry)
	}
	e := &entry{host: host}
	d.entries[host] = d.lru.PushFront(e)
	if d.lru.Len() > maxPins {
		oldest := d.lru.Back()
		d.lru.Remove(oldest)
		delete(d.entries, oldest.Value.(*entry).host)
	}
	return e
}

// learn caches an answer's addresses for host. A lookup's first answer replaces what
// was cached, dropping a pin it doesn't include; later answers add to it.
func (d *raceDialer) learn(host string, addrs []netip.Addr, ttl time.Duration, first bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	e := d.entryFor(host)
	expires := time.Now().Add(ttl)
	if first {
		e.addrs, e.ttl, e.expires = nil, ttl, expires
	} else {
		e.ttl = min(e.ttl, ttl)
		if expires.Before(e.expires) {
			e.expires = expires
		}
	}
	for _, addr := range addrs {
		if !slices.Contains(e.addrs, addr) {
			e.addrs = append(e.addrs, addr)
		}
	}
	if !slices.Contains(e.addrs, e.pin) {
		e.pin = netip.Addr{}
	}
}

// pinTo pins host to a race's winner for half its addresses' lifetime, if they
// include the winner.
func (d *raceDialer) pinTo(host string, winner netip.Addr) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if el, ok := d.entries[host]; ok {
		if e := el.Value.(*entry); slices.Contains(e.addrs, winner) {
			e.pin, e.pinUntil = winner, time.Now().Add(min(max(e.ttl/2, minPin), maxPin))
		}
	}
}

func (d *raceDialer) unpin(host string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if el, ok := d.entries[host]; ok {
		el.Value.(*entry).pin = netip.Addr{}
	}
}

// refresh resolves host again in the background, one refresh at a time, while its
// expired addresses keep serving.
func (d *raceDialer) refresh(host string) {
	d.mu.Lock()
	el, ok := d.entries[host]
	if !ok || el.Value.(*entry).refreshing {
		d.mu.Unlock()
		return
	}
	el.Value.(*entry).refreshing = true
	d.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
		defer cancel()
		first := true
		d.resolve(ctx, host, func(addrs []netip.Addr, ttl time.Duration) {
			d.learn(host, addrs, ttl, first)
			first = false
		})
		d.mu.Lock()
		if el, ok := d.entries[host]; ok {
			el.Value.(*entry).refreshing = false
		}
		d.mu.Unlock()
	}()
}
