package main

import (
	"container/list"
	"context"
	"errors"
	"math"
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
		r.start(cached.pin)
	} else {
		r.add(cached.addrs)
	}
	// What is cached connects alone for attemptDelay: the pin by itself, or else every
	// cached address. If that fails or stalls, DNS is asked at once and its answers join
	// the race, where an attempt that is only slow keeps its place.
	if conn, winner, ok := r.waitFor(attemptDelay); ok {
		if !pinned {
			d.pinTo(host, winner)
			if time.Now().After(cached.expires) {
				d.refresh(host)
			}
		}
		return conn, nil
	}
	if pinned {
		d.unpin(host)
	}
	looked := d.lookup(ctx, host, r)
	r.add(cached.addrs)
	conn, winner, err := r.wait()
	if conn == nil {
		lookupErr := <-looked
		if len(r.tried) == 0 {
			if lookupErr != nil {
				return nil, lookupErr
			}
			return nil, &net.DNSError{Err: "no addresses", Name: host, IsNotFound: true}
		}
		return nil, errors.Join(err, lookupErr)
	}
	d.pinTo(host, winner)
	return conn, nil
}

// lookup resolves host in the background, adding each answer's addresses to the cache
// and to r while r waits, and reports the lookup's error once it ends. It outlives the
// dial, up to the dial's deadline, so answers that come after a winner still reach the
// cache.
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
		err := d.resolveAndCache(lookupCtx, host, func(addrs []netip.Addr, ttl time.Duration) {
			d.learn(host, addrs, ttl)
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

// resolveAndCache resolves host, handing each answer to found, and once the lookup
// completes replaces host's cached addresses with every address it found. A lookup
// cut off by its deadline or with no answer leaves the cache as it was.
func (d *raceDialer) resolveAndCache(ctx context.Context, host string, found func([]netip.Addr, time.Duration)) error {
	var all []netip.Addr
	shortest := time.Duration(math.MaxInt64)
	err := d.resolve(ctx, host, func(addrs []netip.Addr, ttl time.Duration) {
		for _, addr := range addrs {
			if !slices.Contains(all, addr) {
				all = append(all, addr)
			}
		}
		shortest = min(shortest, ttl)
		found(addrs, ttl)
	})
	if err == nil && len(all) > 0 {
		d.settle(host, all, shortest)
	}
	return err
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

// won takes in one attempt's result and reports whether it is the winning connection.
func (r *race) won(res dialResult) bool {
	r.pending--
	if res.err != nil {
		r.errs = append(r.errs, res.err)
		return false
	}
	go resetLosers(r.results, r.pending)
	return true
}

// waitFor waits up to delay for a connection, giving up early once every attempt so
// far has failed.
func (r *race) waitFor(delay time.Duration) (net.Conn, netip.Addr, bool) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for r.pending > 0 {
		select {
		case res := <-r.results:
			if r.won(res) {
				return res.conn, res.addr, true
			}
		case <-timer.C:
			return nil, netip.Addr{}, false
		}
	}
	return nil, netip.Addr{}, false
}

// wait returns the first connection to complete, taking in a running lookup's
// addresses as they arrive, or every attempt's error once nothing is left to try.
func (r *race) wait() (net.Conn, netip.Addr, error) {
	for r.pending > 0 || r.more != nil {
		select {
		case res := <-r.results:
			if r.won(res) {
				return res.conn, res.addr, nil
			}
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

// learn adds an answer's addresses to host's entry while a lookup runs. A new entry
// takes the answer's lifetime; an existing one keeps its own until the lookup settles.
func (d *raceDialer) learn(host string, addrs []netip.Addr, ttl time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	e := d.entryFor(host)
	if e.expires.IsZero() {
		e.ttl, e.expires = ttl, time.Now().Add(ttl)
	}
	for _, addr := range addrs {
		if !slices.Contains(e.addrs, addr) {
			e.addrs = append(e.addrs, addr)
		}
	}
}

// settle replaces host's addresses with those of a completed lookup, with the shortest
// lifetime among its answers, dropping a pin they no longer include.
func (d *raceDialer) settle(host string, addrs []netip.Addr, ttl time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	e := d.entryFor(host)
	e.addrs, e.ttl, e.expires = addrs, ttl, time.Now().Add(ttl)
	if !slices.Contains(addrs, e.pin) {
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
		d.resolveAndCache(ctx, host, func([]netip.Addr, time.Duration) {})
		d.mu.Lock()
		if el, ok := d.entries[host]; ok {
			el.Value.(*entry).refreshing = false
		}
		d.mu.Unlock()
	}()
}
