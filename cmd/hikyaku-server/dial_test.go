package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
)

func localListener(t *testing.T) (net.Listener, uint16) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { c.Close() })
		}
	}()
	return ln, uint16(ln.Addr().(*net.TCPAddr).Port)
}

// blackHole makes d's dials to addr hang until cancelled, like an address that drops
// every SYN, whatever the test machine's network does.
func blackHole(d *raceDialer, addr string) {
	d.dialer.ControlContext = func(ctx context.Context, _, address string, _ syscall.RawConn) error {
		if host, _, _ := net.SplitHostPort(address); host == addr {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
}

// slowDial makes d's dials to addr take delay before they connect.
func slowDial(d *raceDialer, addr string, delay time.Duration) {
	d.dialer.ControlContext = func(ctx context.Context, _, address string, _ syscall.RawConn) error {
		if host, _, _ := net.SplitHostPort(address); host == addr {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
}

// withEntry edits host's cache entry, creating it if needed.
func withEntry(d *raceDialer, host string, edit func(e *entry)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	edit(d.entryFor(host))
}

func forget(d *raceDialer, host string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if el, ok := d.entries[host]; ok {
		d.lru.Remove(el)
		delete(d.entries, host)
	}
}

// answers returns a resolver that gives addrs with ttl as one answer, counting lookups.
func answers(addrs []netip.Addr, ttl *time.Duration, lookups *atomic.Int32) func(context.Context, string, func([]netip.Addr, time.Duration)) error {
	return func(_ context.Context, _ string, found func([]netip.Addr, time.Duration)) error {
		lookups.Add(1)
		found(addrs, *ttl)
		return nil
	}
}

func TestRaceDialPicksTheAnsweringAddress(t *testing.T) {
	_, port := localListener(t)
	d := newRaceDialer()
	blackHole(d, "192.0.2.1")
	ttl := time.Minute
	d.resolve = answers([]netip.Addr{netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("127.0.0.1")}, &ttl, new(atomic.Int32))
	start := time.Now()
	conn, err := d.DialContext(context.Background(), proxy.Addr{Domain: "race.test", Port: port})
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("the race took %v, so it waited on the address that never answers", elapsed)
	}
	if got := d.cached("race.test").pin; got != netip.MustParseAddr("127.0.0.1") {
		t.Fatalf("pinned %v, want the address that answered", got)
	}
}

func TestResetLosers(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	loser, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	results := make(chan dialResult, 2)
	results <- dialResult{err: errors.New("refused")}
	results <- dialResult{conn: loser}
	resetLosers(results, 2)
	server.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := server.Read(make([]byte, 1)); !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("the losing connection ended with %v, want a reset", err)
	}
}

func TestPinnedAddress(t *testing.T) {
	_, port := localListener(t)
	local := netip.MustParseAddr("127.0.0.1")
	var races atomic.Int32
	ttl := 10 * time.Second
	d := newRaceDialer()
	d.resolve = answers([]netip.Addr{local}, &ttl, &races)
	dial := func() {
		t.Helper()
		conn, err := d.DialContext(context.Background(), proxy.Addr{Domain: "pin.test", Port: port})
		if err != nil {
			t.Fatal(err)
		}
		conn.Close()
	}
	pinnedFor := func() time.Duration { return time.Until(d.cached("pin.test").pinUntil) }

	dial()
	dial()
	if n := races.Load(); n != 1 {
		t.Fatalf("%d races for two dials within the pin, want 1", n)
	}
	if left := pinnedFor(); left > ttl/2 || left < ttl/2-time.Second {
		t.Fatalf("pinned for %v, want half the %v TTL", left, ttl)
	}

	// An expired pin over addresses that are still fresh races them without DNS.
	withEntry(d, "pin.test", func(e *entry) { e.pinUntil = time.Now().Add(-time.Second) })
	dial()
	if n := races.Load(); n != 1 || pinnedFor() < ttl/2-time.Second {
		t.Fatalf("an expired pin over fresh addresses was not raced and pinned again without DNS (%d lookups)", n)
	}

	// Nothing listens on [::1] at this port, so dialing the pin is refused at once.
	withEntry(d, "pin.test", func(e *entry) {
		dead := netip.MustParseAddr("::1")
		e.addrs, e.pin, e.pinUntil = []netip.Addr{dead}, dead, time.Now().Add(time.Minute)
	})
	dial()
	if races.Load() != 2 || d.cached("pin.test").pin != local {
		t.Fatalf("a pin that no longer answers was not raced again")
	}

	ttl = time.Second
	forget(d, "pin.test")
	dial()
	if left := pinnedFor(); left > minPin || left < minPin-time.Second {
		t.Fatalf("pinned for %v with a %v TTL, want the %v minimum", left, ttl, minPin)
	}

	ttl = time.Hour
	forget(d, "pin.test")
	dial()
	if left := pinnedFor(); left > maxPin || left < maxPin-time.Second {
		t.Fatalf("pinned for %v with a %v TTL, want the %v maximum", left, ttl, maxPin)
	}
}

func TestSilentPinRacesAfterAttemptDelay(t *testing.T) {
	_, port := localListener(t)
	var races atomic.Int32
	ttl := time.Minute
	d := newRaceDialer()
	blackHole(d, "192.0.2.1")
	d.resolve = answers([]netip.Addr{netip.MustParseAddr("127.0.0.1")}, &ttl, &races)
	withEntry(d, "hole.test", func(e *entry) {
		hole := netip.MustParseAddr("192.0.2.1")
		e.addrs, e.ttl, e.expires = []netip.Addr{hole}, time.Minute, time.Now().Add(time.Minute)
		e.pin, e.pinUntil = hole, time.Now().Add(time.Minute)
	})
	start := time.Now()
	conn, err := d.DialContext(context.Background(), proxy.Addr{Domain: "hole.test", Port: port})
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	elapsed := time.Since(start)
	if elapsed < attemptDelay || elapsed > attemptDelay+time.Second {
		t.Fatalf("a silent pin was replaced after %v, want just past the %v attempt delay", elapsed, attemptDelay)
	}
	if races.Load() != 1 || d.cached("hole.test").pin != netip.MustParseAddr("127.0.0.1") {
		t.Fatalf("a silent pin was not replaced by the race's winner")
	}
}

func TestPinsEvictLeastRecentlyUsed(t *testing.T) {
	d := newRaceDialer()
	addrs := []netip.Addr{netip.MustParseAddr("127.0.0.1")}
	d.learn("first", addrs, time.Minute)
	for i := 1; i < maxPins; i++ {
		d.learn(fmt.Sprintf("name%d.test", i), addrs, time.Minute)
	}
	d.cached("first")
	d.learn("overflow.test", addrs, time.Minute)
	if len(d.cached("first").addrs) == 0 {
		t.Fatal("a recently used name was evicted")
	}
	if len(d.cached("name1.test").addrs) != 0 {
		t.Fatal("the least recently used name was kept past the limit")
	}
	if len(d.entries) != maxPins || d.lru.Len() != maxPins {
		t.Fatalf("%d names in the map and %d in the list, want %d", len(d.entries), d.lru.Len(), maxPins)
	}
}

func TestStaleAddressesServedWhileRefreshing(t *testing.T) {
	_, port := localListener(t)
	local := netip.MustParseAddr("127.0.0.1")
	var lookups atomic.Int32
	d := newRaceDialer()
	d.resolve = func(_ context.Context, _ string, found func([]netip.Addr, time.Duration)) error {
		lookups.Add(1)
		time.Sleep(300 * time.Millisecond)
		found([]netip.Addr{local}, time.Minute)
		return nil
	}
	withEntry(d, "stale.test", func(e *entry) {
		e.addrs, e.ttl, e.expires = []netip.Addr{local}, time.Minute, time.Now().Add(-time.Second)
	})
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			start := time.Now()
			conn, err := d.DialContext(context.Background(), proxy.Addr{Domain: "stale.test", Port: port})
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
			if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
				t.Errorf("a dial over expired addresses took %v, so it waited for DNS", elapsed)
			}
		})
	}
	wg.Wait()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && time.Now().After(d.cached("stale.test").expires) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := lookups.Load(); n != 1 || time.Now().After(d.cached("stale.test").expires) {
		t.Fatalf("%d lookups; want one background refresh that renewed the expired addresses", n)
	}
}

func TestCachedAddressesThatAllFailResolveAgain(t *testing.T) {
	_, port := localListener(t)
	local := netip.MustParseAddr("127.0.0.1")
	var lookups atomic.Int32
	ttl := time.Minute
	d := newRaceDialer()
	d.resolve = answers([]netip.Addr{local}, &ttl, &lookups)
	// The name moved: its cached address, [::1] at this port, refuses.
	withEntry(d, "moved.test", func(e *entry) {
		e.addrs, e.ttl, e.expires = []netip.Addr{netip.MustParseAddr("::1")}, time.Minute, time.Now().Add(time.Minute)
	})
	conn, err := d.DialContext(context.Background(), proxy.Addr{Domain: "moved.test", Port: port})
	if err != nil {
		t.Fatalf("a name whose cached addresses all fail was not resolved again: %v", err)
	}
	conn.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !slices.Equal(d.cached("moved.test").addrs, []netip.Addr{local}) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := lookups.Load(); n != 1 || !slices.Equal(d.cached("moved.test").addrs, []netip.Addr{local}) {
		t.Fatalf("%d lookups, cached %v; want one lookup replacing the dead address", n, d.cached("moved.test").addrs)
	}
}

func TestSilentCachedAddressesAskDNS(t *testing.T) {
	_, port := localListener(t)
	ttl := time.Minute
	d := newRaceDialer()
	blackHole(d, "192.0.2.1")
	d.resolve = answers([]netip.Addr{netip.MustParseAddr("127.0.0.1")}, &ttl, new(atomic.Int32))
	// The cached address is fresh and unpinned, but drops every SYN.
	withEntry(d, "silent.test", func(e *entry) {
		e.addrs, e.ttl, e.expires = []netip.Addr{netip.MustParseAddr("192.0.2.1")}, time.Minute, time.Now().Add(time.Minute)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	conn, err := d.DialContext(ctx, proxy.Addr{Domain: "silent.test", Port: port})
	if err != nil {
		t.Fatalf("a name whose cached address went silent failed: %v", err)
	}
	conn.Close()
	if elapsed := time.Since(start); elapsed < attemptDelay || elapsed > time.Second {
		t.Fatalf("connected after %v; want DNS asked once the cached address stalled past %v", elapsed, attemptDelay)
	}
}

func TestLookupAddsUntilItCompletes(t *testing.T) {
	old, first, second := netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("192.0.2.2"), netip.MustParseAddr("192.0.2.3")
	for _, complete := range []bool{true, false} {
		d := newRaceDialer()
		release := make(chan struct{})
		d.resolve = func(_ context.Context, _ string, found func([]netip.Addr, time.Duration)) error {
			found([]netip.Addr{first}, time.Minute)
			<-release
			found([]netip.Addr{second}, time.Minute)
			if !complete {
				return context.DeadlineExceeded
			}
			return nil
		}
		withEntry(d, "add.test", func(e *entry) {
			e.addrs, e.ttl, e.expires = []netip.Addr{old}, time.Minute, time.Now().Add(time.Minute)
		})
		ctx, cancel := context.WithCancel(context.Background())
		r := &race{ctx: ctx, results: make(chan dialResult), tried: make(map[netip.Addr]bool)}
		done := d.lookup(ctx, "add.test", r)
		more := r.more
		<-more
		if addrs := d.cached("add.test").addrs; !slices.Equal(addrs, []netip.Addr{old, first}) {
			t.Fatalf("cached %v while the lookup runs; want its first answer added to what was cached", addrs)
		}
		close(release)
		for range more {
		}
		<-done
		cancel()
		want := []netip.Addr{first, second}
		if !complete {
			want = []netip.Addr{old, first, second}
		}
		if addrs := d.cached("add.test").addrs; !slices.Equal(addrs, want) {
			t.Fatalf("complete=%v: cached %v after the lookup, want %v", complete, addrs, want)
		}
	}
}

func TestAnswerForEvictedNameKeepsItsLifetime(t *testing.T) {
	d := newRaceDialer()
	d.learn("evicted.test", []netip.Addr{netip.MustParseAddr("192.0.2.1")}, 10*time.Minute)
	if e := d.cached("evicted.test"); e.ttl != 10*time.Minute || time.Until(e.expires) < 9*time.Minute {
		t.Fatalf("lifetime %v, expiring in %v; want the answer's 10m", e.ttl, time.Until(e.expires))
	}
}

func TestDialReportsLookupError(t *testing.T) {
	d := newRaceDialer()
	blackHole(d, "192.0.2.1")
	d.resolve = func(context.Context, string, func([]netip.Addr, time.Duration)) error {
		return &net.DNSError{Err: "no such host", Name: "gone.test", IsNotFound: true}
	}
	withEntry(d, "gone.test", func(e *entry) {
		hole := netip.MustParseAddr("192.0.2.1")
		e.addrs, e.ttl, e.expires = []netip.Addr{hole}, time.Minute, time.Now().Add(time.Minute)
		e.pin, e.pinUntil = hole, time.Now().Add(time.Minute)
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := d.DialContext(ctx, proxy.Addr{Domain: "gone.test", Port: 443})
	var dnsErr *net.DNSError
	if !errors.As(err, &dnsErr) || !dnsErr.IsNotFound {
		t.Fatalf("got %v; want the lookup's NXDOMAIN alongside the dial's own failure", err)
	}
}

func TestPinKeptOnlyWhileListed(t *testing.T) {
	_, port := localListener(t)
	old := netip.MustParseAddr("127.0.0.1")
	d := newRaceDialer()
	slowDial(d, "127.0.0.1", 400*time.Millisecond)
	// DNS no longer lists the pinned address; [::1] at this port refuses.
	ttl := time.Minute
	d.resolve = answers([]netip.Addr{netip.MustParseAddr("::1")}, &ttl, new(atomic.Int32))
	withEntry(d, "old.test", func(e *entry) {
		e.addrs, e.ttl, e.expires = []netip.Addr{old}, time.Minute, time.Now().Add(time.Minute)
		e.pin, e.pinUntil = old, time.Now().Add(time.Minute)
	})
	conn, err := d.DialContext(context.Background(), proxy.Addr{Domain: "old.test", Port: port})
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if p := d.cached("old.test").pin; p.IsValid() {
		t.Fatalf("pinned %v, which DNS no longer lists", p)
	}
}

func TestNoAddresses(t *testing.T) {
	d := newRaceDialer()
	d.resolve = func(context.Context, string, func([]netip.Addr, time.Duration)) error { return nil }
	conn, err := d.DialContext(context.Background(), proxy.Addr{Domain: "empty.test", Port: 443})
	var dnsErr *net.DNSError
	if conn != nil || !errors.As(err, &dnsErr) {
		t.Fatalf("got %v, %v; want no connection and a DNS error", conn, err)
	}
}

func TestRaceWinsOnFirstAnswer(t *testing.T) {
	_, port := localListener(t)
	later := netip.MustParseAddr("192.0.2.30")
	d := newRaceDialer()
	d.resolve = func(_ context.Context, _ string, found func([]netip.Addr, time.Duration)) error {
		found([]netip.Addr{netip.MustParseAddr("127.0.0.1")}, time.Minute)
		time.Sleep(500 * time.Millisecond)
		found([]netip.Addr{later}, time.Minute)
		return nil
	}
	start := time.Now()
	conn, err := d.DialContext(context.Background(), proxy.Addr{Domain: "first.test", Port: port})
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("the dial took %v, so it waited for the whole lookup", elapsed)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !slices.Contains(d.cached("first.test").addrs, later) {
		time.Sleep(10 * time.Millisecond)
	}
	if addrs := d.cached("first.test").addrs; !slices.Contains(addrs, later) {
		t.Fatalf("cached %v; want the answer that came after the race merged in", addrs)
	}
}

func TestLaterAnswerJoinsRunningRace(t *testing.T) {
	_, port := localListener(t)
	d := newRaceDialer()
	blackHole(d, "192.0.2.1")
	d.resolve = func(_ context.Context, _ string, found func([]netip.Addr, time.Duration)) error {
		found([]netip.Addr{netip.MustParseAddr("192.0.2.1")}, time.Minute)
		time.Sleep(300 * time.Millisecond)
		found([]netip.Addr{netip.MustParseAddr("127.0.0.1")}, time.Minute)
		return nil
	}
	start := time.Now()
	conn, err := d.DialContext(context.Background(), proxy.Addr{Domain: "join.test", Port: port})
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if elapsed := time.Since(start); elapsed < 300*time.Millisecond || elapsed > time.Second {
		t.Fatalf("connected after %v; want the second answer's address to join the race at 300ms", elapsed)
	}
}
