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

func TestRaceDialPicksTheAnsweringAddress(t *testing.T) {
	_, port := localListener(t)
	d := newRaceDialer()
	blackHole(d, "192.0.2.1")
	d.resolve = func(context.Context, string) ([]netip.Addr, time.Duration, error) {
		return []netip.Addr{netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("127.0.0.1")}, time.Minute, nil
	}
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
	races, ttl := 0, 10*time.Second
	d := newRaceDialer()
	d.resolve = func(context.Context, string) ([]netip.Addr, time.Duration, error) {
		races++
		return []netip.Addr{local}, ttl, nil
	}
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
	if races != 1 {
		t.Fatalf("%d races for two dials within the pin, want 1", races)
	}
	if left := pinnedFor(); left > ttl/2 || left < ttl/2-time.Second {
		t.Fatalf("pinned for %v, want half the %v TTL", left, ttl)
	}

	// An expired pin over addresses that are still fresh races them without DNS.
	withEntry(d, "pin.test", func(e *entry) { e.pinUntil = time.Now().Add(-time.Second) })
	dial()
	if races != 1 || pinnedFor() < ttl/2-time.Second {
		t.Fatalf("an expired pin over fresh addresses was not raced and pinned again without DNS (%d lookups)", races)
	}

	// Nothing listens on [::1] at this port, so dialing the pin is refused at once.
	withEntry(d, "pin.test", func(e *entry) {
		dead := netip.MustParseAddr("::1")
		e.addrs, e.pin, e.pinUntil = []netip.Addr{dead}, dead, time.Now().Add(time.Minute)
	})
	dial()
	if races != 2 || d.cached("pin.test").pin != local {
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
	races := 0
	d := newRaceDialer()
	blackHole(d, "192.0.2.1")
	d.resolve = func(context.Context, string) ([]netip.Addr, time.Duration, error) {
		races++
		return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, time.Minute, nil
	}
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
	if races != 1 || d.cached("hole.test").pin != netip.MustParseAddr("127.0.0.1") {
		t.Fatalf("a silent pin was not replaced by the race's winner")
	}
}

func TestPinsEvictLeastRecentlyUsed(t *testing.T) {
	d := newRaceDialer()
	addrs := []netip.Addr{netip.MustParseAddr("127.0.0.1")}
	d.remember("first", addrs, time.Minute)
	for i := 1; i < maxPins; i++ {
		d.remember(fmt.Sprintf("name%d.test", i), addrs, time.Minute)
	}
	d.cached("first")
	d.remember("overflow.test", addrs, time.Minute)
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
	d.resolve = func(context.Context, string) ([]netip.Addr, time.Duration, error) {
		lookups.Add(1)
		time.Sleep(300 * time.Millisecond)
		return []netip.Addr{local}, time.Minute, nil
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
	lookups := 0
	d := newRaceDialer()
	d.resolve = func(context.Context, string) ([]netip.Addr, time.Duration, error) {
		lookups++
		return []netip.Addr{local}, time.Minute, nil
	}
	// The name moved: its cached address, [::1] at this port, refuses.
	withEntry(d, "moved.test", func(e *entry) {
		e.addrs, e.ttl, e.expires = []netip.Addr{netip.MustParseAddr("::1")}, time.Minute, time.Now().Add(time.Minute)
	})
	conn, err := d.DialContext(context.Background(), proxy.Addr{Domain: "moved.test", Port: port})
	if err != nil {
		t.Fatalf("a name whose cached addresses all fail was not resolved again: %v", err)
	}
	conn.Close()
	if lookups != 1 || !slices.Equal(d.cached("moved.test").addrs, []netip.Addr{local}) {
		t.Fatalf("%d lookups, cached %v; want one lookup replacing the dead address", lookups, d.cached("moved.test").addrs)
	}
}

func TestPinKeptOnlyWhileListed(t *testing.T) {
	_, port := localListener(t)
	old := netip.MustParseAddr("127.0.0.1")
	d := newRaceDialer()
	slowDial(d, "127.0.0.1", 400*time.Millisecond)
	// DNS no longer lists the pinned address; [::1] at this port refuses.
	d.resolve = func(context.Context, string) ([]netip.Addr, time.Duration, error) {
		return []netip.Addr{netip.MustParseAddr("::1")}, time.Minute, nil
	}
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
	d.resolve = func(context.Context, string) ([]netip.Addr, time.Duration, error) {
		return nil, time.Minute, nil
	}
	conn, err := d.DialContext(context.Background(), proxy.Addr{Domain: "empty.test", Port: 443})
	var dnsErr *net.DNSError
	if conn != nil || !errors.As(err, &dnsErr) {
		t.Fatalf("got %v, %v; want no connection and a DNS error", conn, err)
	}
}
