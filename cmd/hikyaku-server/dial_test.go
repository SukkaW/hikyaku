package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"syscall"
	"testing"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
	"github.com/miekg/dns"
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

func pinOf(d *raceDialer, host string) pin {
	if e, ok := d.pinned[host]; ok {
		return *e.Value.(*pin)
	}
	return pin{}
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
	if got := pinOf(d, "race.test").addr; got != netip.MustParseAddr("127.0.0.1") {
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
	pinnedFor := func() time.Duration { return time.Until(pinOf(d, "pin.test").until) }

	dial()
	dial()
	if races != 1 {
		t.Fatalf("%d races for two dials within the pin, want 1", races)
	}
	if left := pinnedFor(); left > ttl/2 || left < ttl/2-time.Second {
		t.Fatalf("pinned for %v, want half the %v TTL", left, ttl)
	}

	d.pin("pin.test", local, -time.Second)
	dial()
	if races != 2 {
		t.Fatalf("an expired pin was reused instead of racing again")
	}

	// Nothing listens on [::1] at this port, so dialing the pin is refused at once.
	d.pin("pin.test", netip.MustParseAddr("::1"), time.Minute)
	dial()
	if races != 3 || pinOf(d, "pin.test").addr != local {
		t.Fatalf("a pin that no longer answers was not raced again")
	}

	ttl = time.Second
	d.unpin("pin.test")
	dial()
	if left := pinnedFor(); left > minPin || left < minPin-time.Second {
		t.Fatalf("pinned for %v with a %v TTL, want the %v minimum", left, ttl, minPin)
	}

	ttl = time.Hour
	d.unpin("pin.test")
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
	d.pin("hole.test", netip.MustParseAddr("192.0.2.1"), time.Minute)
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
	if races != 1 || pinOf(d, "hole.test").addr != netip.MustParseAddr("127.0.0.1") {
		t.Fatalf("a silent pin was not replaced by the race's winner")
	}
}

func TestPinsEvictLeastRecentlyUsed(t *testing.T) {
	d := newRaceDialer()
	addr := netip.MustParseAddr("127.0.0.1")
	d.pin("first", addr, time.Minute)
	for i := 1; i < maxPins; i++ {
		d.pin(fmt.Sprintf("name%d.test", i), addr, time.Minute)
	}
	d.pinnedAddr("first")
	d.pin("overflow.test", addr, time.Minute)
	if _, ok := d.pinnedAddr("first"); !ok {
		t.Fatal("a recently used pin was evicted")
	}
	if _, ok := d.pinnedAddr("name1.test"); ok {
		t.Fatal("the least recently used pin was kept past the limit")
	}
	if len(d.pinned) != maxPins || d.lru.Len() != maxPins {
		t.Fatalf("%d pins in the map and %d in the list, want %d", len(d.pinned), d.lru.Len(), maxPins)
	}
}

func TestLookupDNS(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &dns.Server{PacketConn: pc, Handler: dns.HandlerFunc(func(w dns.ResponseWriter, r *dns.Msg) {
		m := new(dns.Msg)
		m.SetReply(r)
		if q := r.Question[0]; q.Qtype == dns.TypeA {
			m.Answer = append(m.Answer,
				&dns.CNAME{Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeCNAME, Class: dns.ClassINET, Ttl: 30}, Target: "target.test."},
				&dns.A{Hdr: dns.RR_Header{Name: "target.test.", Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60}, A: net.ParseIP("192.0.2.7")},
			)
		}
		w.WriteMsg(m)
	})}
	go srv.ActivateAndServe()
	t.Cleanup(func() { srv.Shutdown() })

	host, port, _ := net.SplitHostPort(pc.LocalAddr().String())
	addrs, ttl := lookupDNS(context.Background(), &dns.ClientConfig{Servers: []string{host}, Port: port}, "name.test")
	if len(addrs) != 1 || addrs[0] != netip.MustParseAddr("192.0.2.7") {
		t.Fatalf("addresses %v, want [192.0.2.7]", addrs)
	}
	if ttl != 30*time.Second {
		t.Fatalf("TTL %v, want the CNAME's shorter 30s", ttl)
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
