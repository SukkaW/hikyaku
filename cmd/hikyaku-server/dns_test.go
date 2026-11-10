package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AdguardTeam/dnsproxy/upstream"
	"github.com/AdguardTeam/golibs/hostsfile"
	"github.com/miekg/dns"
)

// fakeDNS answers every query with reply, after its delay, from a loopback UDP port,
// and returns the server's udp:// address.
func fakeDNS(t *testing.T, reply func(q dns.Question) (rcode int, answer []dns.RR, delay time.Duration)) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &dns.Server{PacketConn: pc, Handler: dns.HandlerFunc(func(w dns.ResponseWriter, r *dns.Msg) {
		rcode, answer, delay := reply(r.Question[0])
		time.Sleep(delay)
		m := new(dns.Msg)
		m.SetRcode(r, rcode)
		m.Answer = answer
		w.WriteMsg(m)
	})}
	go srv.ActivateAndServe()
	t.Cleanup(func() { srv.Shutdown() })
	return "udp://" + pc.LocalAddr().String()
}

func rrA(name string, ttl uint32, ip string) dns.RR {
	return &dns.A{Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: ttl}, A: net.ParseIP(ip)}
}

func rrAAAA(name string, ttl uint32, ip string) dns.RR {
	return &dns.AAAA{Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: ttl}, AAAA: net.ParseIP(ip)}
}

// testResolver queries servers, with hosts as its only hosts file.
func testResolver(t *testing.T, hosts string, servers ...string) *dnsResolver {
	t.Helper()
	r, err := newDNSResolver(servers)
	if err != nil {
		t.Fatal(err)
	}
	storage, err := hostsfile.NewDefaultStorage(context.Background(), &hostsfile.DefaultStorageConfig{
		Logger:  slog.Default(),
		Readers: []io.Reader{strings.NewReader(hosts)},
	})
	if err != nil {
		t.Fatal(err)
	}
	r.hosts = upstream.NewHostsResolver(storage)
	return r
}

// lookupAll runs a lookup to its end and returns every answer's addresses with the
// shortest TTL among them.
func lookupAll(ctx context.Context, r *dnsResolver, host string) ([]netip.Addr, time.Duration, error) {
	var addrs []netip.Addr
	ttl := time.Duration(math.MaxInt64)
	err := r.lookup(ctx, host, func(found []netip.Addr, t time.Duration) {
		addrs = append(addrs, found...)
		ttl = min(ttl, t)
	})
	return addrs, ttl, err
}

func TestDNSIgnoresFailingServers(t *testing.T) {
	failing := fakeDNS(t, func(dns.Question) (int, []dns.RR, time.Duration) { return dns.RcodeServerFailure, nil, 0 })
	answering := fakeDNS(t, func(q dns.Question) (int, []dns.RR, time.Duration) {
		if q.Qtype == dns.TypeA {
			cname := &dns.CNAME{Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeCNAME, Class: dns.ClassINET, Ttl: 50}, Target: "target.test."}
			return dns.RcodeSuccess, []dns.RR{cname, rrA("target.test.", 60, "192.0.2.7")}, 50 * time.Millisecond
		}
		return dns.RcodeSuccess, []dns.RR{rrAAAA(q.Name, 40, "2001:db8::7")}, 50 * time.Millisecond
	})
	addrs, ttl, err := lookupAll(context.Background(), testResolver(t, "", failing, answering), "name.test")
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(addrs, netip.Addr.Compare)
	if want := []netip.Addr{netip.MustParseAddr("192.0.2.7"), netip.MustParseAddr("2001:db8::7")}; !slices.Equal(addrs, want) {
		t.Fatalf("addresses %v, want %v despite the failing server", addrs, want)
	}
	if ttl != 40*time.Second {
		t.Fatalf("TTL %v, want the shortest of the answers used, 40s", ttl)
	}
}

func TestDNSAnswersArriveAsTheyCome(t *testing.T) {
	at := func(ip string, delay time.Duration) string {
		return fakeDNS(t, func(q dns.Question) (int, []dns.RR, time.Duration) {
			if q.Qtype == dns.TypeA {
				return dns.RcodeSuccess, []dns.RR{rrA(q.Name, 60, ip)}, delay
			}
			return dns.RcodeSuccess, nil, delay
		})
	}
	r := testResolver(t, "", at("192.0.2.20", 0), at("192.0.2.21", 300*time.Millisecond))
	start := time.Now()
	var arrivals []time.Duration
	err := r.lookup(context.Background(), "progressive.test", func([]netip.Addr, time.Duration) {
		arrivals = append(arrivals, time.Since(start))
	})
	if err != nil || len(arrivals) != 2 {
		t.Fatalf("got %d answers, %v; want one from each server", len(arrivals), err)
	}
	if arrivals[0] > 150*time.Millisecond || arrivals[1] < 250*time.Millisecond {
		t.Fatalf("answers arrived at %v; want the fast server's at once and the slow one's after it", arrivals)
	}
}

func TestDNSNotFoundWaitsForOtherServers(t *testing.T) {
	missing := fakeDNS(t, func(dns.Question) (int, []dns.RR, time.Duration) { return dns.RcodeNameError, nil, 0 })
	slow := fakeDNS(t, func(q dns.Question) (int, []dns.RR, time.Duration) {
		if q.Qtype == dns.TypeA {
			return dns.RcodeSuccess, []dns.RR{rrA(q.Name, 60, "192.0.2.8")}, 100 * time.Millisecond
		}
		return dns.RcodeSuccess, nil, 100 * time.Millisecond
	})
	addrs, _, err := lookupAll(context.Background(), testResolver(t, "", missing, slow), "name.test")
	if err != nil || !slices.Equal(addrs, []netip.Addr{netip.MustParseAddr("192.0.2.8")}) {
		t.Fatalf("got %v, %v; want the slower server's address despite the other's NXDOMAIN", addrs, err)
	}

	r := testResolver(t, "", missing, missing)
	_, _, err = lookupAll(context.Background(), r, "gone.test")
	var dnsErr *net.DNSError
	if !errors.As(err, &dnsErr) || !dnsErr.IsNotFound {
		t.Fatalf("got %v, want a not-found DNS error when every server says NXDOMAIN", err)
	}
}

func TestDNSHostsFileFirst(t *testing.T) {
	var queries atomic.Int32
	server := fakeDNS(t, func(q dns.Question) (int, []dns.RR, time.Duration) {
		queries.Add(1)
		return dns.RcodeSuccess, []dns.RR{rrA(q.Name, 60, "192.0.2.1")}, 0
	})
	addrs, ttl, err := lookupAll(context.Background(), testResolver(t, "192.0.2.9 hosted.test\n", server), "hosted.test")
	if err != nil || !slices.Equal(addrs, []netip.Addr{netip.MustParseAddr("192.0.2.9")}) || ttl != noTTL {
		t.Fatalf("got %v, %v, %v; want the hosts file's address with the %v no-TTL lifetime", addrs, ttl, err, noTTL)
	}
	if n := queries.Load(); n != 0 {
		t.Fatalf("%d queries reached the DNS server for a name in the hosts file", n)
	}
}

func TestDNSConcurrentLookupsShareAQuery(t *testing.T) {
	var queries atomic.Int32
	server := fakeDNS(t, func(q dns.Question) (int, []dns.RR, time.Duration) {
		queries.Add(1)
		return dns.RcodeSuccess, []dns.RR{rrA(q.Name, 60, "192.0.2.10")}, 200 * time.Millisecond
	})
	r := testResolver(t, "", server)
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			if _, _, err := lookupAll(context.Background(), r, "shared.test"); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if n := queries.Load(); n != 2 {
		t.Fatalf("%d queries for 10 concurrent lookups, want one A and one AAAA", n)
	}
}

func TestDNSLookupHonoursCallerDeadline(t *testing.T) {
	slow := fakeDNS(t, func(q dns.Question) (int, []dns.RR, time.Duration) {
		return dns.RcodeSuccess, []dns.RR{rrA(q.Name, 60, "192.0.2.11")}, 2 * time.Second
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, err := lookupAll(ctx, testResolver(t, "", slow), "slow.test")
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatalf("got %v after %v, want the caller's deadline", err, time.Since(start))
	}
	var dnsErr *net.DNSError
	if !errors.As(err, &dnsErr) || !dnsErr.IsTimeout {
		t.Fatalf("got %v, want a DNS timeout error", err)
	}
}

func TestDNSQueriesAdvertiseEDNS0(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sizes := make(chan uint16, 4)
	srv := &dns.Server{PacketConn: pc, Handler: dns.HandlerFunc(func(w dns.ResponseWriter, r *dns.Msg) {
		var size uint16
		if opt := r.IsEdns0(); opt != nil {
			size = opt.UDPSize()
		}
		sizes <- size
		m := new(dns.Msg)
		m.SetRcode(r, dns.RcodeNameError)
		w.WriteMsg(m)
	})}
	go srv.ActivateAndServe()
	t.Cleanup(func() { srv.Shutdown() })
	lookupAll(context.Background(), testResolver(t, "", "udp://"+pc.LocalAddr().String()), "edns.test")
	for range 2 {
		if size := <-sizes; size != 1232 {
			t.Fatalf("a query advertised a %d-byte EDNS0 limit, want 1232", size)
		}
	}
}
