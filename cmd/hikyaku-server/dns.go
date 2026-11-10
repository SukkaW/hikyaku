package main

import (
	"context"
	"fmt"
	"math"
	"net"
	"net/netip"
	"strings"
	"time"

	"github.com/AdguardTeam/dnsproxy/proxyutil"
	"github.com/AdguardTeam/dnsproxy/upstream"
	"github.com/AdguardTeam/golibs/osutil"
	"github.com/miekg/dns"
	"golang.org/x/sync/singleflight"
)

const (
	// noTTL is the lifetime of addresses from a source that reports no TTL, Go's
	// resolver or the hosts file, so they are pinned for half of it.
	noTTL = 60 * time.Second
	// upstreamTimeout bounds each query to a configured DNS server.
	upstreamTimeout = 5 * time.Second
)

// lookupOS resolves host through Go's resolver, which follows the operating system's
// configuration and hosts file.
func lookupOS(ctx context.Context, host string) ([]netip.Addr, time.Duration, error) {
	addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	for i := range addrs {
		addrs[i] = addrs[i].Unmap()
	}
	return addrs, noTTL, err
}

// dnsResolver resolves names through the DNS servers given with -dns, after the
// hosts file.
type dnsResolver struct {
	upstreams []upstream.Upstream
	hosts     *upstream.HostsResolver
	queries   singleflight.Group
}

type resolved struct {
	addrs []netip.Addr
	ttl   time.Duration
}

func newDNSResolver(servers []string) (*dnsResolver, error) {
	hosts, err := upstream.NewDefaultHostsResolver(context.Background(), osutil.RootDirFS(), nil)
	if err != nil {
		return nil, err
	}
	r := &dnsResolver{hosts: hosts}
	for _, server := range servers {
		// AddressToUpstream writes into its Options, so each server gets its own.
		u, err := upstream.AddressToUpstream(server, &upstream.Options{Timeout: upstreamTimeout})
		if err != nil {
			return nil, fmt.Errorf("-dns %s: %w", server, err)
		}
		r.upstreams = append(r.upstreams, u)
	}
	return r, nil
}

func (r *dnsResolver) lookup(ctx context.Context, host string) ([]netip.Addr, time.Duration, error) {
	if addrs, _ := r.hosts.LookupNetIP(ctx, "ip", strings.TrimSuffix(host, ".")); len(addrs) > 0 {
		for i := range addrs {
			addrs[i] = addrs[i].Unmap()
		}
		return addrs, noTTL, nil
	}
	// Concurrent requests for a name share one query, which each upstream's
	// timeout bounds; every caller still gives up at its own deadline.
	ch := r.queries.DoChan(host, func() (any, error) { return r.query(host) })
	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, 0, res.Err
		}
		v := res.Val.(resolved)
		return v.addrs, v.ttl, nil
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}

// query asks every server for host's A and AAAA records at once and keeps, for each
// record type, the first answer that has addresses. A server's NXDOMAIN or failure
// waits for the others: the name fails only when no server has an address for it.
func (r *dnsResolver) query(host string) (resolved, error) {
	type answer struct {
		qtype    uint16
		addrs    []netip.Addr
		ttl      uint32
		notFound bool
	}
	answers := make(chan answer, 2*len(r.upstreams))
	for _, u := range r.upstreams {
		for _, qtype := range []uint16{dns.TypeA, dns.TypeAAAA} {
			go func() {
				a := answer{qtype: qtype, ttl: math.MaxUint32}
				resp, err := u.Exchange(context.Background(), new(dns.Msg).SetQuestion(dns.Fqdn(host), qtype))
				if err == nil {
					a.notFound = resp.Rcode == dns.RcodeNameError
					if resp.Rcode == dns.RcodeSuccess {
						for _, rr := range resp.Answer {
							a.ttl = min(a.ttl, rr.Header().Ttl)
							if ip := proxyutil.IPFromRR(rr); ip.IsValid() {
								a.addrs = append(a.addrs, ip.Unmap())
							}
						}
					}
				}
				answers <- a
			}()
		}
	}
	res := resolved{ttl: math.MaxInt64}
	left := map[uint16]int{dns.TypeA: len(r.upstreams), dns.TypeAAAA: len(r.upstreams)}
	notFound := false
	for left[dns.TypeA]+left[dns.TypeAAAA] > 0 {
		a := <-answers
		if left[a.qtype] == 0 {
			continue
		}
		left[a.qtype]--
		notFound = notFound || a.notFound
		if len(a.addrs) > 0 {
			left[a.qtype] = 0
			res.addrs = append(res.addrs, a.addrs...)
			res.ttl = min(res.ttl, time.Duration(a.ttl)*time.Second)
		}
	}
	if len(res.addrs) == 0 {
		if notFound {
			return res, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
		}
		return res, &net.DNSError{Err: "no DNS server answered with an address", Name: host}
	}
	return res, nil
}
