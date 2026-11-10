package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/AdguardTeam/dnsproxy/proxyutil"
	"github.com/AdguardTeam/dnsproxy/upstream"
	"github.com/AdguardTeam/golibs/osutil"
	"github.com/miekg/dns"
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
func lookupOS(ctx context.Context, host string, found func([]netip.Addr, time.Duration)) error {
	addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return err
	}
	for i := range addrs {
		addrs[i] = addrs[i].Unmap()
	}
	found(addrs, noTTL)
	return nil
}

// dnsResolver resolves names through the DNS servers given with -dns, after the
// hosts file.
type dnsResolver struct {
	upstreams []upstream.Upstream
	hosts     *upstream.HostsResolver

	mu       sync.Mutex
	inflight map[string]*sharedLookup
}

// sharedLookup is one name's query to every server, shared by the dials that need it
// while it runs. Every answer with addresses reaches each of them as it arrives.
type sharedLookup struct {
	mu      sync.Mutex
	answers []answer
	done    bool
	err     error
	changed chan struct{} // closed and replaced when an answer arrives or the query ends
}

type answer struct {
	addrs []netip.Addr
	ttl   time.Duration
}

func newDNSResolver(servers []string) (*dnsResolver, error) {
	hosts, err := upstream.NewDefaultHostsResolver(context.Background(), osutil.RootDirFS(), nil)
	if err != nil {
		return nil, err
	}
	r := &dnsResolver{hosts: hosts, inflight: make(map[string]*sharedLookup)}
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

func (r *dnsResolver) lookup(ctx context.Context, host string, found func([]netip.Addr, time.Duration)) error {
	if addrs, _ := r.hosts.LookupNetIP(ctx, "ip", strings.TrimSuffix(host, ".")); len(addrs) > 0 {
		for i := range addrs {
			addrs[i] = addrs[i].Unmap()
		}
		found(addrs, noTTL)
		return nil
	}
	// The shared query is bounded by each upstream's timeout; every caller still gives
	// up at its own deadline.
	l := r.join(host)
	for seen := 0; ; {
		l.mu.Lock()
		answers, done, err, changed := l.answers, l.done, l.err, l.changed
		l.mu.Unlock()
		for ; seen < len(answers); seen++ {
			found(answers[seen].addrs, answers[seen].ttl)
		}
		if done {
			return err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			// A DNS failure, as Go's resolver reports it, not the dial's own timeout.
			return &net.DNSError{Err: ctx.Err().Error(), Name: host, IsTimeout: errors.Is(ctx.Err(), context.DeadlineExceeded), UnwrapErr: ctx.Err()}
		}
	}
}

// join returns host's running query, starting one if there is none.
func (r *dnsResolver) join(host string) *sharedLookup {
	r.mu.Lock()
	defer r.mu.Unlock()
	if l, ok := r.inflight[host]; ok {
		return l
	}
	l := &sharedLookup{changed: make(chan struct{})}
	r.inflight[host] = l
	go r.query(host, l)
	return l
}

// update records an answer or the query's end, and wakes every dial waiting on it.
func (l *sharedLookup) update(record func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	record()
	close(l.changed)
	l.changed = make(chan struct{})
}

// query asks every server for host's A and AAAA records at once and shares each
// answer that has addresses as it arrives. A server's NXDOMAIN or failure waits for
// the others: the name fails only when no server has an address for it.
func (r *dnsResolver) query(host string, l *sharedLookup) {
	type reply struct {
		addrs    []netip.Addr
		ttl      uint32
		notFound bool
	}
	replies := make(chan reply, 2*len(r.upstreams))
	for _, u := range r.upstreams {
		for _, qtype := range []uint16{dns.TypeA, dns.TypeAAAA} {
			go func() {
				a := reply{ttl: math.MaxUint32}
				// EDNS0's 1232-byte limit keeps larger answers from being cut at 512 bytes.
				resp, err := u.Exchange(context.Background(), new(dns.Msg).SetQuestion(dns.Fqdn(host), qtype).SetEdns0(1232, false))
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
				replies <- a
			}()
		}
	}
	notFound, found := false, false
	for range cap(replies) {
		a := <-replies
		notFound = notFound || a.notFound
		if len(a.addrs) > 0 {
			found = true
			l.update(func() { l.answers = append(l.answers, answer{a.addrs, time.Duration(a.ttl) * time.Second}) })
		}
	}
	r.mu.Lock()
	delete(r.inflight, host)
	r.mu.Unlock()
	var err error
	switch {
	case found:
	case notFound:
		err = &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	default:
		err = &net.DNSError{Err: "no DNS server answered with an address", Name: host}
	}
	l.update(func() { l.done, l.err = true, err })
}
