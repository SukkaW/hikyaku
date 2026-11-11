package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"net"
	"net/netip"
	"slices"
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

// systemResolver is Go's resolver, which follows the operating system's configuration
// and hosts file; tests point it at a fake DNS server.
var systemResolver = net.DefaultResolver

// lookupOS resolves host through Go's resolver, asking for IPv4 and IPv6 apart, so
// a resolver that never answers one of them doesn't hold up the other.
func lookupOS(ctx context.Context, host string, found func([]netip.Addr, time.Duration)) error {
	type result struct {
		addrs []netip.Addr
		err   error
	}
	results := make(chan result, 2)
	for _, network := range []string{"ip4", "ip6"} {
		go func() {
			addrs, err := systemResolver.LookupNetIP(ctx, network, host)
			results <- result{addrs, err}
		}()
	}
	var firstErr error
	answered := false
	for range 2 {
		r := <-results
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		for i := range r.addrs {
			r.addrs[i] = r.addrs[i].Unmap()
		}
		answered = true
		found(r.addrs, noTTL)
	}
	if answered {
		return nil
	}
	return firstErr
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
		server   string
		err      error // why the server gave no address
	}
	type exchanged struct {
		resp *dns.Msg
		err  error
	}
	replies := make(chan reply, 2*len(r.upstreams))
	for _, u := range r.upstreams {
		for _, qtype := range []uint16{dns.TypeA, dns.TypeAAAA} {
			go func() {
				a := reply{ttl: math.MaxUint32, server: u.Address()}
				// A plain server is retried once with a fresh timeout and without the
				// caller's deadline, so each query is cut off here instead.
				ctx, cancel := context.WithTimeout(context.Background(), upstreamTimeout)
				defer cancel()
				done := make(chan exchanged, 1)
				go func() {
					// EDNS0's 1232-byte limit keeps larger answers from being cut at 512 bytes.
					resp, err := u.Exchange(ctx, new(dns.Msg).SetQuestion(dns.Fqdn(host), qtype).SetEdns0(1232, false))
					done <- exchanged{resp, err}
				}()
				var res exchanged
				select {
				case res = <-done:
				case <-ctx.Done():
					res.err = ctx.Err()
				}
				switch resp := res.resp; {
				case res.err != nil:
					a.err = res.err
				case resp.Rcode == dns.RcodeSuccess:
					for _, rr := range resp.Answer {
						a.ttl = min(a.ttl, rr.Header().Ttl)
						if ip := proxyutil.IPFromRR(rr); ip.IsValid() {
							a.addrs = append(a.addrs, ip.Unmap())
						}
					}
				default:
					a.notFound = resp.Rcode == dns.RcodeNameError
					a.err = errors.New(dns.RcodeToString[resp.Rcode])
				}
				replies <- a
			}()
		}
	}
	notFound, found := false, false
	causes := make(map[string]error) // each server's last error
	for range cap(replies) {
		a := <-replies
		notFound = notFound || a.notFound
		if a.err != nil {
			causes[a.server] = a.err
		}
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
		msg := "no DNS server answered with an address"
		if len(causes) > 0 {
			servers := slices.Sorted(maps.Keys(causes))
			parts := make([]string, len(servers))
			for i, server := range servers {
				parts[i] = server + ": " + causes[server].Error()
			}
			msg += " (" + strings.Join(parts, "; ") + ")"
		}
		err = &net.DNSError{Err: msg, Name: host}
	}
	l.update(func() { l.done, l.err = true, err })
}
