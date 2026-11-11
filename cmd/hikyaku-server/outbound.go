package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/netip"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
)

const (
	headerTimeout = 10 * time.Second
	dialTimeout   = 10 * time.Second
)

type outbound struct {
	dialer *raceDialer
}

func newOutbound() *outbound {
	return &outbound{dialer: newRaceDialer()}
}

func (o *outbound) handle(stream net.Conn) {
	defer stream.Close()
	stream.SetReadDeadline(time.Now().Add(headerTimeout))
	destination, udp, err := proxy.ReadRequest(stream)
	if err != nil {
		return
	}
	stream.SetReadDeadline(time.Time{})
	if err := proxy.WriteAck(stream); err != nil {
		return
	}

	if udp {
		o.relayUDP(stream)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	conn, err := o.dialer.DialContext(ctx, destination)
	cancel()
	if err != nil {
		log.Printf("dial %s: %v", destination, err)
		proxy.WriteResult(stream, resultCode(err), err.Error())
		return
	}
	if err := proxy.WriteResult(stream, proxy.ResultOK, ""); err != nil {
		conn.Close()
		return
	}
	proxy.Relay(conn, stream)
}

// resultCode tells the client why reaching the destination failed, so it can
// separate a dead website from a dead carrier.
func resultCode(err error) byte {
	// A race that failed on every address reports its most telling attempt: an
	// unreachable network is often only a missing IPv4 or IPv6 route, so it ranks last.
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		codes := make(map[byte]bool)
		for _, e := range joined.Unwrap() {
			codes[resultCode(e)] = true
		}
		for _, code := range []byte{proxy.ResultDNS, proxy.ResultTimeout, proxy.ResultRefused, proxy.ResultHostUnreachable, proxy.ResultNetUnreachable} {
			if codes[code] {
				return code
			}
		}
		return proxy.ResultServerError
	}
	var dnsErr *net.DNSError
	var netErr net.Error
	switch {
	case errors.As(err, &dnsErr):
		return proxy.ResultDNS
	case errors.Is(err, errRefused):
		return proxy.ResultRefused
	case errors.Is(err, errNetUnreachable):
		return proxy.ResultNetUnreachable
	case errors.Is(err, errHostUnreachable):
		return proxy.ResultHostUnreachable
	case errors.As(err, &netErr) && netErr.Timeout():
		return proxy.ResultTimeout
	default:
		return proxy.ResultServerError
	}
}

// relayUDP serves a udp-over-tcp stream from one UDP socket: packets from the stream
// go out to their destinations, and replies come back tagged with their source.
func (o *outbound) relayUDP(stream net.Conn) {
	udp, err := net.ListenUDP("udp", nil)
	if err != nil {
		log.Printf("udp: %v", err)
		proxy.WriteResult(stream, proxy.ResultServerError, err.Error())
		return
	}
	defer udp.Close()
	if err := proxy.WriteResult(stream, proxy.ResultOK, ""); err != nil {
		return
	}
	go func() {
		defer stream.Close()
		buf := make([]byte, proxy.MaxPacket)
		for {
			n, from, err := udp.ReadFromUDPAddrPort(buf)
			if err != nil {
				return
			}
			source := proxy.Addr{IP: from.Addr().Unmap(), Port: from.Port()}
			if err := proxy.WritePacket(stream, source, buf[:n]); err != nil {
				// Anyone can send to this socket, so one unsendable datagram must not end
				// the association.
				if errors.Is(err, proxy.ErrBadPacket) {
					continue
				}
				return
			}
		}
	}()
	// Only this goroutine sends, so the per-association resolve cache needs no lock.
	resolved := make(map[string]netip.Addr)
	buf := make([]byte, proxy.MaxPacket)
	for {
		dst, payload, err := proxy.ReadPacket(stream, buf)
		if errors.Is(err, proxy.ErrBadPacket) {
			continue
		}
		if err != nil {
			return
		}
		if to, ok := o.udpTarget(dst, resolved); ok {
			udp.WriteToUDPAddrPort(payload, to)
		}
	}
}

// udpTarget finds where a datagram goes, resolving names once per association.
func (o *outbound) udpTarget(dst proxy.Addr, resolved map[string]netip.Addr) (netip.AddrPort, bool) {
	addr := dst.IP.Unmap()
	if dst.Domain != "" {
		var ok bool
		addr, ok = resolved[dst.Domain]
		if !ok {
			if addr, ok = o.udpAddress(dst.Domain); !ok {
				return netip.AddrPort{}, false
			}
			resolved[dst.Domain] = addr
		}
	}
	return netip.AddrPortFrom(addr, dst.Port), true
}

// udpAddress resolves a UDP destination's name through the same resolver as TCP
// dials, preferring IPv4, as the UDP socket may have no IPv6 route.
func (o *outbound) udpAddress(host string) (netip.Addr, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	var v4, v6 netip.Addr
	o.dialer.resolve(ctx, host, func(addrs []netip.Addr, _ time.Duration) {
		for _, addr := range addrs {
			switch {
			case addr.Is4() && !v4.IsValid():
				v4 = addr
				cancel()
			case addr.Is6() && !v6.IsValid():
				v6 = addr
			}
		}
	})
	if v4.IsValid() {
		return v4, true
	}
	return v6, v6.IsValid()
}
