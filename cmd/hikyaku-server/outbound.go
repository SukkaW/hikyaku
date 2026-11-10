package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/netip"
	"syscall"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
	"github.com/xtaci/smux"
)

const (
	headerTimeout = 10 * time.Second
	dialTimeout   = 10 * time.Second
)

var errForbidden = errors.New("destination address not allowed")

type outbound struct {
	allowPrivate bool
	dialer       net.Dialer
}

func newOutbound(allowPrivate bool) *outbound {
	o := &outbound{allowPrivate: allowPrivate, dialer: net.Dialer{Timeout: dialTimeout}}
	if !allowPrivate {
		// Checked at connect time, so every address a hostname resolves to is covered
		// and DNS cannot point the server at an internal service.
		o.dialer.ControlContext = func(_ context.Context, _, address string, _ syscall.RawConn) error {
			ap, err := netip.ParseAddrPort(address)
			if err != nil {
				return err
			}
			if !public(ap.Addr()) {
				return errForbidden
			}
			return nil
		}
	}
	return o
}

func public(addr netip.Addr) bool {
	addr = addr.Unmap()
	return !addr.IsLoopback() && !addr.IsPrivate() && !addr.IsLinkLocalUnicast() && !addr.IsMulticast() && !addr.IsUnspecified()
}

func (o *outbound) handle(stream *smux.Stream) {
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
	conn, err := o.dialer.DialContext(context.Background(), "tcp", destination.String())
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
	var dnsErr *net.DNSError
	var netErr net.Error
	switch {
	case errors.Is(err, errForbidden):
		return proxy.ResultBlocked
	case errors.As(err, &dnsErr):
		return proxy.ResultDNS
	case errors.Is(err, syscall.ECONNREFUSED):
		return proxy.ResultRefused
	case errors.Is(err, syscall.ENETUNREACH):
		return proxy.ResultNetUnreachable
	case errors.Is(err, syscall.EHOSTUNREACH):
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

// udpTarget applies the destination policy to a datagram, resolving names once per
// association.
func (o *outbound) udpTarget(dst proxy.Addr, resolved map[string]netip.Addr) (netip.AddrPort, bool) {
	addr := dst.IP.Unmap()
	if dst.Domain != "" {
		var ok bool
		addr, ok = resolved[dst.Domain]
		if !ok {
			if addr, ok = resolve(dst.Domain); !ok {
				return netip.AddrPort{}, false
			}
			resolved[dst.Domain] = addr
		}
	}
	if !o.allowPrivate && !public(addr) {
		return netip.AddrPort{}, false
	}
	return netip.AddrPortFrom(addr, dst.Port), true
}

func resolve(host string) (netip.Addr, bool) {
	for _, network := range []string{"ip4", "ip6"} {
		addrs, err := net.DefaultResolver.LookupNetIP(context.Background(), network, host)
		if err == nil && len(addrs) > 0 {
			return addrs[0], true
		}
	}
	return netip.Addr{}, false
}
