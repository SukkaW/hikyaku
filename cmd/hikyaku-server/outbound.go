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
	"github.com/things-go/go-socks5/statute"
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
	destination, err := proxy.ReadRequest(stream)
	if err != nil {
		return
	}
	stream.SetReadDeadline(time.Time{})
	if _, err := stream.Write([]byte{proxy.Pong}); err != nil {
		return
	}

	if destination.FQDN == proxy.UDPDestination {
		o.relayUDP(stream)
		return
	}
	conn, err := o.dialer.DialContext(context.Background(), "tcp", destination.String())
	if err != nil {
		log.Printf("dial %s: %v", destination.String(), err)
		return
	}
	proxy.Relay(conn, stream)
}

// relayUDP serves a udp-over-tcp stream from one UDP socket: packets from the stream
// go out to their destinations, and replies come back tagged with their source.
func (o *outbound) relayUDP(stream net.Conn) {
	udp, err := net.ListenUDP("udp", nil)
	if err != nil {
		log.Printf("udp: %v", err)
		return
	}
	defer udp.Close()
	go func() {
		defer stream.Close()
		buf := make([]byte, proxy.MaxPacket)
		for {
			n, from, err := udp.ReadFromUDPAddrPort(buf)
			if err != nil {
				return
			}
			reply := statute.Datagram{DstAddr: addrSpec(from), Data: buf[:n]}
			if proxy.WritePacket(stream, reply.Bytes()) != nil {
				return
			}
		}
	}()
	// Only this goroutine sends, so the per-association resolve cache needs no lock.
	resolved := make(map[string]netip.Addr)
	buf := make([]byte, proxy.MaxPacket)
	for {
		p, err := proxy.ReadPacket(stream, buf)
		if err != nil {
			return
		}
		d, err := statute.ParseDatagram(p)
		if err != nil || d.Frag != 0 {
			continue
		}
		if to, ok := o.udpTarget(d.DstAddr, resolved); ok {
			udp.WriteToUDPAddrPort(d.Data, to)
		}
	}
}

// udpTarget applies the destination policy to a datagram, resolving names once per
// association.
func (o *outbound) udpTarget(dst statute.AddrSpec, resolved map[string]netip.Addr) (netip.AddrPort, bool) {
	var addr netip.Addr
	if dst.FQDN != "" {
		var ok bool
		addr, ok = resolved[dst.FQDN]
		if !ok {
			if addr, ok = resolve(dst.FQDN); !ok {
				return netip.AddrPort{}, false
			}
			resolved[dst.FQDN] = addr
		}
	} else {
		var ok bool
		if addr, ok = netip.AddrFromSlice(dst.IP); !ok {
			return netip.AddrPort{}, false
		}
		addr = addr.Unmap()
	}
	if !o.allowPrivate && !public(addr) {
		return netip.AddrPort{}, false
	}
	return netip.AddrPortFrom(addr, uint16(dst.Port)), true
}

func addrSpec(ap netip.AddrPort) statute.AddrSpec {
	addr := ap.Addr().Unmap()
	spec := statute.AddrSpec{IP: addr.AsSlice(), Port: int(ap.Port()), AddrType: statute.ATYPIPv6}
	if addr.Is4() {
		spec.AddrType = statute.ATYPIPv4
	}
	return spec
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
