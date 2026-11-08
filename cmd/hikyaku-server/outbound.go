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
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/uot"
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
	destination, err := M.SocksaddrSerializer.ReadAddrPort(stream)
	if err != nil {
		return
	}
	stream.SetReadDeadline(time.Time{})
	if _, err := stream.Write([]byte{proxy.Pong}); err != nil {
		return
	}

	if destination.Fqdn == uot.MagicAddress {
		o.relayUDP(stream)
		return
	}
	conn, err := o.dialer.DialContext(context.Background(), "tcp", destination.String())
	if err != nil {
		log.Printf("dial %s: %v", destination, err)
		return
	}
	proxy.Relay(conn, stream)
}

func (o *outbound) relayUDP(stream net.Conn) {
	request, err := uot.ReadRequest(stream)
	if err != nil {
		return
	}
	udp, err := net.ListenPacket("udp", "")
	if err != nil {
		log.Printf("udp: %v", err)
		return
	}
	remote := &guardedPacketConn{
		NetPacketConn: bufio.NewPacketConn(udp),
		allowPrivate:  o.allowPrivate,
		resolved:      make(map[string]netip.Addr),
	}
	bufio.CopyPacketConn(context.Background(), uot.NewConn(stream, *request), remote)
}

// guardedPacketConn applies the destination policy to every datagram. WritePacket
// is only called from the single upload goroutine of CopyPacketConn, so the
// per-association resolve cache needs no lock.
type guardedPacketConn struct {
	N.NetPacketConn
	allowPrivate bool
	resolved     map[string]netip.Addr
}

func (c *guardedPacketConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	if destination.IsDomain() {
		addr, ok := c.resolved[destination.Fqdn]
		if !ok {
			addr, ok = resolve(destination.Fqdn)
			if !ok {
				buffer.Release()
				return nil
			}
			c.resolved[destination.Fqdn] = addr
		}
		destination = M.SocksaddrFrom(addr, destination.Port)
	}
	if !c.allowPrivate && !public(destination.Addr) {
		buffer.Release()
		return nil
	}
	return c.NetPacketConn.WritePacket(buffer, destination)
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
