package main

import (
	"cmp"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
	"github.com/things-go/go-socks5"
	"github.com/things-go/go-socks5/statute"
)

const udpTimeout = 5 * time.Minute

// udpTimeouts are sing-box's default UDP timeouts for the protocols it recognizes by
// port. Other ports keep udpTimeout.
var udpTimeouts = map[int]time.Duration{
	53:   10 * time.Second, // DNS
	123:  10 * time.Second, // NTP
	3478: 10 * time.Second, // STUN
	443:  30 * time.Second, // QUIC
}

var errNotConn = errors.New("SOCKS5 writer is not a connection")

type inbound struct {
	pool *pool
}

func (h *inbound) serve(ln net.Listener) error {
	server := socks5.NewServer(
		socks5.WithResolver(remoteResolver{}),
		socks5.WithConnectHandle(h.connect),
		socks5.WithAssociateHandle(h.associate),
	)
	return server.Serve(proxy.KeepAccepting(ln))
}

// remoteResolver leaves domain names alone, so the hikyaku server resolves them
// instead of the cabin's DNS, and no round trip is spent before the stream opens.
type remoteResolver struct{}

func (remoteResolver) Resolve(ctx context.Context, _ string) (context.Context, net.IP, error) {
	return ctx, nil, nil
}

func (h *inbound) connect(_ context.Context, writer io.Writer, request *socks5.Request) error {
	conn, ok := writer.(net.Conn)
	if !ok {
		return errNotConn
	}
	stream, err := newReplayStream(h, toAddr(*request.DestAddr))
	if err != nil {
		socks5.SendReply(writer, statute.RepHostUnreachable, nil)
		return err
	}
	// Reply without waiting for the server, so the app's first bytes ride the stream's
	// first flight.
	if err := socks5.SendReply(writer, statute.RepSuccess, request.LocalAddr); err != nil {
		stream.Close()
		return err
	}
	// go-socks5 parsed the request through request.Reader, which may already hold
	// bytes the client sent right behind it.
	proxy.Relay(&bufferedConn{Conn: conn, reader: request.Reader}, stream)
	return nil
}

// associate relays a SOCKS5 UDP association over one udp-over-tcp stream. It lasts
// as long as the client's control connection (RFC 1928 section 7), or until no
// packet has come from the client for its idle timeout (see udpIdle), and udpTimeout
// before its first packet.
func (h *inbound) associate(_ context.Context, writer io.Writer, request *socks5.Request) error {
	conn, ok := writer.(net.Conn)
	if !ok {
		return errNotConn
	}
	local, _ := request.LocalAddr.(*net.TCPAddr)
	peer, _ := request.RemoteAddr.(*net.TCPAddr)
	if local == nil || peer == nil {
		socks5.SendReply(writer, statute.RepServerFailure, nil)
		return errNotConn
	}
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: local.IP})
	if err != nil {
		socks5.SendReply(writer, statute.RepServerFailure, nil)
		return err
	}
	defer udp.Close()
	_, stream, err := h.pool.openStream()
	if err != nil {
		socks5.SendReply(writer, statute.RepServerFailure, nil)
		return err
	}
	defer stream.Close()
	if _, err := stream.Write(proxy.UDPRequest); err != nil {
		socks5.SendReply(writer, statute.RepServerFailure, nil)
		return err
	}
	if err := socks5.SendReply(writer, statute.RepSuccess, udp.LocalAddr()); err != nil {
		return err
	}

	var client atomic.Pointer[net.UDPAddr]
	go func() {
		defer conn.Close()
		buf := make([]byte, proxy.MaxPacket)
		var idle time.Duration
		for {
			udp.SetReadDeadline(time.Now().Add(cmp.Or(idle, udpTimeout)))
			n, from, err := udp.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if !from.IP.Equal(peer.IP) {
				continue
			}
			d, err := statute.ParseDatagram(buf[:n])
			if err != nil || d.Frag != 0 {
				continue
			}
			client.Store(from)
			idle = udpIdle(idle, d.DstAddr.Port)
			if err := proxy.WritePacket(stream, toAddr(d.DstAddr), d.Data); err != nil {
				if errors.Is(err, proxy.ErrBadPacket) {
					continue
				}
				return
			}
		}
	}()
	go func() {
		defer conn.Close()
		buf := make([]byte, proxy.MaxPacket)
		for {
			source, payload, err := proxy.ReadPacket(stream, buf)
			if errors.Is(err, proxy.ErrBadPacket) {
				continue
			}
			if err != nil {
				var failed *proxy.ResultError
				if errors.As(err, &failed) {
					log.Printf("udp: %v", err)
				}
				return
			}
			if to := client.Load(); to != nil {
				reply := statute.Datagram{DstAddr: toSpec(source), Data: payload}
				udp.WriteToUDP(reply.Bytes(), to)
			}
		}
	}()
	io.Copy(io.Discard, request.Reader)
	return nil
}

// udpIdle returns an association's idle timeout once it has also sent to port, given
// its timeout so far (0 before its first packet). One association can carry packets to
// many destinations, so it keeps the longest timeout among them.
func udpIdle(current time.Duration, port int) time.Duration {
	timeout, ok := udpTimeouts[port]
	if !ok {
		timeout = udpTimeout
	}
	return max(current, timeout)
}

func toAddr(spec statute.AddrSpec) proxy.Addr {
	if spec.FQDN != "" {
		return proxy.Addr{Domain: spec.FQDN, Port: uint16(spec.Port)}
	}
	ip, _ := netip.AddrFromSlice(spec.IP)
	return proxy.Addr{IP: ip.Unmap(), Port: uint16(spec.Port)}
}

func toSpec(addr proxy.Addr) statute.AddrSpec {
	if addr.Domain != "" {
		return statute.AddrSpec{FQDN: addr.Domain, Port: int(addr.Port), AddrType: statute.ATYPDomain}
	}
	spec := statute.AddrSpec{IP: addr.IP.AsSlice(), Port: int(addr.Port), AddrType: statute.ATYPIPv6}
	if addr.IP.Is4() {
		spec.AddrType = statute.ATYPIPv4
	}
	return spec
}

type bufferedConn struct {
	net.Conn
	reader io.Reader
}

func (c *bufferedConn) Read(b []byte) (int, error) {
	return c.reader.Read(b)
}

// SetLinger lets proxy.Relay reset the app's connection when its stream fails.
func (c *bufferedConn) SetLinger(sec int) error {
	if l, ok := c.Conn.(interface{ SetLinger(sec int) error }); ok {
		return l.SetLinger(sec)
	}
	return nil
}
