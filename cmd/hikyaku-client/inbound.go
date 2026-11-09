package main

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
	"github.com/things-go/go-socks5"
	"github.com/things-go/go-socks5/statute"
)

const udpTimeout = 5 * time.Minute

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
	return server.Serve(ln)
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
	stream, err := newReplayStream(h, *request.DestAddr)
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
// packet has come from the client for udpTimeout.
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
	_, stream, err := h.open(proxy.UDPRequest)
	if err != nil {
		socks5.SendReply(writer, statute.RepServerFailure, nil)
		return err
	}
	defer stream.Close()
	if err := socks5.SendReply(writer, statute.RepSuccess, udp.LocalAddr()); err != nil {
		return err
	}

	var client atomic.Pointer[net.UDPAddr]
	go func() {
		defer conn.Close()
		buf := make([]byte, proxy.MaxPacket)
		for {
			udp.SetReadDeadline(time.Now().Add(udpTimeout))
			n, from, err := udp.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if !from.IP.Equal(peer.IP) {
				continue
			}
			if d, err := statute.ParseDatagram(buf[:n]); err != nil || d.Frag != 0 {
				continue
			}
			client.Store(from)
			if proxy.WritePacket(stream, buf[:n]) != nil {
				return
			}
		}
	}()
	go func() {
		defer conn.Close()
		buf := make([]byte, proxy.MaxPacket)
		for {
			p, err := proxy.ReadPacket(stream, buf)
			if err != nil {
				return
			}
			if to := client.Load(); to != nil {
				udp.WriteToUDP(p, to)
			}
		}
	}()
	io.Copy(io.Discard, request.Reader)
	return nil
}

// open writes the destination as soon as the stream exists, so protocols where
// the server speaks first still reach it.
func (h *inbound) open(destination statute.AddrSpec) (*carrier, *carrierStream, error) {
	c, stream, err := h.pool.openStream()
	if err != nil {
		return nil, nil, err
	}
	if err := proxy.WriteRequest(stream, destination); err != nil {
		stream.Close()
		return nil, nil, err
	}
	return c, stream, nil
}

type bufferedConn struct {
	net.Conn
	reader io.Reader
}

func (c *bufferedConn) Read(b []byte) (int, error) {
	return c.reader.Read(b)
}
