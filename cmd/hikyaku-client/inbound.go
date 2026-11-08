package main

import (
	std_bufio "bufio"
	"context"
	"net"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
	"github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/uot"
	"github.com/sagernet/sing/protocol/socks"
	"github.com/xtaci/smux"
)

const udpTimeout = 5 * time.Minute

type inbound struct {
	pool *pool
}

func (h *inbound) serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go h.serveConn(conn)
	}
}

func (h *inbound) serveConn(conn net.Conn) {
	reader := std_bufio.NewReader(conn)
	// sing parses the request from reader but relays conn, so anything a client
	// pipelines after its request must be read back through the same buffer.
	buffered := &bufferedConn{Conn: conn, reader: reader}
	err := socks.HandleConnectionEx(context.Background(), buffered, reader, nil, h, nil, udpTimeout, M.SocksaddrFromNet(conn.RemoteAddr()), nil)
	if err != nil {
		conn.Close()
	}
}

func (h *inbound) NewConnectionEx(_ context.Context, conn net.Conn, _ M.Socksaddr, destination M.Socksaddr, _ N.CloseHandlerFunc) {
	stream, err := h.open(destination)
	if err != nil {
		N.ReportHandshakeFailure(conn, err)
		conn.Close()
		return
	}
	// sing's lazy SOCKS reply is not safe for the relay's concurrent first Read
	// and Write, so send it before relaying; it still does not wait for the server.
	if err := N.ReportConnHandshakeSuccess(conn, stream); err != nil {
		conn.Close()
		stream.Close()
		return
	}
	proxy.Relay(conn, stream)
}

func (h *inbound) NewPacketConnectionEx(ctx context.Context, conn N.PacketConn, _ M.Socksaddr, destination M.Socksaddr, _ N.CloseHandlerFunc) {
	stream, err := h.open(uot.RequestDestination(uot.Version))
	if err != nil {
		conn.Close()
		return
	}
	bufio.CopyPacketConn(ctx, conn, uot.NewLazyConn(stream, uot.Request{Destination: destination}))
}

// open writes the destination as soon as the stream exists, so protocols where
// the server speaks first still reach it.
func (h *inbound) open(destination M.Socksaddr) (*smux.Stream, error) {
	stream, err := h.pool.openStream()
	if err != nil {
		return nil, err
	}
	if err := M.SocksaddrSerializer.WriteAddrPort(stream, destination); err != nil {
		stream.Close()
		return nil, err
	}
	return stream, nil
}

type bufferedConn struct {
	net.Conn
	reader *std_bufio.Reader
}

func (c *bufferedConn) Read(b []byte) (int, error) {
	return c.reader.Read(b)
}
