package main

import (
	"context"
	"fmt"
	"net"

	"github.com/things-go/go-socks5/statute"
)

// socksClient is a minimal SOCKS5 client for the tests, built on go-socks5's codec.
type socksClient struct {
	addr string
}

func (c socksClient) handshake(command byte, dst statute.AddrSpec) (net.Conn, statute.Reply, error) {
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return nil, statute.Reply{}, err
	}
	fail := func(err error) (net.Conn, statute.Reply, error) {
		conn.Close()
		return nil, statute.Reply{}, err
	}
	if _, err := conn.Write(statute.NewMethodRequest(statute.VersionSocks5, []byte{statute.MethodNoAuth}).Bytes()); err != nil {
		return fail(err)
	}
	if _, err := statute.ParseMethodReply(conn); err != nil {
		return fail(err)
	}
	request := statute.Request{Version: statute.VersionSocks5, Command: command, DstAddr: dst}
	if _, err := conn.Write(request.Bytes()); err != nil {
		return fail(err)
	}
	reply, err := statute.ParseReply(conn)
	if err != nil {
		return fail(err)
	}
	if reply.Response != statute.RepSuccess {
		return fail(fmt.Errorf("SOCKS5 reply %d", reply.Response))
	}
	return conn, reply, nil
}

func (c socksClient) DialContext(_ context.Context, _, address string) (net.Conn, error) {
	dst, err := statute.ParseAddrSpec(address)
	if err != nil {
		return nil, err
	}
	conn, _, err := c.handshake(statute.CommandConnect, dst)
	return conn, err
}

// associate opens a UDP association and returns its control connection and the
// address to send datagrams to.
func (c socksClient) associate() (net.Conn, *net.UDPAddr, error) {
	conn, reply, err := c.handshake(statute.CommandAssociate, statute.AddrSpec{IP: net.IPv4zero, AddrType: statute.ATYPIPv4})
	if err != nil {
		return nil, nil, err
	}
	return conn, &net.UDPAddr{IP: reply.BndAddr.IP, Port: reply.BndAddr.Port}, nil
}
