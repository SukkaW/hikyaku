package main

import (
	"bytes"
	"crypto/rand"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
	"github.com/xtaci/smux"
)

func TestPingAndGoAway(t *testing.T) {
	t.Cleanup(endShutdown)
	got := make(chan []byte, 4)
	c, _ := startCarrier(t, func(cmd byte, body []byte) { got <- append([]byte{cmd}, body...) })
	next := func(what string) []byte {
		t.Helper()
		select {
		case m := <-got:
			return m
		case <-time.After(5 * time.Second):
			t.Fatalf("no %s from the server", what)
			return nil
		}
	}

	if err := c.WriteControl(proxy.CtrlPing, []byte{0, 0, 0, 7}); err != nil {
		t.Fatal(err)
	}
	if m := next("pong"); !bytes.Equal(m, []byte{proxy.CtrlPong, 0, 0, 0, 7}) {
		t.Fatalf("got %v, want a pong echoing the ping's id", m)
	}

	goAway()
	if m := next("GOAWAY"); len(m) < 3 || m[0] != proxy.CtrlGoAway || m[1] != proxy.GoAwayShutdown || int(m[2]) != len(m)-3 {
		t.Fatalf("got %v, want GOAWAY for a shutdown with its reason", m)
	}
}

// endShutdown undoes goAway's flag, so later tests see a running server.
func endShutdown() {
	live.Lock()
	live.goingAway = false
	live.Unlock()
}

func TestGoAwayReachesCarriersJoiningDuringShutdown(t *testing.T) {
	t.Cleanup(endShutdown)
	goAway()
	got := make(chan byte, 4)
	startCarrier(t, func(cmd byte, _ []byte) { got <- cmd })
	select {
	case cmd := <-got:
		if cmd != proxy.CtrlGoAway {
			t.Fatalf("got control %d, want GOAWAY", cmd)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a carrier that joined during shutdown was not told to go away")
	}
}

func TestEarlyResetForAnyUnopenedStream(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	client, err := smux.Client(clientConn, proxy.MuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := smux.Server(serverConn, proxy.MuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	accepted := make(map[uint32]*smux.Stream)
	for range 2 {
		if _, err := client.OpenStream(); err != nil {
			t.Fatal(err)
		}
		st, err := server.AcceptStream()
		if err != nil {
			t.Fatal(err)
		}
		accepted[st.ID()] = st
	}
	// Stream 5's SYN overtook stream 3's, and 3's RESET came before its SYN.
	s := newStreams()
	if !s.add(&resettable{Stream: accepted[5]}) {
		t.Fatal("stream 5 was refused")
	}
	s.reset(3)
	if s.add(&resettable{Stream: accepted[3]}) {
		t.Fatal("a stream the client reset before its SYN arrived was accepted")
	}
}

func TestIdleCarrierClosed(t *testing.T) {
	psk := make([]byte, 32)
	rand.Read(psk)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			return
		}
		t.Cleanup(func() { conn.Close() })
		proxy.ClientHandshake(conn, psk) // then send nothing at all
	}()
	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c, err := proxy.ServerHandshake(conn, psk, proxy.NewReplayFilter())
	if err != nil {
		t.Fatal(err)
	}
	session, err := smux.Server(c, proxy.MuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	go closeWhenIdle(c, session, 300*time.Millisecond)
	select {
	case <-session.CloseChan():
	case <-time.After(5 * time.Second):
		t.Fatal("server kept a carrier that sent nothing")
	}
}

// failingListener returns errs from Accept in turn.
type failingListener struct {
	net.Listener
	errs []error
}

func (l *failingListener) Accept() (net.Conn, error) {
	err := l.errs[0]
	l.errs = l.errs[1:]
	return nil, err
}

func TestServeSurvivesAcceptErrors(t *testing.T) {
	ln := &failingListener{errs: []error{syscall.EMFILE, net.ErrClosed}}
	if err := serve(ln, []byte("psk"), newOutbound()); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("serve returned %v, want it to keep accepting until the listener closes", err)
	}
}

func TestResetBeforeStreamOpens(t *testing.T) {
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	dialed := make(chan struct{}, 1)
	go func() {
		if conn, err := target.Accept(); err == nil {
			conn.Close()
			dialed <- struct{}{}
		}
	}()
	c, session := startCarrier(t, nil)
	// smux numbers a client's first stream 3; the RESET goes out ahead of its SYN.
	if err := c.WriteControl(proxy.CtrlReset, proxy.ResetBody(3, proxy.ResetAborted)); err != nil {
		t.Fatal(err)
	}
	stream, err := session.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if stream.ID() != 3 {
		t.Fatalf("first stream is %d, want 3", stream.ID())
	}
	if _, err := stream.Write(connectTo(t, target.Addr())); err != nil {
		t.Fatal(err)
	}
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	if n, err := stream.Read(make([]byte, 1)); n != 0 || err == nil || errors.Is(err, smux.ErrTimeout) {
		t.Fatalf("read %d bytes, %v; want the reset stream closed without an answer", n, err)
	}
	select {
	case <-dialed:
		t.Fatal("server dialed the destination of a stream the client had reset")
	case <-time.After(time.Second):
	}
}
