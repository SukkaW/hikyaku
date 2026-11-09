package main

import (
	"bytes"
	"crypto/rand"
	"net"
	"testing"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
	"github.com/xtaci/smux"
)

func TestPingAndGoAway(t *testing.T) {
	got := make(chan []byte, 4)
	c, _ := startCarrier(t, false, func(cmd byte, body []byte) { got <- append([]byte{cmd}, body...) })
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

func TestIdleCarrierClosed(t *testing.T) {
	psk := make([]byte, proxy.PSKSize)
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
