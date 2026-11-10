package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
)

// stallingOrigin never answers its first connection and answers "ok" on later ones,
// reporting the first bytes each connection received.
func stallingOrigin(t *testing.T, firstFlight int) (string, <-chan []byte) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	received := make(chan []byte, 4)
	var accepted atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := accepted.Add(1)
			go func() {
				defer conn.Close()
				buf := make([]byte, firstFlight)
				if _, err := io.ReadFull(conn, buf); err != nil {
					return
				}
				received <- buf
				if n == 1 {
					io.Copy(io.Discard, conn)
					return
				}
				conn.Write([]byte("ok"))
			}()
		}
	}()
	return ln.Addr().String(), received
}

func TestReplay(t *testing.T) {
	key := make([]byte, proxy.PSKSize)
	rand.Read(key)
	psk := base64.StdEncoding.EncodeToString(key)
	server := startServer(t, psk)

	closeSession := func(c *carrier) { c.session.Close() }
	run := func(t *testing.T, firstFlight []byte, kill func(*carrier)) (net.Conn, <-chan []byte) {
		p := newPool(server, []byte(psk), 2)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ln.Close() })
		go (&inbound{pool: p}).serve(ln)
		client := socksClient{addr: ln.Addr().String()}

		origin, received := stallingOrigin(t, len(firstFlight))
		conn, err := client.DialContext(context.Background(), "tcp", origin)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		if _, err := conn.Write(firstFlight); err != nil {
			t.Fatal(err)
		}
		select {
		case <-received:
		case <-time.After(5 * time.Second):
			t.Fatal("origin never got the first flight")
		}
		// Kill the carrier holding the stream before the origin answers.
		p.mu.Lock()
		var busy *carrier
		for _, c := range p.carriers {
			if c != nil && c.session.NumStreams() > 0 {
				busy = c
			}
		}
		p.mu.Unlock()
		if busy == nil {
			t.Fatal("no carrier holds the stream")
		}
		kill(busy)
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		return conn, received
	}

	t.Run("TLS stream moves to another carrier", func(t *testing.T) {
		hello := []byte{0x16, 0x03, 0x01, 0x00, 0x04, 'p', 'i', 'n', 'g'}
		conn, received := run(t, hello, closeSession)
		got := make([]byte, 2)
		if _, err := io.ReadFull(conn, got); err != nil || string(got) != "ok" {
			t.Fatalf("read %q, %v; want the replayed stream's answer", got, err)
		}
		if again := <-received; !bytes.Equal(again, hello) {
			t.Fatalf("replayed first flight %q, want %q", again, hello)
		}
	})

	t.Run("TLS stream moves when its carrier's connection fails", func(t *testing.T) {
		hello := []byte{0x16, 0x03, 0x01, 0x00, 0x04, 'p', 'i', 'n', 'g'}
		// Only the TCP connection fails; smux leaves the session open until the pool's
		// next health check.
		conn, received := run(t, hello, func(c *carrier) { c.conn.Conn.Close() })
		got := make([]byte, 2)
		if _, err := io.ReadFull(conn, got); err != nil || string(got) != "ok" {
			t.Fatalf("read %q, %v; want the replayed stream's answer", got, err)
		}
		if again := <-received; !bytes.Equal(again, hello) {
			t.Fatalf("replayed first flight %q, want %q", again, hello)
		}
	})

	t.Run("plain stream is not replayed", func(t *testing.T) {
		conn, received := run(t, []byte("GET /x\r\n\r"), closeSession)
		if _, err := conn.Read(make([]byte, 2)); err == nil {
			t.Fatal("plain stream survived its carrier")
		}
		select {
		case <-received:
			t.Fatal("plain stream was replayed to the origin")
		case <-time.After(time.Second):
		}
	})
}

func TestCutOffResponseIsReset(t *testing.T) {
	key := make([]byte, proxy.PSKSize)
	rand.Read(key)
	psk := base64.StdEncoding.EncodeToString(key)
	p := newPool(startServer(t, psk), []byte(psk), 2)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go (&inbound{pool: p}).serve(ln)
	client := socksClient{addr: ln.Addr().String()}

	origin, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer origin.Close()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		conn, err := origin.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.Write([]byte("partial"))
		<-stop
	}()
	conn, err := client.DialContext(context.Background(), "tcp", origin.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(conn, make([]byte, len("partial"))); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	var busy *carrier
	for _, c := range p.carriers {
		if c != nil && c.session.NumStreams() > 0 {
			busy = c
		}
	}
	p.mu.Unlock()
	if busy == nil {
		t.Fatal("no carrier holds the stream")
	}
	busy.conn.Conn.Close()
	if _, err := conn.Read(make([]byte, 1)); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("read after the carrier died: %v, want a reset rather than a clean end", err)
	}
}
