package proxy

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func tcpPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		accepted <- c
	}()
	client, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server = <-accepted
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return client, server
}

func newPSK() []byte {
	psk := make([]byte, PSKSize)
	rand.Read(psk)
	return psk
}

type handshakeResult struct {
	conn *Conn
	err  error
}

func serve(server net.Conn, psk []byte, replay *ReplayFilter) <-chan handshakeResult {
	ch := make(chan handshakeResult, 1)
	go func() {
		c, err := ServerHandshake(server, psk, replay)
		ch <- handshakeResult{c, err}
	}()
	return ch
}

func TestHandshakeRoundTrip(t *testing.T) {
	psk := newPSK()
	clientRaw, serverRaw := tcpPair(t)
	result := serve(serverRaw, psk, NewReplayFilter())
	client, err := ClientHandshake(clientRaw, psk)
	if err != nil {
		t.Fatal(err)
	}
	r := <-result
	if r.err != nil {
		t.Fatal(r.err)
	}

	up := make([]byte, 3*maxChunk+17)
	rand.Read(up)
	go client.Write(up)
	got := make([]byte, len(up))
	if _, err := io.ReadFull(r.conn, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, up) {
		t.Fatal("client to server payload mismatch")
	}

	down := []byte("downstream")
	go r.conn.Write(down)
	got = make([]byte, len(down))
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, down) {
		t.Fatal("server to client payload mismatch")
	}
}

func TestWrongPSKRejected(t *testing.T) {
	clientRaw, serverRaw := tcpPair(t)
	result := serve(serverRaw, newPSK(), NewReplayFilter())
	go ClientHandshake(clientRaw, newPSK())
	if r := <-result; r.err == nil {
		t.Fatal("server accepted a client with a different PSK")
	}
}

type recordingConn struct {
	net.Conn
	written bytes.Buffer
}

func (c *recordingConn) Write(b []byte) (int, error) {
	c.written.Write(b)
	return c.Conn.Write(b)
}

func TestReplayedHelloRejected(t *testing.T) {
	psk := newPSK()
	replay := NewReplayFilter()

	clientRaw, serverRaw := tcpPair(t)
	result := serve(serverRaw, psk, replay)
	recorder := &recordingConn{Conn: clientRaw}
	if _, err := ClientHandshake(recorder, psk); err != nil {
		t.Fatal(err)
	}
	if r := <-result; r.err != nil {
		t.Fatal(r.err)
	}

	attackerRaw, serverRaw2 := tcpPair(t)
	result = serve(serverRaw2, psk, replay)
	if _, err := attackerRaw.Write(recorder.written.Bytes()); err != nil {
		t.Fatal(err)
	}
	if r := <-result; !errors.Is(r.err, ErrReplay) {
		t.Fatalf("replayed hello: got %v, want ErrReplay", r.err)
	}
}

func TestZeroRTT(t *testing.T) {
	t.Run("data before the acknowledgement arrives", func(t *testing.T) {
		psk := newPSK()
		clientRaw, serverRaw := tcpPair(t)
		client, err := ClientHandshake(clientRaw, psk)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.Write([]byte("early")); err != nil {
			t.Fatal(err)
		}
		if client.Acked() {
			t.Fatal("acknowledged before the server answered")
		}
		server, err := ServerHandshake(serverRaw, psk, NewReplayFilter())
		if err != nil {
			t.Fatal(err)
		}
		got := make([]byte, 5)
		if _, err := io.ReadFull(server, got); err != nil || string(got) != "early" {
			t.Fatalf("server read %q, %v", got, err)
		}
		go server.Write([]byte("!"))
		if _, err := io.ReadFull(client, got[:1]); err != nil {
			t.Fatal(err)
		}
		if !client.Acked() {
			t.Fatal("not acknowledged after the server's first chunk")
		}
	})

	t.Run("wrong PSK fails the first client read", func(t *testing.T) {
		clientRaw, serverRaw := tcpPair(t)
		client, err := ClientHandshake(clientRaw, newPSK())
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			ServerHandshake(serverRaw, newPSK(), NewReplayFilter())
			serverRaw.Close()
		}()
		client.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := client.Read(make([]byte, 1)); err == nil {
			t.Fatal("client read succeeded with a different PSK")
		}
		if client.Acked() {
			t.Fatal("acknowledged with a different PSK")
		}
	})
}
