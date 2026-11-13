package proxy

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
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
	psk := make([]byte, 32)
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

// sealChunk seals payload as one data chunk of any length, as another sender may.
func sealChunk(c *Conn, payload []byte) []byte {
	var length [2]byte
	binary.BigEndian.PutUint16(length[:], uint16(len(payload)))
	buf := c.seal.Seal(nil, c.sealNonce[:], length[:], nil)
	increment(&c.sealNonce)
	buf = c.seal.Seal(buf, c.sealNonce[:], payload, nil)
	increment(&c.sealNonce)
	return buf
}

func writeChunk(c *Conn, payload []byte) error {
	_, err := c.Conn.Write(sealChunk(c, payload))
	return err
}

func TestLongestChunkAccepted(t *testing.T) {
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
	payload := make([]byte, maxRecvChunk)
	rand.Read(payload)
	go writeChunk(client, payload)
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(r.conn, got); err != nil {
		t.Fatalf("reading a %d-byte chunk: %v", len(payload), err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload mismatch")
	}
}

func TestReceivingData(t *testing.T) {
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
	payload := make([]byte, 1000)
	rand.Read(payload)
	chunk := sealChunk(client, payload)
	half := len(chunk) / 2
	if _, err := clientRaw.Write(chunk[:half]); err != nil {
		t.Fatal(err)
	}
	read := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(r.conn, make([]byte, len(payload)))
		read <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for !r.conn.ReceivingData() {
		if time.Now().After(deadline) {
			t.Fatal("a half-arrived data chunk does not show as receiving")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := clientRaw.Write(chunk[half:]); err != nil {
		t.Fatal(err)
	}
	if err := <-read; err != nil {
		t.Fatal(err)
	}
	if r.conn.ReceivingData() {
		t.Fatal("still receiving after the whole chunk arrived")
	}
}

func TestCarrierEndIsUnexpected(t *testing.T) {
	psk := newPSK()
	clientRaw, serverRaw := tcpPair(t)
	result := serve(serverRaw, psk, NewReplayFilter())
	client, err := ClientHandshake(clientRaw, psk)
	if err != nil {
		t.Fatal(err)
	}
	if r := <-result; r.err != nil {
		t.Fatal(r.err)
	}
	serverRaw.Close() // the server's end closes cleanly, between chunks
	if _, err := client.Read(make([]byte, 1)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("read %v, want io.ErrUnexpectedEOF", err)
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

// clientWithHello starts a client handshake that sends hello instead of the client's
// own, so a test can speak as another protocol version.
func clientWithHello(conn net.Conn, psk, hello []byte) (*Conn, error) {
	c := newConn(conn)
	c.r = bufio.NewReaderSize(conn, readBuffer)
	c.psk = psk
	c.clientSalt = make([]byte, saltSize)
	rand.Read(c.clientSalt)
	var err error
	if c.seal, err = newAEAD(psk, c.clientSalt, "hikyaku c2s"); err != nil {
		return nil, err
	}
	return c, c.writeWithPrefix(c.clientSalt, hello, 0)
}

func TestHelloVersion(t *testing.T) {
	t.Run("a newer client is answered with the version both speak", func(t *testing.T) {
		psk := newPSK()
		clientRaw, serverRaw := tcpPair(t)
		result := serve(serverRaw, psk, NewReplayFilter())
		client, err := clientWithHello(clientRaw, psk, []byte{helloVersion + 1})
		if err != nil {
			t.Fatal(err)
		}
		if r := <-result; r.err != nil {
			t.Fatal(r.err)
		}
		if err := client.awaitAck(); err != nil {
			t.Fatalf("acknowledgement: %v", err)
		}
	})
	t.Run("the old hello with a timestamp is rejected", func(t *testing.T) {
		psk := newPSK()
		clientRaw, serverRaw := tcpPair(t)
		result := serve(serverRaw, psk, NewReplayFilter())
		if _, err := clientWithHello(clientRaw, psk, make([]byte, 9)); err != nil {
			t.Fatal(err)
		}
		if r := <-result; r.err == nil {
			t.Fatal("server accepted a 9-byte hello")
		}
	})
}

func TestReplayFilterGenerations(t *testing.T) {
	newSalt := func() []byte {
		salt := make([]byte, saltSize)
		rand.Read(salt)
		return salt
	}
	// rotate starts a new generation; one rotation keeps salt, a second drops it.
	check := func(t *testing.T, rotate func(f *ReplayFilter, now time.Time) time.Time) {
		f := NewReplayFilter()
		now := f.started
		salt := newSalt()
		if !f.firstSeen(salt, now) {
			t.Fatal("fresh salt rejected")
		}
		if f.firstSeen(salt, now) {
			t.Fatal("repeated salt accepted")
		}
		now = rotate(f, now)
		if f.firstSeen(salt, now) {
			t.Fatal("salt forgotten after one rotation")
		}
		now = rotate(f, now)
		if !f.firstSeen(salt, now) {
			t.Fatal("salt still remembered after two rotations")
		}
	}
	t.Run("age", func(t *testing.T) {
		check(t, func(f *ReplayFilter, now time.Time) time.Time {
			now = now.Add(replayGeneration)
			f.firstSeen(newSalt(), now)
			return now
		})
	})
	t.Run("size", func(t *testing.T) {
		check(t, func(f *ReplayFilter, now time.Time) time.Time {
			for range replayGenerationSize {
				f.firstSeen(newSalt(), now)
			}
			return now
		})
	})
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

func TestControlMessages(t *testing.T) {
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
	server := r.conn
	type message struct {
		cmd  byte
		body string
	}
	atServer, atClient := make(chan message, 1), make(chan message, 1)
	server.OnControl(func(cmd byte, body []byte) { atServer <- message{cmd, string(body)} })
	client.OnControl(func(cmd byte, body []byte) { atClient <- message{cmd, string(body)} })

	// Each side sends data, a control message, then more data; reads see only the data.
	exchange := func(from, to *Conn, cmd byte, got <-chan message) {
		t.Helper()
		go func() {
			from.Write([]byte("ab"))
			from.WriteControl(cmd, []byte("id"))
			from.Write([]byte("cd"))
		}()
		data := make([]byte, 4)
		if _, err := io.ReadFull(to, data); err != nil || string(data) != "abcd" {
			t.Fatalf("read %q, %v; want only the data", data, err)
		}
		if m := <-got; m != (message{cmd, "id"}) {
			t.Fatalf("control handler got %+v", m)
		}
	}
	exchange(client, server, CtrlPing, atServer)
	exchange(server, client, CtrlPong, atClient)
}

// The server's acknowledgement must be its first chunk, so a control message ahead of
// it fails the handshake instead of reaching the control handler.
func TestControlBeforeAckRejected(t *testing.T) {
	psk := newPSK()
	clientRaw, serverRaw := tcpPair(t)
	client, err := ClientHandshake(clientRaw, psk)
	if err != nil {
		t.Fatal(err)
	}
	handled := make(chan byte, 1)
	client.OnControl(func(cmd byte, _ []byte) { handled <- cmd })
	go func() {
		s := newConn(serverRaw)
		clientSalt := make([]byte, saltSize)
		if _, err := io.ReadFull(serverRaw, clientSalt); err != nil {
			return
		}
		s.open, _ = newAEAD(psk, clientSalt, "hikyaku c2s")
		if _, err := s.readHello(); err != nil {
			return
		}
		serverSalt := make([]byte, saltSize)
		rand.Read(serverSalt)
		s.seal, _ = newAEAD(psk, append(serverSalt, clientSalt...), "hikyaku s2c")
		s.writeWithPrefix(serverSalt, []byte{CtrlGoAway, GoAwayShutdown, 0}, controlFlag)
		s.writeWithPrefix(nil, []byte{helloVersion}, 0)
	}()
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = client.Read(make([]byte, 1))
	var netErr net.Error
	if err == nil || errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatalf("read %v, want the handshake to fail", err)
	}
	if client.Acked() {
		t.Fatal("acknowledged after a control message came first")
	}
	select {
	case cmd := <-handled:
		t.Fatalf("control handler ran for command %d before the acknowledgement", cmd)
	default:
	}
}
