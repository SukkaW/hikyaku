package proxy

import (
	"crypto/rand"
	"io"
	"net"
	"testing"

	"github.com/xtaci/smux"
)

// carrierPair returns the two ends of an encrypted carrier over loopback TCP.
func carrierPair(b *testing.B) (client, server *Conn) {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { ln.Close() })
	psk := make([]byte, PSKSize)
	rand.Read(psk)
	type accepted struct {
		c   *Conn
		err error
	}
	ch := make(chan accepted, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			ch <- accepted{err: err}
			return
		}
		c, err := ServerHandshake(raw, psk, NewReplayFilter())
		ch <- accepted{c, err}
	}()
	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	if client, err = ClientHandshake(raw, psk); err != nil {
		b.Fatal(err)
	}
	a := <-ch
	if a.err != nil {
		b.Fatal(a.err)
	}
	b.Cleanup(func() {
		client.Close()
		a.c.Close()
	})
	return client, a.c
}

// pump writes b.N payloads to w while the other end reads them all.
func pump(b *testing.B, w io.Writer, r io.Reader) {
	payload := make([]byte, 32<<10)
	b.SetBytes(int64(len(payload)))
	done := make(chan error, 1)
	go func() {
		_, err := io.CopyN(io.Discard, r, int64(b.N)*int64(len(payload)))
		done <- err
	}()
	b.ResetTimer()
	for range b.N {
		if _, err := w.Write(payload); err != nil {
			b.Fatal(err)
		}
	}
	if err := <-done; err != nil {
		b.Fatal(err)
	}
}

// BenchmarkConn measures the encrypted carrier alone.
func BenchmarkConn(b *testing.B) {
	client, server := carrierPair(b)
	pump(b, client, server)
}

// BenchmarkStream measures one smux stream over the encrypted carrier.
func BenchmarkStream(b *testing.B) {
	client, server := carrierPair(b)
	cs, err := smux.Client(client, MuxConfig())
	if err != nil {
		b.Fatal(err)
	}
	ss, err := smux.Server(server, MuxConfig())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		cs.Close()
		ss.Close()
	})
	out, err := cs.OpenStream()
	if err != nil {
		b.Fatal(err)
	}
	// smux announces a stream with its first frame.
	if _, err := out.Write([]byte{0}); err != nil {
		b.Fatal(err)
	}
	in, err := ss.AcceptStream()
	if err != nil {
		b.Fatal(err)
	}
	if _, err := io.ReadFull(in, make([]byte, 1)); err != nil {
		b.Fatal(err)
	}
	pump(b, out, in)
}
