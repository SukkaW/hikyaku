package main

import (
	"crypto/rand"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
	"github.com/things-go/go-socks5/statute"
	"github.com/xtaci/smux"
)

func openStream(t *testing.T, allowPrivate bool, destination statute.AddrSpec) *smux.Stream {
	t.Helper()
	psk := make([]byte, proxy.PSKSize)
	rand.Read(psk)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go serve(ln, psk, newOutbound(allowPrivate))

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c, err := proxy.ClientHandshake(conn, psk)
	if err != nil {
		t.Fatal(err)
	}
	session, err := smux.Client(c, proxy.MuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	stream, err := session.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := proxy.WriteRequest(stream, destination); err != nil {
		t.Fatal(err)
	}
	return stream
}

func addrOf(t *testing.T, addr net.Addr) statute.AddrSpec {
	t.Helper()
	spec, err := statute.ParseAddrSpec(addr.String())
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

func TestTCPTarget(t *testing.T) {
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		for {
			c, err := target.Accept()
			if err != nil {
				return
			}
			accepted <- c
		}
	}()
	destination := addrOf(t, target.Addr())

	t.Run("refused", func(t *testing.T) {
		stream := openStream(t, false, destination)
		stream.SetReadDeadline(time.Now().Add(5 * time.Second))
		readPong(t, stream)
		if _, err := stream.Read(make([]byte, 1)); err != io.EOF {
			t.Fatalf("stream read: got %v, want EOF from a refused dial", err)
		}
		if len(accepted) != 0 {
			t.Fatal("server connected to a loopback target")
		}
	})

	t.Run("allowed", func(t *testing.T) {
		stream := openStream(t, true, destination)
		stream.Write([]byte("ping"))
		var conn net.Conn
		select {
		case conn = <-accepted:
		case <-time.After(5 * time.Second):
			t.Fatal("server never connected to the target")
		}
		defer conn.Close()
		got := make([]byte, 4)
		if _, err := io.ReadFull(conn, got); err != nil || string(got) != "ping" {
			t.Fatalf("target read %q, %v", got, err)
		}
		conn.Write([]byte("pong"))
		readPong(t, stream)
		if _, err := io.ReadFull(stream, got); err != nil || string(got) != "pong" {
			t.Fatalf("stream read %q, %v", got, err)
		}
	})
}

func readPong(t *testing.T, stream io.Reader) {
	t.Helper()
	var p [1]byte
	if _, err := io.ReadFull(stream, p[:]); err != nil || p[0] != proxy.Pong {
		t.Fatalf("read %v, %v; want the server's pong first", p, err)
	}
}

func TestUDPTarget(t *testing.T) {
	echo, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echo.Close()
	go func() {
		buf := make([]byte, 1500)
		for {
			n, addr, err := echo.ReadFrom(buf)
			if err != nil {
				return
			}
			echo.WriteTo(buf[:n], addr)
		}
	}()

	roundTrip := func(t *testing.T, allowPrivate bool) error {
		stream := openStream(t, allowPrivate, proxy.UDPRequest)
		readPong(t, stream)
		datagram, err := statute.NewDatagram(echo.LocalAddr().String(), []byte("ping"))
		if err != nil {
			t.Fatal(err)
		}
		if err := proxy.WritePacket(stream, datagram.Bytes()); err != nil {
			t.Fatal(err)
		}
		stream.SetReadDeadline(time.Now().Add(time.Second))
		p, err := proxy.ReadPacket(stream, make([]byte, proxy.MaxPacket))
		if err != nil {
			return err
		}
		reply, err := statute.ParseDatagram(p)
		if err != nil || string(reply.Data) != "ping" || reply.DstAddr.String() != echo.LocalAddr().String() {
			t.Fatalf("echo returned %q from %s, %v", reply.Data, reply.DstAddr.String(), err)
		}
		return nil
	}

	t.Run("refused", func(t *testing.T) {
		var timeout net.Error
		if err := roundTrip(t, false); !errors.As(err, &timeout) || !timeout.Timeout() {
			t.Fatalf("got %v, want the datagram to a loopback target dropped", err)
		}
	})
	t.Run("allowed", func(t *testing.T) {
		if err := roundTrip(t, true); err != nil {
			t.Fatal(err)
		}
	})
}
