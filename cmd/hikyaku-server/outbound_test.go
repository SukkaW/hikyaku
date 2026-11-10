package main

import (
	"crypto/rand"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
	"github.com/xtaci/smux"
)

func openStream(t testing.TB, allowPrivate bool, request []byte) *smux.Stream {
	t.Helper()
	_, session := startCarrier(t, allowPrivate, nil)
	stream, err := session.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write(request); err != nil {
		t.Fatal(err)
	}
	return stream
}

// startCarrier starts a server and opens one carrier to it; onControl, if set,
// receives the server's control messages.
func startCarrier(t testing.TB, allowPrivate bool, onControl func(cmd byte, body []byte)) (*proxy.Conn, *smux.Session) {
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
	if onControl != nil {
		c.OnControl(onControl)
	}
	session, err := smux.Client(c, proxy.MuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	return c, session
}

func addrOf(t testing.TB, addr net.Addr) proxy.Addr {
	t.Helper()
	ap, err := netip.ParseAddrPort(addr.String())
	if err != nil {
		t.Fatal(err)
	}
	return proxy.Addr{IP: ap.Addr(), Port: ap.Port()}
}

func connectTo(t testing.TB, addr net.Addr) []byte {
	t.Helper()
	request, err := proxy.AppendConnect(nil, addrOf(t, addr))
	if err != nil {
		t.Fatal(err)
	}
	return request
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
	request := connectTo(t, target.Addr())

	t.Run("refused", func(t *testing.T) {
		stream := openStream(t, false, request)
		stream.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err := proxy.ReadAck(stream); err != nil {
			t.Fatal(err)
		}
		var failed *proxy.ResultError
		if err := proxy.ReadResult(stream); !errors.As(err, &failed) || failed.Code != proxy.ResultBlocked {
			t.Fatalf("result: got %v, want the loopback target blocked", err)
		}
		if _, err := stream.Read(make([]byte, 1)); err != io.EOF {
			t.Fatalf("stream read: got %v, want EOF after the failed result", err)
		}
		if len(accepted) != 0 {
			t.Fatal("server connected to a loopback target")
		}
	})

	t.Run("allowed", func(t *testing.T) {
		stream := openStream(t, true, request)
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
		readReplies(t, stream)
		if _, err := io.ReadFull(stream, got); err != nil || string(got) != "pong" {
			t.Fatalf("stream read %q, %v", got, err)
		}
	})
}

// readReplies reads the server's acknowledgement and a successful result.
func readReplies(t testing.TB, stream io.Reader) {
	t.Helper()
	if err := proxy.ReadAck(stream); err != nil {
		t.Fatalf("acknowledgement: %v", err)
	}
	if err := proxy.ReadResult(stream); err != nil {
		t.Fatalf("result: %v", err)
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
		readReplies(t, stream)
		echoAddr := addrOf(t, echo.LocalAddr())
		if err := proxy.WritePacket(stream, echoAddr, []byte("ping")); err != nil {
			t.Fatal(err)
		}
		stream.SetReadDeadline(time.Now().Add(time.Second))
		source, payload, err := proxy.ReadPacket(stream, make([]byte, proxy.MaxPacket))
		if err != nil {
			return err
		}
		if string(payload) != "ping" || source != echoAddr {
			t.Fatalf("echo returned %q from %s", payload, source)
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
	t.Run("bad packet skipped", func(t *testing.T) {
		stream := openStream(t, true, proxy.UDPRequest)
		readReplies(t, stream)
		if _, err := stream.Write([]byte{0, 1, 9}); err != nil {
			t.Fatal(err)
		}
		echoAddr := addrOf(t, echo.LocalAddr())
		if err := proxy.WritePacket(stream, echoAddr, []byte("ping")); err != nil {
			t.Fatal(err)
		}
		stream.SetReadDeadline(time.Now().Add(time.Second))
		if _, payload, err := proxy.ReadPacket(stream, make([]byte, proxy.MaxPacket)); err != nil || string(payload) != "ping" {
			t.Fatalf("echo after a bad packet: %q, %v; want the association to carry on", payload, err)
		}
	})
}

func TestResultCode(t *testing.T) {
	dial := func(err error) error { return &net.OpError{Op: "dial", Net: "tcp", Err: err} }
	cases := map[byte]error{
		proxy.ResultBlocked:         dial(errForbidden),
		proxy.ResultDNS:             &net.DNSError{Err: "no such host", Name: "example.invalid", IsNotFound: true},
		proxy.ResultRefused:         dial(os.NewSyscallError("connect", syscall.ECONNREFUSED)),
		proxy.ResultNetUnreachable:  dial(os.NewSyscallError("connect", syscall.ENETUNREACH)),
		proxy.ResultHostUnreachable: dial(os.NewSyscallError("connect", syscall.EHOSTUNREACH)),
		proxy.ResultTimeout:         dial(os.ErrDeadlineExceeded),
		proxy.ResultServerError:     errors.New("something else"),
	}
	for want, err := range cases {
		if got := resultCode(err); got != want {
			t.Errorf("%v: code %d, want %d", err, got, want)
		}
	}
}
