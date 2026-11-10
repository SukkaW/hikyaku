package main

import (
	std_bufio "bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
	"github.com/things-go/go-socks5/statute"
)

func startServer(t testing.TB, psk string) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "hikyaku-server")
	if out, err := exec.Command("go", "build", "-o", bin, "github.com/SukkaW/hikyaku/cmd/hikyaku-server").CombinedOutput(); err != nil {
		t.Fatalf("build server: %v\n%s", err, out)
	}
	cmd := exec.Command(bin, "-listen", "127.0.0.1:0", "-psk", psk)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})

	addr := make(chan string, 1)
	go func() {
		scanner := std_bufio.NewScanner(stderr)
		for scanner.Scan() {
			if _, after, ok := strings.Cut(scanner.Text(), "listening on "); ok {
				addr <- after
			}
		}
	}()
	select {
	case a := <-addr:
		return a
	case <-time.After(10 * time.Second):
		t.Fatal("server did not start")
		return ""
	}
}

func startClient(t testing.TB, server string, psk []byte) socksClient {
	t.Helper()
	p := newPool(server, psk, 2)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go (&inbound{pool: p}).serve(ln)
	return socksClient{addr: ln.Addr().String()}
}

func TestEndToEnd(t *testing.T) {
	key := make([]byte, proxy.PSKSize)
	rand.Read(key)
	psk := base64.StdEncoding.EncodeToString(key)
	client := startClient(t, startServer(t, psk), []byte(psk))

	t.Run("fetch", func(t *testing.T) {
		origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			io.WriteString(w, "hello through hikyaku")
		}))
		defer origin.Close()
		httpClient := &http.Client{Transport: &http.Transport{
			DialContext: client.DialContext,
		}}
		resp, err := httpClient.Get(origin.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "hello through hikyaku" {
			t.Fatalf("body %q", body)
		}
	})

	t.Run("cancel", func(t *testing.T) {
		origin, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer origin.Close()
		aborted := make(chan struct{})
		go func() {
			conn, err := origin.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
			chunk := make([]byte, 32<<10)
			for {
				if _, err := conn.Write(chunk); err != nil {
					close(aborted)
					return
				}
			}
		}()
		conn, err := client.DialContext(context.Background(), "tcp", origin.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(conn, make([]byte, 64<<10)); err != nil {
			t.Fatal(err)
		}
		conn.Close()
		select {
		case <-aborted:
		case <-time.After(5 * time.Second):
			t.Fatal("origin kept streaming after the client cancelled")
		}
	})

	t.Run("refused", func(t *testing.T) {
		closed, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := closed.Addr().String()
		closed.Close() // nothing listens there now, so the server's dial is refused
		conn, err := client.DialContext(context.Background(), "tcp", address)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err = conn.Read(make([]byte, 1))
		var timeout net.Error
		if err == nil || (errors.As(err, &timeout) && timeout.Timeout()) {
			t.Fatalf("read: got %v, want the connection closed once the server reports the refusal", err)
		}
	})

	t.Run("udp", func(t *testing.T) {
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
		control, relay, err := client.associate()
		if err != nil {
			t.Fatal(err)
		}
		defer control.Close()
		pc, err := net.DialUDP("udp", nil, relay)
		if err != nil {
			t.Fatal(err)
		}
		defer pc.Close()
		datagram, err := statute.NewDatagram(echo.LocalAddr().String(), []byte("ping"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pc.Write(datagram.Bytes()); err != nil {
			t.Fatal(err)
		}
		pc.SetReadDeadline(time.Now().Add(5 * time.Second))
		got := make([]byte, 64)
		n, err := pc.Read(got)
		if err != nil {
			t.Fatal(err)
		}
		reply, err := statute.ParseDatagram(got[:n])
		if err != nil || string(reply.Data) != "ping" || reply.DstAddr.String() != echo.LocalAddr().String() {
			t.Fatalf("udp echo %q from %s, %v; want ping from %s", reply.Data, reply.DstAddr.String(), err, echo.LocalAddr())
		}
	})

	t.Run("answer while an upload is blocked", func(t *testing.T) {
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
			// Answer without reading the upload, as a server that rejects it early does.
			time.Sleep(time.Second)
			conn.Write([]byte("ok"))
			<-stop
		}()
		conn, err := client.DialContext(context.Background(), "tcp", origin.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		go func() {
			chunk := make([]byte, 64<<10)
			for range (128 << 20) / len(chunk) {
				if _, err := conn.Write(chunk); err != nil {
					return
				}
			}
		}()
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		got := make([]byte, 2)
		if _, err := io.ReadFull(conn, got); err != nil || string(got) != "ok" {
			t.Fatalf("read %q, %v; want the answer that arrived while the upload was blocked", got, err)
		}
	})

	t.Run("half-close keeps the response", func(t *testing.T) {
		origin, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer origin.Close()
		go func() {
			conn, err := origin.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
			request, _ := io.ReadAll(conn) // until the app half-closes
			conn.Write(append([]byte("got "), request...))
		}()
		conn, err := client.DialContext(context.Background(), "tcp", origin.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := conn.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
			t.Fatal(err)
		}
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		if got, err := io.ReadAll(conn); err != nil || string(got) != "got ping" {
			t.Fatalf("read %q, %v; want the answer to a request the app half-closed after", got, err)
		}
	})

	// A reset discards data not yet read, so each side resets only once the other has
	// read what came before it.
	t.Run("destination reset reaches the app", func(t *testing.T) {
		origin, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer origin.Close()
		read := make(chan struct{})
		go func() {
			conn, err := origin.Accept()
			if err != nil {
				return
			}
			conn.Write([]byte("partial"))
			<-read
			conn.(*net.TCPConn).SetLinger(0)
			conn.Close()
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
		close(read)
		if _, err := conn.Read(make([]byte, 1)); err == nil || errors.Is(err, io.EOF) {
			t.Fatalf("read after the destination reset: %v, want a reset rather than a clean end", err)
		}
	})

	t.Run("app reset reaches the destination", func(t *testing.T) {
		origin, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer origin.Close()
		read := make(chan struct{})
		ended := make(chan error, 1)
		go func() {
			conn, err := origin.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
			if _, err := io.ReadFull(conn, make([]byte, 1)); err != nil {
				ended <- err
				return
			}
			close(read)
			conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			_, err = conn.Read(make([]byte, 1))
			ended <- err
		}()
		conn, err := client.DialContext(context.Background(), "tcp", origin.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		select {
		case <-read:
		case err := <-ended:
			t.Fatalf("destination never got the app's first byte: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("destination never got the app's first byte")
		}
		conn.(*net.TCPConn).SetLinger(0)
		conn.Close()
		if err := <-ended; err == nil || errors.Is(err, io.EOF) {
			t.Fatalf("destination read after the app reset: %v, want a reset rather than a clean end", err)
		}
	})
}

func TestUDPIdle(t *testing.T) {
	cases := []struct {
		name  string
		ports []int
		want  time.Duration
	}{
		{"DNS only", []int{53, 53}, 10 * time.Second},
		{"DNS and NTP", []int{53, 123}, 10 * time.Second},
		{"STUN", []int{3478}, 10 * time.Second},
		{"DNS, then QUIC", []int{53, 443}, 30 * time.Second},
		{"QUIC, then DNS", []int{443, 53}, 30 * time.Second},
		{"DNS, then an unrecognized port", []int{53, 8080}, udpTimeout},
		{"an unrecognized port, then DNS", []int{8080, 53}, udpTimeout},
	}
	for _, c := range cases {
		var idle time.Duration
		for _, port := range c.ports {
			idle = udpIdle(idle, port)
		}
		if idle != c.want {
			t.Errorf("%s: %s, want %s", c.name, idle, c.want)
		}
	}
}

// An app that only listens, as to a server pushing updates, keeps its association
// while replies arrive; once both directions go quiet, it still closes.
func TestUDPIdleCountsReplies(t *testing.T) {
	origin, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer origin.Close()
	// Set before the client starts, which reads it, and left in place for the same
	// reason.
	udpTimeouts[origin.LocalAddr().(*net.UDPAddr).Port] = time.Second
	go func() {
		buf := make([]byte, 1500)
		_, addr, err := origin.ReadFrom(buf)
		if err != nil {
			return
		}
		for end := time.Now().Add(3 * time.Second); time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
			origin.WriteTo([]byte("update"), addr)
		}
	}()
	key := make([]byte, proxy.PSKSize)
	rand.Read(key)
	psk := base64.StdEncoding.EncodeToString(key)
	client := startClient(t, startServer(t, psk), []byte(psk))

	control, relay, err := client.associate()
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	pc, err := net.DialUDP("udp", nil, relay)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	datagram, err := statute.NewDatagram(origin.LocalAddr().String(), []byte("subscribe"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pc.Write(datagram.Bytes()); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	var last time.Duration
	for buf := make([]byte, 64); ; {
		pc.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := pc.Read(buf); err != nil {
			break
		}
		last = time.Since(start)
	}
	if last < 2*time.Second {
		t.Fatalf("last update arrived %v after the app's only packet; want updates past the 1 s idle timeout", last)
	}
	control.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := control.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("control connection read %v; want the association closed once both directions went quiet", err)
	}
}
