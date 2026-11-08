package main

import (
	std_bufio "bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
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
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/protocol/socks"
)

func startServer(t *testing.T, psk string) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "hikyaku-server")
	if out, err := exec.Command("go", "build", "-o", bin, "github.com/SukkaW/hikyaku/cmd/hikyaku-server").CombinedOutput(); err != nil {
		t.Fatalf("build server: %v\n%s", err, out)
	}
	cmd := exec.Command(bin, "-listen", "127.0.0.1:0", "-allow-private", "-psk", psk)
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

func startClient(t *testing.T, server string, psk []byte) *socks.Client {
	t.Helper()
	p := newPool(server, psk, 2)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go (&inbound{pool: p}).serve(ln)
	return socks.NewClient(N.SystemDialer, M.ParseSocksaddr(ln.Addr().String()), socks.Version5, "", "")
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
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return client.DialContext(ctx, network, M.ParseSocksaddr(addr))
			},
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
		conn, err := client.DialContext(context.Background(), "tcp", M.ParseSocksaddr(origin.Addr().String()))
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
		pc, err := client.ListenPacket(context.Background(), M.SocksaddrFromNet(echo.LocalAddr()))
		if err != nil {
			t.Fatal(err)
		}
		defer pc.Close()
		if _, err := pc.WriteTo([]byte("ping"), echo.LocalAddr()); err != nil {
			t.Fatal(err)
		}
		pc.SetReadDeadline(time.Now().Add(5 * time.Second))
		got := make([]byte, 16)
		n, _, err := pc.ReadFrom(got)
		if err != nil || string(got[:n]) != "ping" {
			t.Fatalf("udp echo %q, %v", got[:n], err)
		}
	})
}
