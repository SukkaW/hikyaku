package main

import (
	"context"
	"io"
	"net"
	"testing"
)

// BenchmarkSOCKSDownload measures the client's whole download path: SOCKS5, the
// replayable stream, the carrier pool, smux and the encrypted carrier. The server
// runs as a separate process, so a CPU profile shows the client's work alone.
func BenchmarkSOCKSDownload(b *testing.B) {
	psk := "socks-download-benchmark"
	client := startClient(b, startServer(b, psk), []byte(psk))
	origin, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { origin.Close() })
	go func() {
		chunk := make([]byte, 32<<10)
		for {
			conn, err := origin.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				for {
					if _, err := conn.Write(chunk); err != nil {
						return
					}
				}
			}()
		}
	}()
	conn, err := client.DialContext(context.Background(), "tcp", origin.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { conn.Close() })
	const step = 1 << 20
	b.SetBytes(step)
	b.ResetTimer()
	for range b.N {
		if _, err := io.CopyN(io.Discard, conn, step); err != nil {
			b.Fatal(err)
		}
	}
}
