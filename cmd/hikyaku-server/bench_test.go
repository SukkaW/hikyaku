package main

import (
	"io"
	"net"
	"testing"
)

// BenchmarkDownload measures the server's whole download path in one process: a
// local origin, the server's relay, smux and the encrypted carrier to a client
// session that discards what it reads.
func BenchmarkDownload(b *testing.B) {
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
	stream := openStream(b, true, addrOf(b, origin.Addr()))
	readPong(b, stream)
	const step = 1 << 20
	b.SetBytes(step)
	b.ResetTimer()
	for range b.N {
		if _, err := io.CopyN(io.Discard, stream, step); err != nil {
			b.Fatal(err)
		}
	}
}
