package proxy

import (
	"io"
	"net"
	"sync"
	"time"
)

// Dynamic record sizing, after Cloudflare's nginx patch: a burst starts with a frame
// that fits one TCP segment and each later frame is one segment larger, so the
// receiver can use what has arrived without waiting for the rest.
// Each frame costs 42 bytes (34 for the chunk, 8 for the smux header).
const (
	drsSegment   = 1428 // 1500 - 40 IPv6 - 20 TCP - 12 for timestamps as sent; IPv4 segments are larger
	drsFirst     = drsSegment - 42
	drsIdleReset = time.Second
)

// Relay copies between a local connection and a carrier stream, then closes both.
// smux sends the same FIN for Close and CloseWrite, so the peer cannot tell a
// half-close from an abort; the first direction to finish therefore tears down
// both, which keeps a cancelled download from streaming on into a dead stream.
// A direction that ends in an error rather than a clean end resets the local
// connection instead, so a response cut off by a dead carrier does not look complete.
func Relay(local, stream net.Conn) {
	var once sync.Once
	closeBoth := func(err error) {
		once.Do(func() {
			if err != nil {
				abort(local)
			}
			local.Close()
			stream.Close()
		})
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		closeBoth(copyDRS(stream, local))
	}()
	_, err := io.Copy(local, stream)
	closeBoth(err)
	<-done
}

// abort makes conn's Close send a reset instead of a FIN, where conn supports that.
func abort(conn net.Conn) {
	if l, ok := conn.(interface{ SetLinger(sec int) error }); ok {
		l.SetLinger(0)
	}
}

func copyDRS(dst io.Writer, src io.Reader) error {
	buf := make([]byte, maxFramePayload)
	limit := drsFirst
	var lastWrite time.Time
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if time.Since(lastWrite) > drsIdleReset {
				limit = drsFirst
			}
			for p := buf[:n]; len(p) > 0; limit = min(limit+drsSegment, maxFramePayload) {
				size := min(len(p), limit)
				if _, err := dst.Write(p[:size]); err != nil {
					return err
				}
				p = p[size:]
			}
			lastWrite = time.Now()
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
