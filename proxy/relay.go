package proxy

import (
	"errors"
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

// Relay copies between a local connection and a carrier stream until both directions
// end. A direction that ends cleanly half-closes where it was going, so the other
// keeps flowing; one that ends in an error resets the local connection and, through
// stream's Abort, the peer's end, so a cut-off transfer never looks complete. A
// cancelled download stops too: the next write to the closed app fails.
func Relay(local, stream net.Conn) {
	var once sync.Once
	end := func(err error) {
		once.Do(func() {
			if err != nil {
				abort(local)
				if s, ok := stream.(interface{ Abort() }); ok && !errors.Is(err, ErrReset) {
					s.Abort()
				}
			}
			local.Close()
			stream.Close()
		})
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := copyDRS(stream, local); err != nil {
			end(err)
		} else {
			closeWrite(stream)
		}
	}()
	if _, err := io.Copy(local, stream); err != nil {
		end(err)
	} else {
		closeWrite(local)
	}
	<-done
	end(nil)
}

func closeWrite(conn net.Conn) {
	if c, ok := conn.(interface{ CloseWrite() error }); ok {
		c.CloseWrite()
	}
}

// abort makes conn's Close send a reset instead of a FIN, where conn supports that.
func abort(conn net.Conn) {
	if l, ok := conn.(interface{ SetLinger(sec int) error }); ok {
		l.SetLinger(0)
	}
}

func copyDRS(dst io.Writer, src io.Reader) error {
	// The buffer grows with the record-size ramp, so a connection that only ever sends
	// small records keeps a small one.
	buf := make([]byte, drsFirst)
	limit := drsFirst
	var lastWrite time.Time
	for {
		if cap(buf) < limit {
			buf = make([]byte, min(max(2*cap(buf), limit), maxFramePayload))
		}
		n, err := src.Read(buf[:limit])
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
