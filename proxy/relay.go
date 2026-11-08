package proxy

import (
	"io"
	"net"
	"sync"
	"time"
)

// Dynamic record sizing, after Cloudflare's nginx patch: the first frames of a burst
// fit one TCP segment so the receiver can use them without waiting for the rest.
// Each frame costs 42 bytes (34 for the chunk, 8 for the smux header).
const (
	drsLow       = 1388 // (1500 - 40 IPv6 - 20 TCP - 10 timestamps) - 42
	drsHigh      = 4248 // 3 × 1430 - 42
	drsThreshold = 40
	drsIdleReset = time.Second
)

// Relay copies between a local connection and a carrier stream, then closes both.
// smux sends the same FIN for Close and CloseWrite, so the peer cannot tell a
// half-close from an abort; the first direction to finish therefore tears down
// both, which keeps a cancelled download from streaming on into a dead stream.
func Relay(local, stream net.Conn) {
	var once sync.Once
	closeBoth := func() {
		local.Close()
		stream.Close()
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		copyDRS(stream, local)
		once.Do(closeBoth)
	}()
	io.Copy(local, stream)
	once.Do(closeBoth)
	<-done
}

func copyDRS(dst io.Writer, src io.Reader) error {
	buf := make([]byte, maxFramePayload)
	var frames int
	var lastWrite time.Time
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if time.Since(lastWrite) > drsIdleReset {
				frames = 0
			}
			for p := buf[:n]; len(p) > 0; frames++ {
				size := min(len(p), drsFrameSize(frames))
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

func drsFrameSize(frames int) int {
	switch {
	case frames < drsThreshold:
		return drsLow
	case frames < 2*drsThreshold:
		return drsHigh
	default:
		return maxFramePayload
	}
}
