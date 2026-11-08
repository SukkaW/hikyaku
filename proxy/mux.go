package proxy

import (
	"sync"
	"time"

	"github.com/xtaci/smux"
)

// smux writes each frame with its 8-byte header in a single Write, so this keeps
// every frame inside one encrypted chunk.
const maxFramePayload = maxChunk - 8

// Pong is the first byte the server sends on every stream, as soon as it has read
// the request and before the website answers, so the client can time the carrier.
const Pong byte = 0

// KeepAliveInterval is how often each end sends smux keepalives; the client times
// the server's to estimate queueing on each carrier.
const KeepAliveInterval = 2 * time.Second

// smuxNOP is smux's keepalive command; each keepalive arrives as one 8-byte frame
// in a chunk of its own.
const smuxNOP = 3

func MuxConfig() *smux.Config {
	c := smux.DefaultConfig()
	c.Version = 2
	c.MaxFrameSize = maxFramePayload
	// The per-stream window must exceed the GEO bandwidth-delay product
	// (5 Mbit/s × 620 ms ≈ 388 KB), otherwise it caps single-stream throughput.
	c.MaxStreamBuffer = 1 << 20
	c.MaxReceiveBuffer = 8 << 20
	// A steady keepalive gives the client a heartbeat to spot silent carriers; each
	// one is a 42-byte chunk, which costs nothing even on a capped link.
	c.KeepAliveInterval = KeepAliveInterval
	return c
}

// heartbeat turns the peer's fixed-interval keepalives into a queueing estimate:
// how much later the latest one arrived than the schedule its earliest ones set.
type heartbeat struct {
	mu     sync.Mutex
	first  time.Time
	beats  int64
	minLag time.Duration
	lag    time.Duration
}

func (h *heartbeat) observe(t time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.beats == 0 {
		h.first = t
	}
	lag := t.Sub(h.first) - time.Duration(h.beats)*KeepAliveInterval
	h.minLag = min(h.minLag, lag)
	h.lag = lag - h.minLag
	h.beats++
}

func (h *heartbeat) current() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lag
}
