package proxy

import (
	"time"

	"github.com/xtaci/smux"
)

// smux writes each frame with its 8-byte header in a single Write, so this keeps
// every frame inside one encrypted chunk.
const maxFramePayload = maxChunk - 8

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
	c.KeepAliveInterval = 2 * time.Second
	return c
}
