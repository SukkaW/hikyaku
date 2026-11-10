package proxy

import "github.com/xtaci/smux"

// smux writes each frame with its 8-byte header in a single Write, so this keeps
// every frame inside one encrypted chunk.
const maxFramePayload = maxChunk - 8

func MuxConfig() *smux.Config {
	c := smux.DefaultConfig()
	c.Version = 2
	c.MaxFrameSize = maxFramePayload
	// The per-stream window must exceed the path's bandwidth-delay product, otherwise
	// it caps single-stream throughput: 8 MiB covers 500 Mbit/s at 50 ms (≈3 MB) even
	// though smux only refreshes it after half is used. Windows are limits, not
	// allocations.
	c.MaxStreamBuffer = 8 << 20
	// Once a carrier's streams hold this much unread data, smux stops reading the
	// carrier, which freezes every stream on it and its control messages too. Each
	// stream's window already caps what it can hold, so, as in Go's HTTP/2 client
	// (1 GiB per connection, 4 MiB per stream), this is set where it never binds.
	c.MaxReceiveBuffer = 1 << 30
	// The client pings each carrier itself, outside smux and only when it would
	// otherwise be quiet, so smux's fixed-interval keepalive is off.
	c.KeepAliveDisabled = true
	return c
}
