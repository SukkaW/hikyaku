package main

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

const (
	maxReplayBuffer = 64 << 10
	maxReplays      = 2
)

// replayStream relays one app connection over a carrier stream. Until the first
// response byte arrives it keeps a copy of what the app sent, so a stream whose
// carrier dies or goes silent can move to another carrier without the app noticing.
// Only streams that start with a TLS handshake record replay: resending one just
// starts a fresh TLS session at the origin, while a plain request could run twice.
type replayStream struct {
	h           *inbound
	destination M.Socksaddr

	stream     atomic.Pointer[carrierStream]
	carrier    atomic.Pointer[carrier]
	replayable atomic.Bool
	answered   atomic.Bool
	moving     atomic.Bool // the carrier went silent, so the stream should move
	closed     atomic.Bool

	mu      sync.Mutex // serializes Write, buffering and replays
	started bool
	sent    []byte
	replays int
}

func newReplayStream(h *inbound, destination M.Socksaddr) (*replayStream, error) {
	r := &replayStream{h: h, destination: destination}
	if err := r.attach(); err != nil {
		return nil, err
	}
	return r, nil
}

// attach opens a stream on the best carrier and resends what the app already sent.
func (r *replayStream) attach() error {
	c, s, err := r.h.open(r.destination)
	if err != nil {
		return err
	}
	if len(r.sent) > 0 {
		if _, err := s.Write(r.sent); err != nil {
			s.Close()
			return err
		}
	}
	r.stream.Store(s)
	r.carrier.Store(c)
	r.moving.Store(false)
	if r.replayable.Load() {
		c.track(r)
	}
	return nil
}

func (r *replayStream) Write(b []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.started {
		r.started = true
		if len(b) >= 2 && b[0] == 0x16 && b[1] == 0x03 {
			r.replayable.Store(true)
			r.carrier.Load().track(r)
		}
	}
	if r.replayable.Load() && !r.answered.Load() {
		if len(r.sent)+len(b) <= maxReplayBuffer {
			r.sent = append(r.sent, b...)
		} else {
			r.stopReplay()
		}
	}
	s := r.stream.Load()
	n, err := s.Write(b)
	if err != nil && r.replayLocked(s) {
		// b went out with the buffer resent on the new stream.
		return len(b), nil
	}
	return n, err
}

func (r *replayStream) Read(b []byte) (int, error) {
	for {
		s := r.stream.Load()
		n, err := s.Read(b)
		if n > 0 {
			r.markAnswered()
			return n, err
		}
		r.mu.Lock()
		replayed := err != nil && r.replayLocked(s)
		r.mu.Unlock()
		if !replayed {
			return n, err
		}
	}
}

// replayLocked moves to another carrier if s failed before any response arrived
// because its carrier closed or went silent, and reports whether to carry on with
// the new stream. The caller holds r.mu.
func (r *replayStream) replayLocked(s *carrierStream) bool {
	if s != r.stream.Load() {
		return true
	}
	c := r.carrier.Load()
	if r.answered.Load() || !r.replayable.Load() || r.closed.Load() || r.replays >= maxReplays ||
		!(c.session.IsClosed() || r.moving.Load()) {
		return false
	}
	r.replays++
	c.untrack(r)
	s.Close()
	if err := r.attach(); err != nil {
		return false
	}
	if r.closed.Load() {
		r.stream.Load().Close()
		return false
	}
	return true
}

func (r *replayStream) markAnswered() {
	if r.answered.Load() {
		return
	}
	r.mu.Lock()
	r.answered.Store(true)
	r.sent = nil
	r.carrier.Load().untrack(r)
	r.mu.Unlock()
}

// stopReplay gives up on replaying once the app sends more than the buffer holds.
// The caller holds r.mu.
func (r *replayStream) stopReplay() {
	r.replayable.Store(false)
	r.sent = nil
	r.carrier.Load().untrack(r)
}

// abandon moves an unanswered replayable stream off a carrier that went silent.
func (r *replayStream) abandon(c *carrier) {
	if r.carrier.Load() == c && r.replayable.Load() && !r.answered.Load() {
		r.moving.Store(true)
		r.stream.Load().Close()
	}
}

func (r *replayStream) Close() error {
	r.closed.Store(true)
	r.carrier.Load().untrack(r)
	return r.stream.Load().Close()
}

func (r *replayStream) LocalAddr() net.Addr               { return r.stream.Load().LocalAddr() }
func (r *replayStream) RemoteAddr() net.Addr              { return r.stream.Load().RemoteAddr() }
func (r *replayStream) SetDeadline(t time.Time) error     { return r.stream.Load().SetDeadline(t) }
func (r *replayStream) SetReadDeadline(t time.Time) error { return r.stream.Load().SetReadDeadline(t) }
func (r *replayStream) SetWriteDeadline(t time.Time) error {
	return r.stream.Load().SetWriteDeadline(t)
}

func (c *carrier) track(r *replayStream) {
	c.mu.Lock()
	if c.unanswered == nil {
		c.unanswered = make(map[*replayStream]struct{})
	}
	c.unanswered[r] = struct{}{}
	c.mu.Unlock()
}

func (c *carrier) untrack(r *replayStream) {
	c.mu.Lock()
	delete(c.unanswered, r)
	c.mu.Unlock()
}

// moveUnanswered sends streams that have not heard back yet to another carrier.
func (c *carrier) moveUnanswered() {
	c.mu.Lock()
	streams := make([]*replayStream, 0, len(c.unanswered))
	for r := range c.unanswered {
		streams = append(streams, r)
	}
	c.mu.Unlock()
	for _, r := range streams {
		r.abandon(c)
	}
}
