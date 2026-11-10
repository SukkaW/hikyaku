package main

import (
	"errors"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
)

const (
	maxReplayBuffer = 64 << 10
	maxReplays      = 2
	// requestDelay is how long a new stream's request waits to share one write with
	// the app's first bytes. Client-first protocols (TLS, HTTP) send theirs sooner;
	// server-first ones (SSH, SMTP) get the request alone after this long.
	requestDelay = 10 * time.Millisecond
)

// replayStream relays one app connection over a carrier stream. Until the first
// response byte arrives it keeps a copy of what the app sent, so a stream whose
// carrier dies or goes silent can move to another carrier without the app noticing.
// Only streams that start with a TLS handshake record replay: resending one just
// starts a fresh TLS session at the origin, while a plain request could run twice.
type replayStream struct {
	h           *inbound
	destination proxy.Addr

	stream     atomic.Pointer[carrierStream]
	carrier    atomic.Pointer[carrier]
	replayable atomic.Bool
	answered   atomic.Bool
	moving     atomic.Bool // the carrier went silent, so the stream should move
	closed     atomic.Bool

	mu      sync.Mutex // serializes Write, buffering and replays
	started bool
	request []byte // not sent yet: waiting to ride with the app's first bytes
	flush   *time.Timer
	sent    []byte
	replays int
	// writeClosed records that the app half-closed, so a replay half-closes again.
	writeClosed bool
}

func newReplayStream(h *inbound, destination proxy.Addr) (*replayStream, error) {
	r := &replayStream{h: h, destination: destination}
	if err := r.attach(); err != nil {
		return nil, err
	}
	return r, nil
}

// attach opens a stream on the best carrier. A replay resends the request with what
// the app already sent; a new stream holds its request for the app's first bytes.
func (r *replayStream) attach() error {
	c, s, err := r.h.pool.openStream()
	if err != nil {
		return err
	}
	request, err := proxy.AppendConnect(nil, r.destination)
	if err != nil {
		s.Close()
		return err
	}
	if len(r.sent) > 0 {
		if _, err := s.Write(append(request, r.sent...)); err != nil {
			s.Close()
			return err
		}
	} else {
		r.request = request
		r.flush = time.AfterFunc(requestDelay, r.flushRequest)
	}
	r.stream.Store(s)
	r.carrier.Store(c)
	r.moving.Store(false)
	if r.replayable.Load() {
		c.track(r)
	}
	return nil
}

// flushRequest sends a request the app gave no bytes to ride with.
func (r *replayStream) flushRequest() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.request != nil {
		// A failed write shows up on the stream's next Read or Write.
		r.stream.Load().Write(r.request)
		r.request = nil
	}
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
	} else {
		r.sent = nil
	}
	s := r.stream.Load()
	out := b
	if r.request != nil {
		r.flush.Stop()
		out = append(r.request, b...)
		r.request = nil
	}
	_, err := s.Write(out)
	if err != nil && r.replayLocked(s) {
		// b went out with the buffer resent on the new stream.
		return len(b), nil
	}
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func (r *replayStream) Read(b []byte) (int, error) {
	for {
		s := r.stream.Load()
		n, err := s.Read(b)
		if n > 0 {
			r.markAnswered()
			return n, err
		}
		var failed *proxy.ResultError
		if errors.As(err, &failed) {
			log.Printf("%s: %v", r.destination, err)
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
// because its carrier is down or went silent, and reports whether to carry on with
// the new stream. The caller holds r.mu.
func (r *replayStream) replayLocked(s *carrierStream) bool {
	if s != r.stream.Load() {
		return true
	}
	c := r.carrier.Load()
	if r.answered.Load() || !r.replayable.Load() || r.closed.Load() || r.replays >= maxReplays ||
		!(c.down() || r.moving.Load()) {
		return false
	}
	r.replays++
	c.untrack(r)
	if !r.moving.Load() {
		// abandon closes a moving stream itself, after its RESET.
		s.Close()
	}
	if err := r.attach(); err != nil {
		return false
	}
	if r.writeClosed {
		r.stream.Load().CloseWrite()
	}
	if r.closed.Load() {
		// Close ran while the stream moved; attach tracked it on the new carrier.
		r.carrier.Load().untrack(r)
		r.stream.Load().Close()
		return false
	}
	return true
}

// markAnswered must not wait for r.mu: a Write blocked on a full window holds it, and
// that Write may only drain once the app reads this response. The next Write drops
// the replay buffer if r.mu is busy now.
func (r *replayStream) markAnswered() {
	if r.answered.Swap(true) {
		return
	}
	r.carrier.Load().untrack(r)
	if r.mu.TryLock() {
		r.sent = nil
		r.mu.Unlock()
	}
}

// stopReplay gives up on replaying once the app sends more than the buffer holds.
// The caller holds r.mu.
func (r *replayStream) stopReplay() {
	r.replayable.Store(false)
	r.sent = nil
	r.carrier.Load().untrack(r)
}

// abandon moves an unanswered replayable stream off a carrier that went silent.
// The read deadline wakes the stream's reader to move now; the old stream is reset
// before its FIN, off the pool's path, as the carrier's writes may be stuck.
func (r *replayStream) abandon(c *carrier) {
	if r.carrier.Load() == c && r.replayable.Load() && !r.answered.Load() {
		r.moving.Store(true)
		s := r.stream.Load()
		s.SetReadDeadline(time.Now())
		go func() {
			s.Abort()
			s.Close()
		}()
	}
}

func (r *replayStream) Close() error {
	r.closed.Store(true)
	if r.flush != nil {
		r.flush.Stop()
	}
	r.carrier.Load().untrack(r)
	return r.stream.Load().Close()
}

// CloseWrite half-closes the stream, sending first a request that is still waiting
// for the app's first bytes.
func (r *replayStream) CloseWrite() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writeClosed = true
	s := r.stream.Load()
	if r.request != nil {
		r.flush.Stop()
		// A failed write shows up on the stream's next Read.
		s.Write(r.request)
		r.request = nil
	}
	return s.CloseWrite()
}

func (r *replayStream) Abort() { r.stream.Load().Abort() }

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
