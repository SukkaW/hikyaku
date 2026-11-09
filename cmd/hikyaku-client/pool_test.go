package main

import (
	"sync"
	"testing"
	"time"
)

func TestPoolKeepsSpare(t *testing.T) {
	psk := "spare-carrier-test"
	p := newPool(startServer(t, psk), []byte(psk), 4)

	// The first stream rides the first carrier while it is still unacknowledged.
	_, first, err := p.openStream()
	if err != nil {
		t.Fatal(err)
	}
	first.Close()
	waitFor(t, "first carrier acknowledged", func() bool { return p.acked() >= 1 })

	_, second, err := p.openStream()
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	waitFor(t, "an idle spare beside the busy carrier", func() bool { return p.acked() >= 2 })
	p.mu.Lock()
	opened := p.opened
	p.mu.Unlock()
	if opened != 2 {
		t.Fatalf("opened %d slots, want 2: one busy carrier and one spare", opened)
	}
}

func TestPoolWarm(t *testing.T) {
	psk := "warm-pool-test"
	p := newPool(startServer(t, psk), []byte(psk), 4)
	p.warm()
	waitFor(t, "the first carrier acknowledged without any request", func() bool { return p.acked() == 1 })
	p.mu.Lock()
	opened := p.opened
	p.mu.Unlock()
	if opened != 1 {
		t.Fatalf("opened %d slots, want 1", opened)
	}
}

// A burst that arrives while the first carrier is still connecting, as when Surge
// starts the client and sends requests at once, waits for it instead of failing.
func TestPoolWarmBurst(t *testing.T) {
	psk := "warm-burst-test"
	p := newPool(startServer(t, psk), []byte(psk), 4)
	p.warm()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, s, err := p.openStream()
			if err != nil {
				t.Error(err)
				return
			}
			s.Close()
		}()
	}
	wg.Wait()
}

func TestPoolWakesWaiters(t *testing.T) {
	psk := "wake-waiters-test"
	p := newPool(startServer(t, psk), []byte(psk), 1)
	p.opened = 1 // the test supplies the carrier itself, so it decides when one appears
	done := make(chan error, 1)
	go func() {
		_, s, err := p.openStream()
		if err == nil {
			s.Close()
		}
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	c, err := p.dial()
	if err != nil {
		t.Fatal(err)
	}
	defer c.session.Close()
	appeared := time.Now()
	p.set(0, c)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		if waited := time.Since(appeared); waited >= healthCheck/2 {
			t.Fatalf("waiter woke %s after the carrier appeared, so it relied on the %s fallback", waited, healthCheck)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter never woke")
	}
}

func (p *pool) acked() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, c := range p.carriers {
		if c != nil && c.conn.Acked() {
			n++
		}
	}
	return n
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
