package main

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
	"github.com/xtaci/smux"
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

func TestPingInterval(t *testing.T) {
	cases := []struct {
		name                   string
		sinceData, sinceStream time.Duration
		streams                int
		want                   time.Duration
	}{
		{"data flowing", 0, time.Hour, 1, busyPing},
		{"quiet with a stream open", time.Second, time.Hour, 1, warmPing},
		{"quiet soon after a stream closed", time.Second, 10 * time.Second, 0, warmPing},
		{"idle", time.Minute, time.Minute, 0, coldPing},
	}
	for _, c := range cases {
		if got := pingInterval(c.sinceData, c.sinceStream, c.streams); got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
}

func TestChoose(t *testing.T) {
	ms := time.Millisecond
	cases := []struct {
		name    string
		waits   []time.Duration
		streams []int
		want    int
	}{
		{"a clearly quicker carrier beats fewer streams", []time.Duration{600 * ms, 1200 * ms}, []int{3, 0}, 0},
		{"within the band, fewest streams wins", []time.Duration{600 * ms, 700 * ms}, []int{3, 0}, 1},
		{"the band is at least minWaitBand", []time.Duration{30 * ms, 45 * ms}, []int{2, 0}, 1},
		{"just outside a small band", []time.Duration{30 * ms, 60 * ms}, []int{2, 0}, 0},
		{"an unknown wait counts as tied", []time.Duration{-1, 600 * ms}, []int{0, 2}, 0},
		{"all unknown falls back to fewest streams", []time.Duration{-1, -1}, []int{1, 0}, 1},
		{"equal carriers keep slot order", []time.Duration{600 * ms, 600 * ms}, []int{1, 1}, 0},
	}
	for _, c := range cases {
		if got := choose(c.waits, c.streams); got != c.want {
			t.Errorf("%s: chose %d, want %d", c.name, got, c.want)
		}
	}
}

func TestExpectedWait(t *testing.T) {
	now := time.Now()
	c := &carrier{pings: make(map[uint32]time.Time), waiting: make(map[*carrierStream]time.Time)}
	if got := c.expectedWait(now); got >= 0 {
		t.Fatalf("no samples and nothing outstanding: %s, want unknown", got)
	}
	c.sample(600 * time.Millisecond)
	if got := c.expectedWait(now); got != 600*time.Millisecond {
		t.Fatalf("latest round trip only: %s, want 600ms", got)
	}
	c.pings[1] = now.Add(-2 * time.Second)
	if got := c.expectedWait(now); got != 2*time.Second {
		t.Fatalf("with a ping unanswered for 2s: %s, want 2s", got)
	}
	delete(c.pings, 1)
	c.waiting[&carrierStream{}] = now.Add(-time.Second)
	if got := c.expectedWait(now); got != time.Second {
		t.Fatalf("with an acknowledgement 1s overdue: %s, want 1s", got)
	}
}

// fakeServer accepts carriers and runs smux on them like the real server, but answers
// no pings; onCarrier runs for each one.
func fakeServer(t *testing.T, psk string, onCarrier func(*proxy.Conn)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { conn.Close() })
			go func() {
				c, err := proxy.ServerHandshake(conn, []byte(psk), proxy.NewReplayFilter())
				if err != nil {
					return
				}
				if _, err := smux.Server(c, proxy.MuxConfig()); err != nil {
					return
				}
				onCarrier(c)
			}()
		}
	}()
	return ln.Addr().String()
}

func (p *pool) standing() []*carrier {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*carrier(nil), p.standby...)
}

func TestUnansweredPingStandsBy(t *testing.T) {
	psk := "unanswered-ping-test"
	p := newPool(fakeServer(t, psk, func(*proxy.Conn) {}), []byte(psk), 1)
	p.warm()
	waitFor(t, "the carrier acknowledged", func() bool { return p.acked() == 1 })
	deadline := time.Now().Add(healthCheck + pongTimeout + 3*time.Second)
	for len(p.standing()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("a carrier whose pings go unanswered kept its slot")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestGoAwayStandsBy(t *testing.T) {
	psk := "goaway-test"
	p := newPool(fakeServer(t, psk, func(c *proxy.Conn) {
		c.WriteControl(proxy.CtrlGoAway, []byte{proxy.GoAwayShutdown, 4, 't', 'e', 's', 't'})
	}), []byte(psk), 1)
	p.warm()
	waitFor(t, "the carrier to stand by", func() bool { return len(p.standing()) > 0 })
	if reason := p.standing()[0].goaway.Load(); reason == nil || *reason != "test" {
		t.Fatalf("standby carrier's GOAWAY reason %v, want the server's", reason)
	}
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
