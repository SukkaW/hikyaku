package proxy

import (
	"testing"
	"time"
)

func TestHeartbeatLag(t *testing.T) {
	start := time.Unix(1000, 0)
	at := func(seconds float64) time.Time {
		return start.Add(time.Duration(seconds * float64(time.Second)))
	}
	cases := []struct {
		arrival float64
		want    time.Duration
	}{
		{0, 0},
		{2, 0},
		{4, 0},
		{7.5, 1500 * time.Millisecond}, // due at 6 s: 1.5 s of queueing
		{8.1, 100 * time.Millisecond},
		{9.9, 0}, // early: the schedule shifts, so the first beat was itself late
		{12.4, 500 * time.Millisecond},
	}
	var h heartbeat
	for _, c := range cases {
		h.observe(at(c.arrival))
		if got := h.current(); got != c.want {
			t.Fatalf("after a keepalive at %.1fs: lag %v, want %v", c.arrival, got, c.want)
		}
	}
}
