package proxy

import (
	"io"
	"testing"
	"time"
)

type sizeRecorder struct{ sizes []int }

func (w *sizeRecorder) Write(b []byte) (int, error) {
	w.sizes = append(w.sizes, len(b))
	return len(b), nil
}

// burstReader returns full buffers, sleeping before the read at index pauseAt.
type burstReader struct {
	reads   int
	total   int
	pauseAt int
	pause   time.Duration
}

func (r *burstReader) Read(b []byte) (int, error) {
	if r.reads == r.total {
		return 0, io.EOF
	}
	if r.reads == r.pauseAt {
		time.Sleep(r.pause)
	}
	r.reads++
	for i := range b {
		b[i] = byte(i)
	}
	return len(b), nil
}

func checkRamp(t *testing.T, sizes []int) {
	t.Helper()
	reachedMax := false
	for i, size := range sizes {
		if limit := min(drsFirst+i*drsSegment, maxFramePayload); size > limit {
			t.Fatalf("frame %d is %d bytes, over its %d-byte limit", i, size, limit)
		}
		reachedMax = reachedMax || size == maxFramePayload
	}
	if !reachedMax {
		t.Fatal("frame size never grew to the maximum")
	}
	// The first read fills all four, so each is exactly as many segments as its position.
	for i := range 4 {
		if want := (i+1)*drsSegment - 42; sizes[i] != want {
			t.Fatalf("frame %d is %d bytes, want %d", i, sizes[i], want)
		}
	}
}

func TestDRSFrameSizes(t *testing.T) {
	w := &sizeRecorder{}
	if err := copyDRS(w, &burstReader{total: 40, pauseAt: -1}); err != nil {
		t.Fatal(err)
	}
	checkRamp(t, w.sizes)
}

func TestDRSResetsAfterIdle(t *testing.T) {
	w := &sizeRecorder{}
	r := &burstReader{total: 80, pauseAt: 40, pause: drsIdleReset + 100*time.Millisecond}
	if err := copyDRS(w, r); err != nil {
		t.Fatal(err)
	}
	// Frames never straddle reads, so the second burst starts right after 40 full reads.
	written, idx := 0, 0
	for idx < len(w.sizes) && written < 40*maxFramePayload {
		written += w.sizes[idx]
		idx++
	}
	checkRamp(t, w.sizes[:idx])
	checkRamp(t, w.sizes[idx:])
}
