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

func checkStages(t *testing.T, sizes []int) {
	t.Helper()
	if len(sizes) <= 2*drsThreshold {
		t.Fatalf("only %d frames written", len(sizes))
	}
	reachedMax := false
	for i, size := range sizes {
		if limit := drsFrameSize(i); size > limit {
			t.Fatalf("frame %d is %d bytes, over the %d-byte limit for its stage", i, size, limit)
		}
		reachedMax = reachedMax || size == maxFramePayload
	}
	if !reachedMax {
		t.Fatal("frame size never grew to the maximum")
	}
}

func TestDRSFrameSizes(t *testing.T) {
	w := &sizeRecorder{}
	if err := copyDRS(w, &burstReader{total: 40, pauseAt: -1}); err != nil {
		t.Fatal(err)
	}
	checkStages(t, w.sizes)
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
	checkStages(t, w.sizes[:idx])
	checkStages(t, w.sizes[idx:])
	if w.sizes[idx] != drsLow {
		t.Fatalf("first frame after idle is %d bytes, want %d", w.sizes[idx], drsLow)
	}
}
