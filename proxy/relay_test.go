package proxy

import (
	"io"
	"runtime"
	"strings"
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
	// Each read fills one frame, so each is exactly as many segments as its position.
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
	// Each read is one frame, so the second burst starts at frame 40.
	checkRamp(t, w.sizes[:40])
	checkRamp(t, w.sizes[40:])
}

func TestDRSSmallTransferKeepsSmallBuffer(t *testing.T) {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if err := copyDRS(io.Discard, strings.NewReader("GET / HTTP/1.1\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 8<<10 {
		t.Fatalf("a small transfer allocated %d bytes, want the buffer to stay at the first record size", allocated)
	}
}
