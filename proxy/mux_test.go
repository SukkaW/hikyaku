package proxy

import (
	"io"
	"testing"
	"time"

	"github.com/xtaci/smux"
)

// Five streams whose app read once and then stopped, like a paused video, hold 40 MiB
// between them; the carrier must keep delivering another stream's data and the
// peer's control messages.
func TestStalledStreamsKeepCarrierReading(t *testing.T) {
	psk := newPSK()
	clientRaw, serverRaw := tcpPair(t)
	result := serve(serverRaw, psk, NewReplayFilter())
	client, err := ClientHandshake(clientRaw, psk)
	if err != nil {
		t.Fatal(err)
	}
	r := <-result
	if r.err != nil {
		t.Fatal(r.err)
	}
	control := make(chan byte, 1)
	client.OnControl(func(cmd byte, _ []byte) {
		select {
		case control <- cmd:
		default:
		}
	})
	clientSession, err := smux.Client(client, MuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer clientSession.Close()
	serverSession, err := smux.Server(r.conn, MuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer serverSession.Close()

	const stalled = 5
	var streams []*smux.Stream
	for range stalled + 1 {
		s, err := clientSession.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Write([]byte{0}); err != nil {
			t.Fatal(err)
		}
		streams = append(streams, s)
	}
	window := MuxConfig().MaxStreamBuffer
	filled := make(chan struct{}, stalled)
	var live *smux.Stream
	for i := range stalled + 1 {
		s, err := serverSession.AcceptStream()
		if err != nil {
			t.Fatal(err)
		}
		if i == stalled {
			live = s
			break
		}
		go func() {
			if _, err := s.Write(make([]byte, window+1)); err == nil {
				filled <- struct{}{}
			}
		}()
	}
	// A stream's first read opens its window to the full MaxStreamBuffer.
	for _, s := range streams[:stalled] {
		s.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(s, make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
	}
	// Wait until every stalled stream's window is full, or until it is clear the
	// carrier stopped taking data.
	deadline := time.After(3 * time.Second)
wait:
	for range stalled {
		select {
		case <-filled:
		case <-deadline:
			break wait
		}
	}

	// A frozen carrier blocks these writes, so the reads below are what fail.
	go live.Write([]byte("ok"))
	go r.conn.WriteControl(CtrlPing, []byte{0, 0, 0, 1})
	reader := streams[stalled]
	reader.SetReadDeadline(time.Now().Add(5 * time.Second))
	got := make([]byte, 2)
	if _, err := io.ReadFull(reader, got); err != nil || string(got) != "ok" {
		t.Fatalf("read %q, %v; want another stream's data to arrive", got, err)
	}
	select {
	case <-control:
	case <-time.After(5 * time.Second):
		t.Fatal("the peer's control message never arrived")
	}
}
