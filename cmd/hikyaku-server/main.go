package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
	"github.com/xtaci/smux"
)

const (
	handshakeTimeout = 10 * time.Second
	shutdownGrace    = 5 * time.Second
	// idleTimeout closes a carrier the client stopped pinging; an idle carrier is
	// pinged every 15 s, so this is four missed pings.
	idleTimeout = 60 * time.Second
)

// live tracks open carriers, so a server that is shutting down can tell each client.
var live = struct {
	sync.Mutex
	carriers map[*proxy.Conn]*smux.Session
}{carriers: make(map[*proxy.Conn]*smux.Session)}

func main() {
	listen := flag.String("listen", ":8444", "carrier listen address")
	psk := flag.String("psk", "", "pre-shared key, any string")
	genkey := flag.Bool("genkey", false, "print a new random PSK and exit")
	flag.Parse()

	if *genkey {
		key := make([]byte, proxy.PSKSize)
		rand.Read(key)
		fmt.Println(base64.StdEncoding.EncodeToString(key))
		return
	}
	if *psk == "" {
		log.Fatal("-psk is required")
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("listening on %s", ln.Addr())
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signals
		ln.Close()
		goAway()
		os.Exit(0)
	}()
	if err := serve(ln, []byte(*psk), newOutbound()); !errors.Is(err, net.ErrClosed) {
		log.Fatal(err)
	}
	// The listener closed on a signal; the handler exits once carriers have gone away.
	select {}
}

// goAway tells every client its carriers are going away, then gives their streams up
// to shutdownGrace to finish.
func goAway() {
	const reason = "server shutting down"
	body := append([]byte{proxy.GoAwayShutdown, byte(len(reason))}, reason...)
	live.Lock()
	sessions := make([]*smux.Session, 0, len(live.carriers))
	for c, session := range live.carriers {
		// A carrier whose writes are stuck must not hold up the others.
		go c.WriteControl(proxy.CtrlGoAway, body)
		sessions = append(sessions, session)
	}
	live.Unlock()
	log.Printf("shutting down: told %d carriers to go away", len(sessions))
	deadline := time.Now().Add(shutdownGrace)
	for time.Now().Before(deadline) {
		streams := 0
		for _, session := range sessions {
			streams += session.NumStreams()
		}
		if streams == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func serve(ln net.Listener, psk []byte, out *outbound) error {
	ln = proxy.KeepAccepting(ln)
	replay := proxy.NewReplayFilter()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go serveCarrier(conn, psk, replay, out)
	}
}

func serveCarrier(conn net.Conn, psk []byte, replay *proxy.ReplayFilter, out *outbound) {
	defer conn.Close()
	if err := proxy.TuneCarrier(conn); err != nil {
		log.Printf("carrier %s: %v", conn.RemoteAddr(), err)
	}
	conn.SetDeadline(time.Now().Add(handshakeTimeout))
	c, err := proxy.ServerHandshake(conn, psk, replay)
	if err != nil {
		log.Printf("carrier %s: handshake: %v", conn.RemoteAddr(), err)
		return
	}
	conn.SetDeadline(time.Time{})
	pongs := make(chan []byte, 8)
	streams := newStreams()
	c.OnControl(func(cmd byte, body []byte) {
		switch cmd {
		case proxy.CtrlPing:
			select {
			case pongs <- bytes.Clone(body):
			default: // never block the reader; the client pings again
			}
		case proxy.CtrlReset:
			if id, _, ok := proxy.ParseReset(body); ok {
				streams.reset(id)
			}
		}
	})
	session, err := smux.Server(c, proxy.MuxConfig())
	if err != nil {
		log.Printf("carrier %s: %v", conn.RemoteAddr(), err)
		return
	}
	defer session.Close()
	live.Lock()
	live.carriers[c] = session
	live.Unlock()
	defer func() {
		live.Lock()
		delete(live.carriers, c)
		live.Unlock()
	}()
	go answerPings(c, session, pongs)
	go closeWhenIdle(c, session, idleTimeout)
	for {
		stream, err := session.AcceptStream()
		if err != nil {
			return
		}
		rs := &resettable{Stream: stream, conn: c}
		if !streams.add(rs) {
			stream.Close()
			continue
		}
		go func() {
			defer streams.remove(rs.ID())
			out.handle(rs)
		}()
	}
}

// streams tracks a carrier's open streams by id, so the client's RESET can abort one.
// A RESET can overtake its stream's SYN, which waits in smux's queue; the client's
// stream ids only increase, so one for an id past the newest accepted waits for it.
type streams struct {
	mu     sync.Mutex
	open   map[uint32]*resettable
	early  map[uint32]struct{}
	newest uint32
}

func newStreams() *streams {
	return &streams{open: make(map[uint32]*resettable), early: make(map[uint32]struct{})}
}

// add records an accepted stream, reporting false if the client already reset it.
func (s *streams) add(stream *resettable) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := stream.ID()
	s.newest = max(s.newest, id)
	_, reset := s.early[id]
	for early := range s.early {
		if early <= id {
			delete(s.early, early)
		}
	}
	if !reset {
		s.open[id] = stream
	}
	return !reset
}

func (s *streams) remove(id uint32) {
	s.mu.Lock()
	delete(s.open, id)
	s.mu.Unlock()
}

// reset runs on the carrier's reading goroutine. It marks the stream before smux
// reads the FIN that follows the RESET, and leaves the close, which writes a FIN of
// its own, to another goroutine.
func (s *streams) reset(id uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if stream, ok := s.open[id]; ok {
		stream.reset.Store(true)
		go stream.Stream.Close()
	} else if id > s.newest {
		s.early[id] = struct{}{}
	}
}

// resettable is a stream whose Abort resets the client's end too, and whose reads
// fail with proxy.ErrReset once the client resets it.
type resettable struct {
	*smux.Stream
	conn  *proxy.Conn
	reset atomic.Bool
}

// Abort writes the RESET before the relay closes the stream, so it reaches the client
// ahead of the stream's FIN; a carrier whose writes are stuck holds the relay until it
// closes.
func (s *resettable) Abort() {
	s.conn.WriteControl(proxy.CtrlReset, proxy.ResetBody(s.ID(), proxy.ResetAborted))
}

func (s *resettable) Read(b []byte) (int, error) {
	n, err := s.Stream.Read(b)
	if err != nil && s.reset.Load() {
		err = proxy.ErrReset
	}
	return n, err
}

// WriteTo keeps smux's copy loop, which io.Copy uses instead of Read and which ends
// cleanly at a FIN, but reports a reset the way Read does.
func (s *resettable) WriteTo(w io.Writer) (int64, error) {
	n, err := s.Stream.WriteTo(w)
	if s.reset.Load() {
		err = proxy.ErrReset
	}
	return n, err
}

func answerPings(c *proxy.Conn, session *smux.Session, pongs <-chan []byte) {
	for {
		select {
		case id := <-pongs:
			if c.WriteControl(proxy.CtrlPong, id) != nil {
				return
			}
		case <-session.CloseChan():
			return
		}
	}
}

// closeWhenIdle closes a carrier that has sent nothing for timeout, which smux's own
// keepalive timeout did before.
func closeWhenIdle(c *proxy.Conn, session *smux.Session, timeout time.Duration) {
	tick := time.NewTicker(timeout / 4)
	defer tick.Stop()
	for {
		select {
		case <-session.CloseChan():
			return
		case <-tick.C:
			if time.Since(c.LastRead()) >= timeout {
				log.Printf("carrier %s: nothing received for %s, closing", c.RemoteAddr(), timeout)
				session.Close()
				return
			}
		}
	}
}
