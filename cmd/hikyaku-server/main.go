package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
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
	allowPrivate := flag.Bool("allow-private", false, "allow loopback, private, link-local, multicast and unspecified destinations")
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
		goAway()
		os.Exit(0)
	}()
	log.Fatal(serve(ln, []byte(*psk), newOutbound(*allowPrivate)))
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
	c.OnControl(func(cmd byte, body []byte) {
		if cmd == proxy.CtrlPing {
			select {
			case pongs <- bytes.Clone(body):
			default: // never block the reader; the client pings again
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
		go out.handle(stream)
	}
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
