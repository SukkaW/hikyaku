package main

import (
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
	"github.com/xtaci/smux"
)

const handshakeTimeout = 10 * time.Second

func main() {
	listen := flag.String("listen", ":8444", "carrier listen address")
	allowPrivate := flag.Bool("allow-private", false, "allow loopback, private, link-local, multicast and unspecified destinations")
	genkey := flag.Bool("genkey", false, "print a new random PSK and exit")
	flag.Parse()

	if *genkey {
		psk := make([]byte, proxy.PSKSize)
		rand.Read(psk)
		fmt.Println(base64.StdEncoding.EncodeToString(psk))
		return
	}

	psk, err := proxy.PSKFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("listening on %s", ln.Addr())
	log.Fatal(serve(ln, psk, newOutbound(*allowPrivate)))
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
	session, err := smux.Server(c, proxy.MuxConfig())
	if err != nil {
		log.Printf("carrier %s: %v", conn.RemoteAddr(), err)
		return
	}
	defer session.Close()
	for {
		stream, err := session.AcceptStream()
		if err != nil {
			return
		}
		go out.handle(stream)
	}
}
