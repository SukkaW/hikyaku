package main

import (
	"flag"
	"log"
	"net"

	"github.com/SukkaW/hikyaku/proxy"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:1080", "SOCKS5 listen address")
	server := flag.String("server", "", "server address (host:port)")
	carriers := flag.Int("carriers", 4, "number of pre-opened carriers")
	flag.Parse()

	if *server == "" {
		log.Fatal("-server is required")
	}
	if *carriers < 1 {
		log.Fatal("-carriers must be at least 1")
	}
	psk, err := proxy.PSKFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("SOCKS5 listening on %s", ln.Addr())
	h := &inbound{pool: newPool(*server, psk, *carriers)}
	log.Fatal(h.serve(ln))
}
