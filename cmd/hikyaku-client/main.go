package main

import (
	"flag"
	"log"
	"net"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:1080", "SOCKS5 listen address")
	server := flag.String("server", "", "server address (host:port)")
	psk := flag.String("psk", "", "pre-shared key, any string")
	carriers := flag.Int("carriers", 4, "maximum number of carriers")
	flag.Parse()

	if *server == "" {
		log.Fatal("-server is required")
	}
	if *psk == "" {
		log.Fatal("-psk is required")
	}
	if *carriers < 1 {
		log.Fatal("-carriers must be at least 1")
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("SOCKS5 listening on %s", ln.Addr())
	h := &inbound{pool: newPool(*server, []byte(*psk), *carriers)}
	log.Fatal(h.serve(ln))
}
