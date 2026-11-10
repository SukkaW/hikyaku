package proxy

import (
	"errors"
	"log"
	"net"
	"time"
)

// KeepAccepting wraps ln so that Accept rides out errors such as running out of file
// descriptors, which pass, retrying with a growing pause as net/http does; it fails
// only once ln is closed.
func KeepAccepting(ln net.Listener) net.Listener {
	return &keepAccepting{Listener: ln}
}

type keepAccepting struct {
	net.Listener
}

func (l *keepAccepting) Accept() (net.Conn, error) {
	var delay time.Duration
	for {
		conn, err := l.Listener.Accept()
		if err == nil || errors.Is(err, net.ErrClosed) {
			return conn, err
		}
		delay = min(max(2*delay, 5*time.Millisecond), time.Second)
		log.Printf("accept: %v; retrying in %s", err, delay)
		time.Sleep(delay)
	}
}
