package proxy

import (
	"errors"
	"net"
	"syscall"
	"testing"
)

// scriptedListener returns the given results from Accept in turn.
type scriptedListener struct {
	net.Listener
	errs []error
}

func (l *scriptedListener) Accept() (net.Conn, error) {
	err := l.errs[0]
	l.errs = l.errs[1:]
	return nil, err
}

func TestKeepAccepting(t *testing.T) {
	ln := KeepAccepting(&scriptedListener{errs: []error{syscall.EMFILE, syscall.ECONNABORTED, net.ErrClosed}})
	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Accept returned %v, want it to retry until the listener closes", err)
	}
}
