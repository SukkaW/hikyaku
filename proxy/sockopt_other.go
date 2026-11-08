//go:build !linux

package proxy

import "net"

func TuneCarrier(net.Conn) error { return nil }
