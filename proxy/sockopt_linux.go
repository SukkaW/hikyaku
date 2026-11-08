package proxy

import (
	"errors"
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// TuneCarrier limits unsent data in the kernel so smux keeps scheduling streams
// fairly, and asks for BBR; it reports which options the kernel refused.
func TuneCarrier(conn net.Conn) error {
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return nil
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return err
	}
	var lowatErr, bbrErr error
	if err := raw.Control(func(fd uintptr) {
		lowatErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_NOTSENT_LOWAT, 16384)
		bbrErr = unix.SetsockoptString(int(fd), unix.IPPROTO_TCP, unix.TCP_CONGESTION, "bbr")
	}); err != nil {
		return err
	}
	var errs []error
	if lowatErr != nil {
		errs = append(errs, fmt.Errorf("TCP_NOTSENT_LOWAT: %w", lowatErr))
	}
	if bbrErr != nil {
		errs = append(errs, fmt.Errorf("TCP_CONGESTION bbr: %w", bbrErr))
	}
	return errors.Join(errs...)
}
