package proxy

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/things-go/go-socks5/statute"
)

// UDPDestination turns a stream into a UDP relay (udp-over-tcp v0). After the
// server's pong, each packet in either direction is a 2-byte big-endian length and a
// SOCKS5 UDP datagram (RFC 1928 section 7): from the client it names the packet's
// destination, from the server the source of the reply. Fragments are not supported.
const UDPDestination = "udp-over-tcp.v0.hikyaku.arpa"

// UDPRequest addresses UDPDestination.
var UDPRequest = statute.AddrSpec{FQDN: UDPDestination, AddrType: statute.ATYPDomain}

// MaxPacket is the largest datagram, SOCKS5 header included, a UDP stream carries.
const MaxPacket = 65535

var errPacketTooLarge = errors.New("UDP packet too large")

// WriteRequest starts a stream with a SOCKS5 CONNECT request (RFC 1928 section 4)
// naming its destination.
func WriteRequest(w io.Writer, dst statute.AddrSpec) error {
	_, err := w.Write(statute.Request{Version: statute.VersionSocks5, Command: statute.CommandConnect, DstAddr: dst}.Bytes())
	return err
}

// ReadRequest reads the destination a stream starts with.
func ReadRequest(r io.Reader) (statute.AddrSpec, error) {
	req, err := statute.ParseRequest(r)
	if err != nil {
		return statute.AddrSpec{}, err
	}
	if req.Command != statute.CommandConnect {
		return statute.AddrSpec{}, fmt.Errorf("unexpected SOCKS5 command %d", req.Command)
	}
	return req.DstAddr, nil
}

// WritePacket sends one length-prefixed datagram in a single write, so it stays
// whole as long as one goroutine writes the stream.
func WritePacket(w io.Writer, datagram []byte) error {
	if len(datagram) > MaxPacket {
		return errPacketTooLarge
	}
	b := make([]byte, 2+len(datagram))
	binary.BigEndian.PutUint16(b, uint16(len(datagram)))
	copy(b[2:], datagram)
	_, err := w.Write(b)
	return err
}

// ReadPacket reads one length-prefixed datagram into buf, which must hold MaxPacket
// bytes.
func ReadPacket(r io.Reader, buf []byte) ([]byte, error) {
	var size [2]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint16(size[:]))
	if _, err := io.ReadFull(r, buf[:n]); err != nil {
		return nil, err
	}
	return buf[:n], nil
}
