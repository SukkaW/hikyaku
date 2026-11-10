package proxy

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"
)

// Each stream starts with a request from the client. The server answers replyAck as
// soon as it has read the request, then replyResult once it has reached the
// destination or opened its UDP socket. Everything after the request is the app's
// data, which the client sends without waiting for either reply.
const (
	cmdConnect  byte = 0x10
	cmdUDP      byte = 0x11
	replyAck    byte = 0x20
	replyResult byte = 0x21
)

// Carrier control messages travel outside smux (see Conn.WriteControl). The client
// pings, the server answers with a pong echoing the ping's 4-byte id, and the server
// sends GOAWAY, [code][length][reason], before it stops serving a carrier.
const (
	CtrlPing   byte = 0x01
	CtrlPong   byte = 0x02
	CtrlGoAway byte = 0x03
)

// GoAwayShutdown is the GOAWAY code for a server that is shutting down.
const GoAwayShutdown byte = 1

// Result codes a server reports after trying the destination.
const (
	ResultOK byte = iota
	ResultDNS
	ResultRefused
	ResultNetUnreachable
	ResultHostUnreachable
	ResultTimeout
	ResultBlocked
	ResultServerError
)

// Address types, numbered as in SOCKS5.
const (
	addrIPv4   byte = 1
	addrDomain byte = 3
	addrIPv6   byte = 4
)

// MaxPacket is the largest UDP packet, its address included, a UDP stream carries.
const MaxPacket = 65535

// ErrBadPacket is a UDP packet that cannot be encoded or decoded. The stream it was on
// stays usable, so the packet is skipped rather than ending the association.
var ErrBadPacket = errors.New("invalid UDP packet")

var (
	errPacketTooLarge = fmt.Errorf("%w: too large", ErrBadPacket)
	errBadAddr        = errors.New("invalid address")
	errBadAck         = errors.New("stream did not start with an acknowledgement")
	errBadResult      = errors.New("stream did not report a result")
)

// Addr is a destination or source: an IP address or a domain name, and a port.
type Addr struct {
	IP     netip.Addr // used when Domain is empty
	Domain string
	Port   uint16
}

func (a Addr) String() string {
	host := a.Domain
	if host == "" {
		host = a.IP.String()
	}
	return net.JoinHostPort(host, strconv.Itoa(int(a.Port)))
}

func appendAddr(b []byte, a Addr) ([]byte, error) {
	switch {
	case a.Domain != "":
		if len(a.Domain) > 255 {
			return nil, errBadAddr
		}
		b = append(b, addrDomain, byte(len(a.Domain)))
		b = append(b, a.Domain...)
	case a.IP.Is4():
		b = append(b, addrIPv4)
		b = append(b, a.IP.AsSlice()...)
	case a.IP.Is6():
		b = append(b, addrIPv6)
		b = append(b, a.IP.AsSlice()...)
	default:
		return nil, errBadAddr
	}
	return binary.BigEndian.AppendUint16(b, a.Port), nil
}

func readAddr(r io.Reader) (Addr, error) {
	var kind [1]byte
	if _, err := io.ReadFull(r, kind[:]); err != nil {
		return Addr{}, err
	}
	var a Addr
	switch kind[0] {
	case addrIPv4:
		var ip [4]byte
		if _, err := io.ReadFull(r, ip[:]); err != nil {
			return Addr{}, err
		}
		a.IP = netip.AddrFrom4(ip)
	case addrIPv6:
		var ip [16]byte
		if _, err := io.ReadFull(r, ip[:]); err != nil {
			return Addr{}, err
		}
		a.IP = netip.AddrFrom16(ip)
	case addrDomain:
		var size [1]byte
		if _, err := io.ReadFull(r, size[:]); err != nil {
			return Addr{}, err
		}
		if size[0] == 0 {
			return Addr{}, errBadAddr
		}
		name := make([]byte, size[0])
		if _, err := io.ReadFull(r, name); err != nil {
			return Addr{}, err
		}
		a.Domain = string(name)
	default:
		return Addr{}, errBadAddr
	}
	var port [2]byte
	if _, err := io.ReadFull(r, port[:]); err != nil {
		return Addr{}, err
	}
	a.Port = binary.BigEndian.Uint16(port[:])
	return a, nil
}

// AppendConnect appends a request for a TCP connection to dst.
func AppendConnect(b []byte, dst Addr) ([]byte, error) {
	return appendAddr(append(b, cmdConnect), dst)
}

// UDPRequest turns a stream into a UDP relay; every packet in either direction then
// carries its own address.
var UDPRequest = []byte{cmdUDP}

// ReadRequest reads the request a stream starts with; udp reports a UDP relay, which
// has no destination of its own.
func ReadRequest(r io.Reader) (dst Addr, udp bool, err error) {
	var cmd [1]byte
	if _, err := io.ReadFull(r, cmd[:]); err != nil {
		return Addr{}, false, err
	}
	switch cmd[0] {
	case cmdConnect:
		dst, err = readAddr(r)
		return dst, false, err
	case cmdUDP:
		return Addr{}, true, nil
	default:
		return Addr{}, false, fmt.Errorf("unknown command 0x%02x", cmd[0])
	}
}

// WriteAck tells the client its request arrived.
func WriteAck(w io.Writer) error {
	_, err := w.Write([]byte{replyAck})
	return err
}

// ReadAck reads the server's acknowledgement.
func ReadAck(r io.Reader) error {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return err
	}
	if b[0] != replyAck {
		return errBadAck
	}
	return nil
}

// WriteResult reports how reaching the destination went; message explains a failure
// and is cut to 255 bytes.
func WriteResult(w io.Writer, code byte, message string) error {
	if len(message) > 255 {
		message = message[:255]
	}
	_, err := w.Write(append([]byte{replyResult, code, byte(len(message))}, message...))
	return err
}

// ResultError is a failure the server reported reaching the destination.
type ResultError struct {
	Code    byte
	Message string
}

func (e *ResultError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("server reported result %d", e.Code)
	}
	return e.Message
}

// ReadResult reads the server's result, returning a *ResultError for a failure.
func ReadResult(r io.Reader) error {
	var h [3]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return err
	}
	if h[0] != replyResult {
		return errBadResult
	}
	message := make([]byte, h[2])
	if _, err := io.ReadFull(r, message); err != nil {
		return err
	}
	if h[1] != ResultOK {
		return &ResultError{Code: h[1], Message: string(message)}
	}
	return nil
}

// WritePacket sends one length-prefixed datagram and its address in a single write,
// so it stays whole as long as one goroutine writes the stream.
func WritePacket(w io.Writer, addr Addr, payload []byte) error {
	b, err := appendAddr(make([]byte, 2, 2+1+1+255+2+len(payload)), addr)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrBadPacket, err)
	}
	b = append(b, payload...)
	if len(b)-2 > MaxPacket {
		return errPacketTooLarge
	}
	binary.BigEndian.PutUint16(b, uint16(len(b)-2))
	_, err = w.Write(b)
	return err
}

// ReadPacket reads one length-prefixed datagram into buf, which must hold MaxPacket
// bytes, and returns its address and payload.
func ReadPacket(r io.Reader, buf []byte) (Addr, []byte, error) {
	var size [2]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return Addr{}, nil, err
	}
	n := int(binary.BigEndian.Uint16(size[:]))
	if n == 0 {
		return Addr{}, nil, fmt.Errorf("%w: empty", ErrBadPacket)
	}
	if _, err := io.ReadFull(r, buf[:n]); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return Addr{}, nil, err
	}
	packet := bytes.NewReader(buf[:n])
	addr, err := readAddr(packet)
	if err != nil {
		return Addr{}, nil, fmt.Errorf("%w: %v", ErrBadPacket, err)
	}
	return addr, buf[n-packet.Len() : n], nil
}
