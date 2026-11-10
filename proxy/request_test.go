package proxy

import (
	"bytes"
	"errors"
	"io"
	"net/netip"
	"strings"
	"testing"
)

var testAddrs = []Addr{
	{IP: netip.MustParseAddr("192.0.2.1"), Port: 443},
	{IP: netip.MustParseAddr("2001:db8::1"), Port: 53},
	{Domain: "example.com", Port: 8080},
}

func TestRequestRoundTrip(t *testing.T) {
	for _, want := range testAddrs {
		b, err := AppendConnect(nil, want)
		if err != nil {
			t.Fatal(err)
		}
		got, udp, err := ReadRequest(bytes.NewReader(b))
		if err != nil || udp || got != want {
			t.Fatalf("connect to %s read back as %s, udp %v, %v", want, got, udp, err)
		}
	}
	if _, udp, err := ReadRequest(bytes.NewReader(UDPRequest)); err != nil || !udp {
		t.Fatalf("UDP request read back as udp %v, %v", udp, err)
	}
}

func TestReplyRoundTrip(t *testing.T) {
	var b bytes.Buffer
	WriteAck(&b)
	WriteResult(&b, ResultOK, "")
	WriteResult(&b, ResultRefused, strings.Repeat("x", 300))
	if err := ReadAck(&b); err != nil {
		t.Fatal(err)
	}
	if err := ReadResult(&b); err != nil {
		t.Fatalf("OK result read back as %v", err)
	}
	var failed *ResultError
	if err := ReadResult(&b); !errors.As(err, &failed) || failed.Code != ResultRefused || len(failed.Message) != 255 {
		t.Fatalf("failed result read back as %v", err)
	}
}

func TestPacketRoundTrip(t *testing.T) {
	buf := make([]byte, MaxPacket)
	for _, want := range testAddrs {
		var b bytes.Buffer
		if err := WritePacket(&b, want, []byte("payload")); err != nil {
			t.Fatal(err)
		}
		got, payload, err := ReadPacket(&b, buf)
		if err != nil || got != want || string(payload) != "payload" {
			t.Fatalf("packet for %s read back as %s %q, %v", want, got, payload, err)
		}
	}
}

func TestMalformedInput(t *testing.T) {
	cases := map[string]func() error{
		"unknown command": func() error {
			_, _, err := ReadRequest(bytes.NewReader([]byte{0x7f}))
			return err
		},
		"unknown address type": func() error {
			_, _, err := ReadRequest(bytes.NewReader([]byte{cmdConnect, 2, 0, 0}))
			return err
		},
		"empty domain": func() error {
			_, _, err := ReadRequest(bytes.NewReader([]byte{cmdConnect, addrDomain, 0, 0, 80}))
			return err
		},
		"truncated address": func() error {
			_, _, err := ReadRequest(bytes.NewReader([]byte{cmdConnect, addrIPv4, 192, 0}))
			return err
		},
		"not an acknowledgement": func() error {
			return ReadAck(bytes.NewReader([]byte{replyResult}))
		},
		"not a result": func() error {
			return ReadResult(bytes.NewReader([]byte{replyAck, 0, 0}))
		},
		"packet with a bad address": func() error {
			_, _, err := ReadPacket(bytes.NewReader([]byte{0, 1, 9}), make([]byte, MaxPacket))
			return err
		},
		"address with no IP or domain": func() error {
			_, err := AppendConnect(nil, Addr{Port: 80})
			return err
		},
	}
	for name, read := range cases {
		if err := read(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestBadPackets(t *testing.T) {
	buf := make([]byte, MaxPacket)
	cases := map[string]func() error{
		"empty frame": func() error {
			_, _, err := ReadPacket(bytes.NewReader([]byte{0, 0}), buf)
			return err
		},
		"unknown address type": func() error {
			_, _, err := ReadPacket(bytes.NewReader([]byte{0, 1, 9}), buf)
			return err
		},
		"truncated address": func() error {
			_, _, err := ReadPacket(bytes.NewReader([]byte{0, 2, addrIPv4, 1}), buf)
			return err
		},
		"address with no IP or domain": func() error {
			return WritePacket(io.Discard, Addr{Port: 53}, []byte("query"))
		},
		"too large with its address": func() error {
			return WritePacket(io.Discard, Addr{IP: netip.IPv6Loopback(), Port: 53}, make([]byte, MaxPacket))
		},
	}
	for name, run := range cases {
		if err := run(); !errors.Is(err, ErrBadPacket) {
			t.Errorf("%s: %v, want ErrBadPacket", name, err)
		}
	}

	// A bad frame is skipped whole, so the next one still decodes.
	var b bytes.Buffer
	b.Write([]byte{0, 1, 9})
	want := Addr{IP: netip.MustParseAddr("192.0.2.1"), Port: 53}
	if err := WritePacket(&b, want, []byte("next")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadPacket(&b, buf); !errors.Is(err, ErrBadPacket) {
		t.Fatalf("bad frame: %v, want ErrBadPacket", err)
	}
	if got, payload, err := ReadPacket(&b, buf); err != nil || got != want || string(payload) != "next" {
		t.Fatalf("frame after a bad one read as %s %q, %v", got, payload, err)
	}
}
