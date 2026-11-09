package proxy

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	PSKSize = 32

	saltSize = 32
	maxChunk = 16 << 10
	// helloVersion is the wire protocol version; every format before the first
	// release is 0, and numbering starts there.
	helloVersion = 0

	replayGeneration     = 12 * time.Hour
	replayGenerationSize = 100_000

	// controlFlag marks a chunk as a carrier control message rather than smux data;
	// chunks are at most maxChunk long, so the length's top bit is free.
	controlFlag = 0x8000
)

var ErrReplay = errors.New("replayed handshake")

// ReplayFilter remembers client salts in two generations: a new one starts once the
// current one is replayGeneration old or holds replayGenerationSize salts, and the
// oldest is dropped, so a salt is remembered for one to two generations. Salts are
// random, so their first 8 bytes are key enough.
type ReplayFilter struct {
	mu       sync.Mutex
	current  map[uint64]struct{}
	previous map[uint64]struct{}
	started  time.Time
}

func NewReplayFilter() *ReplayFilter {
	return &ReplayFilter{current: make(map[uint64]struct{}), previous: make(map[uint64]struct{}), started: time.Now()}
}

func (f *ReplayFilter) firstSeen(salt []byte, now time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := binary.BigEndian.Uint64(salt)
	if _, ok := f.current[key]; ok {
		return false
	}
	if _, ok := f.previous[key]; ok {
		return false
	}
	if now.Sub(f.started) >= replayGeneration || len(f.current) >= replayGenerationSize {
		f.previous, f.current, f.started = f.current, make(map[uint64]struct{}), now
	}
	f.current[key] = struct{}{}
	return true
}

// Conn encrypts a carrier with AES-256-GCM in length-prefixed chunks. Each Write
// becomes as few chunks as possible, so a smux frame never straddles two chunks.
// Reads are expected from one goroutine at a time, as smux does; writes are
// serialized, so control messages can go out while smux writes.
type Conn struct {
	net.Conn
	r *bufio.Reader

	open      cipher.AEAD
	openNonce [12]byte
	rbuf      []byte
	pending   []byte

	wmu       sync.Mutex
	seal      cipher.AEAD
	sealNonce [12]byte
	wbuf      []byte

	psk        []byte
	clientSalt []byte
	acked      atomic.Bool

	control  func(cmd byte, body []byte)
	lastRead atomic.Int64 // unix nanoseconds of the last authenticated chunk
	lastData atomic.Int64 // unix nanoseconds of the last chunk carrying smux data
	lastSent atomic.Int64 // unix nanoseconds of the last smux data written
	readErr  atomic.Pointer[error]
}

// ClientHandshake sends the client salt and hello and returns at once, so streams
// can follow in the same flight. The server's acknowledgement is checked on the
// first Read; until then the PSK is unconfirmed.
func ClientHandshake(conn net.Conn, psk []byte) (*Conn, error) {
	c := newConn(conn)
	c.psk = psk
	c.clientSalt = make([]byte, saltSize)
	rand.Read(c.clientSalt)
	var err error
	if c.seal, err = newAEAD(psk, c.clientSalt, "hikyaku c2s"); err != nil {
		return nil, err
	}
	if err := c.writeWithPrefix(c.clientSalt, []byte{helloVersion}, 0); err != nil {
		return nil, err
	}
	return c, nil
}

func ServerHandshake(conn net.Conn, psk []byte, replay *ReplayFilter) (*Conn, error) {
	c := newConn(conn)
	clientSalt := make([]byte, saltSize)
	if _, err := io.ReadFull(c.r, clientSalt); err != nil {
		return nil, err
	}
	var err error
	if c.open, err = newAEAD(psk, clientSalt, "hikyaku c2s"); err != nil {
		return nil, err
	}
	hello, err := c.readChunk()
	if err != nil {
		return nil, fmt.Errorf("read client hello: %w", err)
	}
	if len(hello) != 1 {
		return nil, errors.New("unexpected client hello")
	}
	if !replay.firstSeen(clientSalt, time.Now()) {
		return nil, ErrReplay
	}
	// The hello names the newest version the client speaks; the server answers with
	// the newest one both speak, which the client then accepts or gives up on.
	version := min(hello[0], helloVersion)

	serverSalt := make([]byte, saltSize)
	rand.Read(serverSalt)
	if c.seal, err = newAEAD(psk, append(serverSalt, clientSalt...), "hikyaku s2c"); err != nil {
		return nil, err
	}
	if err := c.writeWithPrefix(serverSalt, []byte{version}, 0); err != nil {
		return nil, err
	}
	return c, nil
}

func newConn(conn net.Conn) *Conn {
	c := &Conn{Conn: conn, r: bufio.NewReaderSize(conn, 64<<10)}
	// Counting from creation makes a carrier that never hears back look silent.
	c.lastRead.Store(time.Now().UnixNano())
	c.lastData.Store(c.lastRead.Load())
	return c
}

func newAEAD(psk, salt []byte, info string) (cipher.AEAD, error) {
	key, err := hkdf.Key(sha256.New, psk, salt, info, 32)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// LastRead reports when the peer's most recent chunk arrived.
func (c *Conn) LastRead() time.Time {
	return time.Unix(0, c.lastRead.Load())
}

// LastData reports when the peer's most recent smux data arrived; control messages
// do not count.
func (c *Conn) LastData() time.Time {
	return time.Unix(0, c.lastData.Load())
}

// LastSent reports when smux data was last written; control messages do not count.
func (c *Conn) LastSent() time.Time {
	return time.Unix(0, c.lastSent.Load())
}

// Acked reports whether the server has acknowledged a client handshake.
func (c *Conn) Acked() bool {
	return c.acked.Load()
}

// OnControl sets the handler for the peer's control messages; set it before smux
// starts reading. It runs on the reading goroutine, so it must not block, and body
// is only valid during the call.
func (c *Conn) OnControl(handle func(cmd byte, body []byte)) {
	c.control = handle
}

// WriteControl sends a control message, which smux on the other side never sees.
func (c *Conn) WriteControl(cmd byte, body []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.writeWithPrefix(nil, append([]byte{cmd}, body...), controlFlag)
}

// ReadErr returns the first error the carrier hit while reading, if any.
func (c *Conn) ReadErr() error {
	if err := c.readErr.Load(); err != nil {
		return *err
	}
	return nil
}

func (c *Conn) Read(b []byte) (int, error) {
	// Errors are sticky, so a failed acknowledgement can never be skipped by reading again.
	if err := c.ReadErr(); err != nil {
		return 0, err
	}
	if len(c.pending) == 0 {
		err := c.awaitAck()
		var p []byte
		if err == nil {
			p, err = c.readChunk()
		}
		if err != nil {
			c.readErr.CompareAndSwap(nil, &err)
			return 0, err
		}
		c.pending = p
	}
	n := copy(b, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

// awaitAck finishes a client handshake on the first Read: the server salt and
// acknowledgement arrive after whatever the client already sent behind its hello.
func (c *Conn) awaitAck() error {
	if c.open != nil {
		return nil
	}
	serverSalt := make([]byte, saltSize)
	if _, err := io.ReadFull(c.r, serverSalt); err != nil {
		return err
	}
	// Binding the client salt keeps a recorded server response from being replayed to a new carrier.
	open, err := newAEAD(c.psk, append(serverSalt, c.clientSalt...), "hikyaku s2c")
	if err != nil {
		return err
	}
	c.open = open
	ack, err := c.readChunk()
	if err != nil {
		return fmt.Errorf("read server acknowledgement: %w", err)
	}
	if len(ack) != 1 {
		return errors.New("unexpected server acknowledgement")
	}
	if ack[0] != helloVersion {
		return fmt.Errorf("server speaks protocol version %d, this client speaks %d", ack[0], helloVersion)
	}
	c.acked.Store(true)
	return nil
}

func (c *Conn) Write(b []byte) (int, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.lastSent.Store(time.Now().UnixNano())
	if err := c.writeWithPrefix(nil, b, 0); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (c *Conn) writeWithPrefix(prefix, b []byte, flag uint16) error {
	c.wbuf = append(c.wbuf[:0], prefix...)
	for len(b) > 0 {
		n := min(len(b), maxChunk)
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(n)|flag)
		c.wbuf = c.seal.Seal(c.wbuf, c.sealNonce[:], length[:], nil)
		increment(&c.sealNonce)
		c.wbuf = c.seal.Seal(c.wbuf, c.sealNonce[:], b[:n], nil)
		increment(&c.sealNonce)
		b = b[n:]
	}
	_, err := c.Conn.Write(c.wbuf)
	return err
}

// readChunk returns the next chunk of smux data, handing control messages that
// arrive before it to the control handler.
func (c *Conn) readChunk() ([]byte, error) {
	overhead := c.open.Overhead()
	if cap(c.rbuf) < maxChunk+overhead {
		c.rbuf = make([]byte, maxChunk+overhead)
	}
	for {
		header := c.rbuf[:2+overhead]
		if _, err := io.ReadFull(c.r, header); err != nil {
			return nil, err
		}
		length, err := c.open.Open(header[:0], c.openNonce[:], header, nil)
		if err != nil {
			return nil, err
		}
		increment(&c.openNonce)
		n := int(binary.BigEndian.Uint16(length))
		control := n&controlFlag != 0
		n &^= controlFlag
		if n == 0 || n > maxChunk {
			return nil, fmt.Errorf("invalid chunk length %d", n)
		}
		payload := c.rbuf[:n+overhead]
		if _, err := io.ReadFull(c.r, payload); err != nil {
			return nil, err
		}
		plain, err := c.open.Open(payload[:0], c.openNonce[:], payload, nil)
		if err != nil {
			return nil, err
		}
		increment(&c.openNonce)
		now := time.Now().UnixNano()
		c.lastRead.Store(now)
		if !control {
			c.lastData.Store(now)
			return plain, nil
		}
		if c.control != nil {
			c.control(plain[0], plain[1:])
		}
	}
}

func increment(nonce *[12]byte) {
	for i := range nonce {
		nonce[i]++
		if nonce[i] != 0 {
			return
		}
	}
}
