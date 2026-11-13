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
	saltSize = 32
	// maxChunk is the longest chunk a sender writes.
	maxChunk = maxRecvChunk
	// maxRecvChunk is the longest chunk a receiver accepts, the most a 15-bit length
	// holds.
	maxRecvChunk = 1<<15 - 1
	// readBuffer is the carrier's read buffer; a server allocates it only once the
	// client's hello authenticates.
	readBuffer = 64 << 10
	// helloVersion is the wire protocol version; every format before the first
	// release is 0, and numbering starts there.
	helloVersion = 0

	replayGeneration     = 12 * time.Hour
	replayGenerationSize = 100_000

	// controlFlag marks a chunk as a carrier control message rather than smux data;
	// chunks are at most maxRecvChunk long, so the length's top bit is free.
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
	wlen      [2]byte

	psk        []byte
	clientSalt []byte
	acked      atomic.Bool

	control  func(cmd byte, body []byte)
	lastRead atomic.Int64 // unix nanoseconds of the last authenticated chunk
	lastData atomic.Int64 // unix nanoseconds of the last chunk carrying smux data
	lastSent atomic.Int64 // unix nanoseconds of the last smux data written
	// receiving is set while a smux data chunk's payload is still arriving.
	receiving atomic.Bool
	readErr   atomic.Pointer[error]
}

// ClientHandshake sends the client salt and hello and returns at once, so streams
// can follow in the same flight. The server's acknowledgement is checked on the
// first Read; until then the PSK is unconfirmed.
func ClientHandshake(conn net.Conn, psk []byte) (*Conn, error) {
	c := newConn(conn)
	c.r = bufio.NewReaderSize(conn, readBuffer)
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
	// Until the hello authenticates, read exactly what it takes straight from conn: a
	// peer without the PSK then costs no buffers, and the client's early data behind
	// the hello stays unread for the reader made afterwards.
	clientSalt := make([]byte, saltSize)
	if _, err := io.ReadFull(conn, clientSalt); err != nil {
		return nil, err
	}
	var err error
	if c.open, err = newAEAD(psk, clientSalt, "hikyaku c2s"); err != nil {
		return nil, err
	}
	hello, err := c.readHello()
	if err != nil {
		return nil, fmt.Errorf("read client hello: %w", err)
	}
	if !replay.firstSeen(clientSalt, time.Now()) {
		return nil, ErrReplay
	}
	c.r = bufio.NewReaderSize(conn, readBuffer)
	// The hello names the newest version the client speaks; the server answers with
	// the newest one both speak, which the client then accepts or gives up on.
	version := min(hello, helloVersion)

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
	c := &Conn{Conn: conn}
	// Counting from creation makes a carrier that never hears back look silent.
	c.lastRead.Store(time.Now().UnixNano())
	c.lastData.Store(c.lastRead.Load())
	return c
}

// readHello reads the client's one-byte hello chunk with exact reads, before the
// server has any buffers; see ServerHandshake.
func (c *Conn) readHello() (byte, error) {
	overhead := c.open.Overhead()
	header := make([]byte, 2+overhead)
	if _, err := io.ReadFull(c.Conn, header); err != nil {
		return 0, err
	}
	length, err := c.open.Open(header[:0], c.openNonce[:], header, nil)
	if err != nil {
		return 0, err
	}
	increment(&c.openNonce)
	if binary.BigEndian.Uint16(length) != 1 {
		return 0, errors.New("unexpected client hello")
	}
	payload := make([]byte, 1+overhead)
	if _, err := io.ReadFull(c.Conn, payload); err != nil {
		return 0, err
	}
	hello, err := c.open.Open(payload[:0], c.openNonce[:], payload, nil)
	if err != nil {
		return 0, err
	}
	increment(&c.openNonce)
	return hello[0], nil
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

// ReceivingData reports whether a chunk of smux data is partly received, which
// LastData only shows once the whole chunk has arrived.
func (c *Conn) ReceivingData() bool {
	return c.receiving.Load()
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
			p, err = c.readChunk(true)
		}
		if err != nil {
			// smux ends streams with frames of its own, so the carrier itself ending,
			// even cleanly between chunks, cuts off whatever its streams were sending.
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			// Storing &err would move err to the heap on every read that fetches a chunk.
			stored := err
			c.readErr.CompareAndSwap(nil, &stored)
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
	ack, err := c.readChunk(false)
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
		binary.BigEndian.PutUint16(c.wlen[:], uint16(n)|flag)
		c.wbuf = c.seal.Seal(c.wbuf, c.sealNonce[:], c.wlen[:], nil)
		increment(&c.sealNonce)
		c.wbuf = c.seal.Seal(c.wbuf, c.sealNonce[:], b[:n], nil)
		increment(&c.sealNonce)
		b = b[n:]
	}
	_, err := c.Conn.Write(c.wbuf)
	return err
}

// readChunk returns the next chunk of smux data, handing control messages that
// arrive before it to the control handler, or failing on one unless controlAllowed.
func (c *Conn) readChunk(controlAllowed bool) ([]byte, error) {
	overhead := c.open.Overhead()
	if cap(c.rbuf) < maxRecvChunk+overhead {
		c.rbuf = make([]byte, maxRecvChunk+overhead)
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
		if n == 0 || n > maxRecvChunk {
			return nil, fmt.Errorf("invalid chunk length %d", n)
		}
		payload := c.rbuf[:n+overhead]
		c.receiving.Store(!control)
		_, err = io.ReadFull(c.r, payload)
		c.receiving.Store(false)
		if err != nil {
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
		if !controlAllowed {
			return nil, errors.New("unexpected control message")
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
