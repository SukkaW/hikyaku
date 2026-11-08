package proxy

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	PSKEnv  = "HIKYAKU_PSK"
	PSKSize = 32

	saltSize     = 32
	maxChunk     = 16 << 10
	helloVersion = 1
	maxClockSkew = 60 * time.Second
	replayWindow = 2 * maxClockSkew
)

var ErrReplay = errors.New("replayed or stale handshake")

func PSKFromEnv() ([]byte, error) {
	psk, err := base64.StdEncoding.DecodeString(os.Getenv(PSKEnv))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", PSKEnv, err)
	}
	if len(psk) != PSKSize {
		return nil, fmt.Errorf("%s must be %d bytes encoded in base64, got %d bytes", PSKEnv, PSKSize, len(psk))
	}
	return psk, nil
}

// ReplayFilter remembers client salts for as long as their hello timestamp is acceptable.
type ReplayFilter struct {
	mu   sync.Mutex
	seen map[[saltSize]byte]time.Time
}

func NewReplayFilter() *ReplayFilter {
	return &ReplayFilter{seen: make(map[[saltSize]byte]time.Time)}
}

func (f *ReplayFilter) firstSeen(salt []byte, now time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, t := range f.seen {
		if now.Sub(t) > replayWindow {
			delete(f.seen, k)
		}
	}
	key := [saltSize]byte(salt)
	if _, ok := f.seen[key]; ok {
		return false
	}
	f.seen[key] = now
	return true
}

// Conn encrypts a carrier with AES-256-GCM in length-prefixed chunks. Each Write
// becomes as few chunks as possible, so a smux frame never straddles two chunks.
// Reads and Writes are each expected from one goroutine at a time, as smux does.
type Conn struct {
	net.Conn
	r *bufio.Reader

	open      cipher.AEAD
	openNonce [12]byte
	rbuf      []byte
	pending   []byte

	seal      cipher.AEAD
	sealNonce [12]byte
	wbuf      []byte

	lastRead atomic.Int64 // unix nanoseconds of the last authenticated chunk
	readErr  atomic.Pointer[error]
}

// ClientHandshake sends the client salt and hello, then waits for the server's
// acknowledgement so that a returned Conn is known to share the PSK.
func ClientHandshake(conn net.Conn, psk []byte) (*Conn, error) {
	c := newConn(conn)
	clientSalt := make([]byte, saltSize)
	rand.Read(clientSalt)
	var err error
	if c.seal, err = newAEAD(psk, clientSalt, "hikyaku c2s"); err != nil {
		return nil, err
	}
	hello := make([]byte, 9)
	hello[0] = helloVersion
	binary.BigEndian.PutUint64(hello[1:], uint64(time.Now().Unix()))
	if err := c.writeWithPrefix(clientSalt, hello); err != nil {
		return nil, err
	}

	serverSalt := make([]byte, saltSize)
	if _, err := io.ReadFull(c.r, serverSalt); err != nil {
		return nil, err
	}
	// Binding the client salt keeps a recorded server response from being replayed to a new carrier.
	if c.open, err = newAEAD(psk, append(serverSalt, clientSalt...), "hikyaku s2c"); err != nil {
		return nil, err
	}
	ack, err := c.readChunk()
	if err != nil {
		return nil, fmt.Errorf("read server acknowledgement: %w", err)
	}
	if len(ack) != 1 || ack[0] != helloVersion {
		return nil, errors.New("unexpected server acknowledgement")
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
	if len(hello) != 9 || hello[0] != helloVersion {
		return nil, errors.New("unexpected client hello")
	}
	now := time.Now()
	sent := time.Unix(int64(binary.BigEndian.Uint64(hello[1:])), 0)
	if now.Sub(sent).Abs() > maxClockSkew || !replay.firstSeen(clientSalt, now) {
		return nil, ErrReplay
	}

	serverSalt := make([]byte, saltSize)
	rand.Read(serverSalt)
	if c.seal, err = newAEAD(psk, append(serverSalt, clientSalt...), "hikyaku s2c"); err != nil {
		return nil, err
	}
	if err := c.writeWithPrefix(serverSalt, []byte{helloVersion}); err != nil {
		return nil, err
	}
	return c, nil
}

func newConn(conn net.Conn) *Conn {
	return &Conn{Conn: conn, r: bufio.NewReaderSize(conn, 64<<10)}
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

// ReadErr returns the first error the carrier hit while reading, if any.
func (c *Conn) ReadErr() error {
	if err := c.readErr.Load(); err != nil {
		return *err
	}
	return nil
}

func (c *Conn) Read(b []byte) (int, error) {
	if len(c.pending) == 0 {
		p, err := c.readChunk()
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

func (c *Conn) Write(b []byte) (int, error) {
	if err := c.writeWithPrefix(nil, b); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (c *Conn) writeWithPrefix(prefix, b []byte) error {
	c.wbuf = append(c.wbuf[:0], prefix...)
	for len(b) > 0 {
		n := min(len(b), maxChunk)
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(n))
		c.wbuf = c.seal.Seal(c.wbuf, c.sealNonce[:], length[:], nil)
		increment(&c.sealNonce)
		c.wbuf = c.seal.Seal(c.wbuf, c.sealNonce[:], b[:n], nil)
		increment(&c.sealNonce)
		b = b[n:]
	}
	_, err := c.Conn.Write(c.wbuf)
	return err
}

func (c *Conn) readChunk() ([]byte, error) {
	overhead := c.open.Overhead()
	if cap(c.rbuf) < maxChunk+overhead {
		c.rbuf = make([]byte, maxChunk+overhead)
	}
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
	c.lastRead.Store(time.Now().UnixNano())
	return plain, nil
}

func increment(nonce *[12]byte) {
	for i := range nonce {
		nonce[i]++
		if nonce[i] != 0 {
			return
		}
	}
}
