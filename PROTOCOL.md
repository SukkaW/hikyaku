# Hikyaku Protocol

## Version

`0`

Every format before the first release is version `0`; version numbers increase from there.

## Abstract

This document specifies the Hikyaku protocol, which carries many TCP connections and UDP associations between a client and a server over a small number of long-lived TCP connections. All traffic is encrypted and authenticated with a key derived from a pre-shared key. A client may send its first requests together with its handshake, so a request costs no extra round trip on a new connection. An implementation that follows this document produces the same bytes as any other, given the same keys and random values, and interoperates with any other conforming client or server.

## 1. Overview

A client opens one or more TCP connections to a server, called carriers. Each carrier starts with a handshake that proves both sides hold the same pre-shared key, then carries encrypted chunks in both directions. A chunk is either a data chunk or a control chunk.

Data chunks carry a byte stream of multiplexing frames (Section 4), which split the carrier into independent bidirectional streams. Each stream carries one TCP connection or one UDP association: the client opens it, sends a request naming the destination (Section 5), and the server answers, then relays.

Control chunks carry messages about the carrier itself, outside the multiplexing layer: pings and pongs, a server's notice that it is going away, and stream resets.

```
+-------------------------------------------------------------+
| TCP connection (carrier)                                    |
|  salt, handshake chunk, then encrypted chunks               |
|   +------------------------------+  +--------------------+  |
|   | data chunks: smux v2 frames  |  | control chunks:    |  |
|   |  stream 3: request, replies, |  |  PING, PONG,       |  |
|   |            relayed bytes     |  |  GOAWAY, RESET     |  |
|   |  stream 5: ...               |  |                    |  |
|   +------------------------------+  +--------------------+  |
+-------------------------------------------------------------+
```

The protocol does not try to hide that it is in use.

### 1.1. Conventions and Terms

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in BCP 14 [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) [RFC 8174](https://www.rfc-editor.org/rfc/rfc8174) when, and only when, they appear in all capitals, as shown here.

All integers are unsigned and big-endian unless noted otherwise; the multiplexing frame header (Section 4.1) is little-endian. `u8`, `u16be`, `u32be`, `u16le` and `u32le` name integers of 8, 16 or 32 bits in big-endian (`be`) or little-endian (`le`) order. `||` is concatenation. Sizes are in bytes.

- Client: the side that opens carriers and streams.
- Server: the side that accepts carriers and relays streams to their destinations.
- Carrier: one TCP connection between a client and a server.
- Chunk: one encrypted unit on a carrier (Section 3.1).
- Stream: one bidirectional channel within a carrier (Section 4).
- App connection: the TCP connection or UDP association at the client that a stream carries.
- Destination: the host and port a stream's request names.

## 2. Keys and Encryption

### 2.1. Pre-Shared Key

The pre-shared key (PSK) is a byte string of any length, configured identically on the client and the server. An implementation that takes the PSK as text MUST use the text's UTF-8 bytes unchanged.

### 2.2. Key Derivation

Each carrier uses two keys, one per direction, derived with HKDF-SHA256 ([RFC 5869](https://www.rfc-editor.org/rfc/rfc5869)) from the PSK and the salts exchanged in the handshake (Section 3.2):

```
client_key := HKDF-SHA256(IKM: psk, salt: client_salt, info: "hikyaku c2s", L: 32)
server_key := HKDF-SHA256(IKM: psk, salt: server_salt || client_salt, info: "hikyaku s2c", L: 32)
```

`info` is the ASCII bytes of the string, without a terminator. `client_key` encrypts everything the client sends after its salt; `server_key` encrypts everything the server sends after its salt.

### 2.3. Encryption

Chunks are encrypted with AES-256-GCM, with a 16-byte authentication tag and no associated data.

Each direction of a carrier keeps its own nonce: a 12-byte little-endian integer that starts at zero and is incremented by one after each encryption (by the sender) or decryption (by the receiver). Every chunk uses two consecutive nonces, the first for its length and the second for its payload (Section 3.1). A nonce MUST NOT be reused with the same key.

## 3. Carrier

### 3.1. Chunks

```
+------------------+---------------------------+
| encrypted length | encrypted payload         |
+------------------+---------------------------+
| 2 + 16 (tag)     | L + 16 (tag)              |
+------------------+---------------------------+

length (u16be, before encryption):
+---+----------------------+
| C | L                    |
+---+----------------------+
| 1 bit | 15 bits          |
+---+----------------------+
```

- `C` (the most significant bit): `0` for a data chunk, `1` for a control chunk.
- `L`: the payload's length before encryption, from 1 to 32767.

The payload of a data chunk is the next `L` bytes of the carrier's multiplexing byte stream (Section 4). Chunk boundaries carry no meaning: a receiver MUST treat the payloads of a direction's data chunks, in order, as one continuous byte stream.

The payload of a control chunk is one control message (Section 3.4) and is not part of the byte stream.

A receiver MUST close the carrier when a chunk fails authentication, or when `L` is 0.

### 3.2. Handshake

The client sends its first flight:

```
+-------------+--------------------------------------------+
| client_salt | hello chunk                                |
+-------------+--------------------------------------------+
| 32          | data chunk, L = 1: client version (u8)     |
+-------------+--------------------------------------------+
```

- `client_salt`: 32 bytes from a cryptographically secure random number generator, new for every carrier.
- The hello chunk is encrypted with `client_key` and nonces 0 and 1. Its payload is the highest protocol version the client speaks; this document defines version `0`.

The client MAY send further chunks right after the hello chunk, without waiting for the server (Section 3.2.1).

The server reads `client_salt` and the hello chunk, and MUST close the carrier without replying if the hello chunk fails authentication, is a control chunk, or has `L` other than 1, or if `client_salt` is one it remembers (Section 3.3). Otherwise it replies:

```
+-------------+----------------------------------------------+
| server_salt | acknowledgement chunk                        |
+-------------+----------------------------------------------+
| 32          | data chunk, L = 1: negotiated version (u8)   |
+-------------+----------------------------------------------+
```

- `server_salt`: 32 bytes from a cryptographically secure random number generator, new for every carrier.
- The acknowledgement chunk is encrypted with `server_key` and nonces 0 and 1. Its payload is the lower of the client's version and the highest version the server speaks.

The acknowledgement chunk MUST be the server's first chunk. The client MUST close the carrier if the acknowledgement chunk fails authentication, is a control chunk, has `L` other than 1, or names a version the client does not speak. The handshake is complete once the client has accepted the acknowledgement.

A server SHOULD close a carrier whose hello chunk has not arrived within a bounded time.

#### 3.2.1. Early Data

Chunks the client sends after the hello chunk, before the acknowledgement arrives, are encrypted with `client_key` and nonces 2, 3 and onwards, like any later chunk. The server processes them, in order, only after it has accepted the hello chunk. A client can therefore open a stream and send its request and the app's first bytes in its first flight.

Until the acknowledgement arrives, the client has no proof that the server holds the PSK; it SHOULD NOT treat the carrier as working before then.

### 3.3. Replay Protection

A server MUST remember the `client_salt` of every carrier whose hello chunk it accepted, for a period of its choosing, and MUST close without replying any carrier that presents a remembered `client_salt`. A server remembers only salts whose hello chunk authenticated.

Timestamps are not part of the handshake, so a client's clock never affects whether it is accepted.

### 3.4. Control Messages

```
+-----+----------+
| CMD | BODY     |
+-----+----------+
| u8  | variable |
+-----+----------+
```

| CMD | Name | Sent by | Body |
|---|---|---|---|
| `0x01` | PING | client | `id` (u32be) |
| `0x02` | PONG | server | `id` (u32be), copied from the PING |
| `0x03` | GOAWAY | server | `code` (u8) \|\| `length` (u8) \|\| `reason` (`length` bytes, UTF-8) |
| `0x04` | RESET | client or server | `stream_id` (u32be) \|\| `code` (u8) |

A receiver MUST ignore control messages with a command it does not know, a PONG whose body is not 4 bytes, and a RESET whose body is not 5 bytes.

#### 3.4.1. PING and PONG

A server MUST answer each PING with a PONG carrying the same `id`, in the order the PINGs arrived. A server MAY drop a PING it cannot answer promptly; it never answers a dropped PING later. A client therefore SHOULD treat a PONG as also settling every PING it sent before that one.

`id` is chosen by the client; a client SHOULD count up from 0 on each carrier.

#### 3.4.2. GOAWAY

A server sends GOAWAY before it stops serving a carrier. `code` `0x01` means the server is shutting down; `reason` is human-readable text. After GOAWAY, the server keeps serving the carrier's open streams for a grace period of its choosing and then closes the carrier. A client MUST NOT open new streams on a carrier after receiving GOAWAY on it. A client MUST treat an unknown `code` like `0x01`, and a GOAWAY whose body is shorter than `2 + length` as a GOAWAY without a reason.

#### 3.4.3. RESET

Either side sends RESET when its end of a stream ended in an error, such as an app connection or destination connection that was reset, as opposed to closed cleanly. `code` `0x01` means aborted; a receiver MUST accept any `code`.

The sender MUST write the RESET before the stream's FIN frame (Section 4.4), so it arrives first. A receiver of RESET MUST abort its end of the stream: it ends the app connection or destination connection with an error (for TCP, a reset) rather than a clean close, and stops reading and writing the stream.

A RESET can arrive before the SYN frame that opens its stream. A server that receives RESET for a stream ID greater than any it has seen opened MUST remember it, and abort that stream as soon as its SYN arrives. A receiver MUST ignore RESET for any other stream ID it does not know.

### 3.5. Carrier Lifetime

A server closes a carrier from which it has received no chunk for 60 seconds. A client that wants to keep a carrier open MUST therefore send at least one chunk on it every 60 seconds; a PING suffices.

Either side may close a carrier at any time. Closing a carrier does not end its streams cleanly: both sides MUST treat every stream still open on it as aborted (Section 3.4.3).

## 4. Multiplexing

The data-chunk byte stream of each direction is a sequence of frames of the smux protocol, version 2, as implemented by [github.com/xtaci/smux](https://github.com/xtaci/smux). This section defines everything an implementation needs.

### 4.1. Frame

```
+-----+-----+--------+-----------+----------+
| VER | CMD | LENGTH | STREAM_ID | DATA     |
+-----+-----+--------+-----------+----------+
| u8  | u8  | u16le  | u32le     | LENGTH   |
+-----+-----+--------+-----------+----------+
```

- `VER`: always `2`.
- `CMD`: the frame type.
- `LENGTH`: the size of `DATA`, from 0 to 65535.
- `STREAM_ID`: the stream the frame belongs to.

| CMD | Name | LENGTH | Meaning |
|---|---|---|---|
| `0x00` | SYN | 0 | open stream `STREAM_ID` |
| `0x01` | FIN | 0 | the sender will send no more data on the stream |
| `0x02` | PSH | any | `DATA` is the stream's next bytes |
| `0x03` | NOP | 0 | no operation |
| `0x04` | UPD | 8 | window update: `DATA` is `consumed` (u32le) \|\| `window` (u32le) |

A receiver MUST close the carrier when a frame has `VER` other than 2, an unknown `CMD`, or a `LENGTH` its `CMD` does not allow (non-zero for SYN, FIN or NOP, other than 8 for UPD).

A receiver MUST ignore a SYN for a stream ID that is already open, a PSH with `LENGTH` 0, and a PSH, FIN or UPD for a stream it does not know (reading and discarding the PSH's `DATA`).

A frame may span several data chunks, and a data chunk may hold several frames.

### 4.2. Streams

Only the client opens streams. Stream IDs are odd: the client's first stream is 3, and each new stream takes the next odd number (5, 7, ...). A client MUST NOT reuse a stream ID on a carrier, and MUST send each stream's SYN after the SYN of every lower-numbered stream. A server MUST NOT open streams.

A stream opens with its SYN frame; its data follows in PSH frames.

### 4.3. Flow Control

Each direction of each stream is flow-controlled. A sender keeps:

- `written`: the total bytes of PSH `DATA` it has sent on the stream, modulo 2^32.
- `peer_consumed` and `peer_window`: the values of the latest UPD it received for the stream, initially 0 and 262144.

The sender MUST NOT send PSH `DATA` that would make `written - peer_consumed` (modulo 2^32, as a signed 32-bit value) exceed `peer_window`.

A receiver keeps `consumed`, the total bytes of the stream's data its app has read, modulo 2^32, and a `window`, the most it will buffer for the stream. It sends UPD with its current `consumed` and `window`:

- after its app reads from the stream for the first time, and
- every time its app has read at least `window / 2` bytes since the last UPD it sent.

A receiver MUST send UPDs at least this often, or a sender following this section can stall.

### 4.4. Closing a Stream

FIN ends one direction of a stream: after sending FIN, a side sends no more PSH on that stream. The other direction continues until that side sends its own FIN, so a stream can be half-closed. A stream ends when both directions have sent FIN, or when either side has sent RESET (Section 3.4.3).

NOP frames are not needed; liveness uses PING and PONG (Section 3.4.1).

## 5. Streams

### 5.1. Request

The first bytes the client sends on a stream are its request.

```
CONNECT:
+------+----------+
| CMD  | ADDR     |
+------+----------+
| 0x10 | variable |
+------+----------+

UDP:
+------+
| CMD  |
+------+
| 0x11 |
+------+
```

- CONNECT: relay a TCP connection to the destination `ADDR`.
- UDP: relay UDP packets, each with its own address (Section 5.5).

```
ADDR:
+------+----------+--------+
| TYPE | ADDRESS  | PORT   |
+------+----------+--------+
| u8   | variable | u16be  |
+------+----------+--------+
```

| TYPE | ADDRESS |
|---|---|
| `0x01` | IPv4 address, 4 bytes |
| `0x03` | domain name: `length` (u8, 1 to 255) \|\| `length` bytes |
| `0x04` | IPv6 address, 16 bytes |

The server resolves domain names itself.

The client MAY send the app's data right after the request, in the same frame, without waiting for any reply.

A server MUST close the stream without replying if the request has an unknown command or a malformed `ADDR`. A server SHOULD close a stream whose request has not arrived within a bounded time.

### 5.2. Replies

The server writes two replies at the start of the stream's server-to-client data:

```
ACK:
+------+
| 0x20 |
+------+

RESULT:
+------+------+--------+----------+
| 0x21 | CODE | LENGTH | MESSAGE  |
+------+------+--------+----------+
| u8   | u8   | u8     | LENGTH   |
+------+------+--------+----------+
```

The server sends ACK as soon as it has read the request, before contacting the destination. It sends RESULT once it has connected to the destination (CONNECT) or opened its UDP socket (UDP). `MESSAGE` is human-readable UTF-8 text describing a failure, at most 255 bytes, and empty on success.

| CODE | Meaning |
|---|---|
| 0 | success |
| 1 | the destination's name could not be resolved |
| 2 | the destination refused the connection |
| 3 | the destination's network is unreachable |
| 4 | the destination host is unreachable |
| 5 | connecting timed out |
| 6 | any other failure |

After a RESULT other than 0, the server sends FIN and relays nothing; the client MUST treat the stream as failed. A client MUST treat a stream whose first byte is not ACK, or whose next reply is not RESULT, as failed.

ACK lets a client tell that the carrier and the server are working before the destination answers.

### 5.3. TCP Relay

After RESULT 0, the rest of the stream in each direction is the TCP connection's data, relayed unchanged.

- When one end of the relayed connection closes cleanly, the side that sees it sends FIN, and the other side closes the write half of its connection, so half-closed connections keep working.
- When one end ends in an error, the side that sees it sends RESET (Section 3.4.3).

### 5.4. Ordering of the First Flight

The bytes a client may send on a new carrier before it hears anything are, in order: `client_salt`, the hello chunk, then data chunks holding the SYN of each stream it opens and PSH frames with each stream's request and early data. Section 7 gives this flight byte for byte.

### 5.5. UDP Relay

After a UDP request and its replies (RESULT 0, or 6 when the server cannot open a UDP socket), both directions of the stream carry packets:

```
+--------+----------+----------+
| LENGTH | ADDR     | PAYLOAD  |
+--------+----------+----------+
| u16be  | variable | variable |
+--------+----------+----------+
```

- `LENGTH`: the size of `ADDR` and `PAYLOAD` together, from 1 to 65535.
- `ADDR` (Section 5.1): from the client, the packet's destination, which may be a domain name; from the server, the packet's source, always an IP address.

The server sends packets for a stream from one UDP socket, and relays back every packet that socket receives, from any source. A domain-name destination that cannot be resolved drops the packet. A receiver MUST skip a packet whose `LENGTH` is 0 or whose `ADDR` is malformed, and carry on with the next packet. Packets are never fragmented. The association lasts until the stream ends.

## 6. Error Handling

| Condition | Action |
|---|---|
| A chunk fails authentication, or `L` is 0 | close the carrier |
| Bad hello chunk, or a remembered `client_salt` | server closes the carrier without replying |
| Bad acknowledgement chunk, or a version the client does not speak | client closes the carrier |
| Frame with `VER` other than 2, an unknown `CMD`, or a bad `LENGTH` | close the carrier |
| Unknown control command, or a control body of the wrong length | ignore the message |
| Request with an unknown command or a malformed `ADDR` | server closes the stream without replying |
| RESULT other than 0 | the stream has failed; the server sends FIN |
| Malformed UDP packet | skip the packet |
| The carrier closes | abort every stream still open on it |

## 7. Test Vectors

These vectors fix the random salts to show the bytes exactly. The PSK is the string `hikyaku example psk`. The client's first flight opens stream 3 with a CONNECT request to `example.com:443` and no early data, then sends PING with `id` 42; the server's first flight acknowledges and answers stream 3 with ACK and RESULT 0, each in its own PSH frame. All values are hexadecimal.

### 7.1. Inputs

```
psk          68696b79616b75206578616d706c652070736b
client_salt  000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f
server_salt  202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f
```

### 7.2. Keys

```
client_key  ea591a6949239333dda27365905f91f7413ee0f2c9aba94ac84827730a5dd484
server_key  aa5af7fb82b681188da8de016a1d900651a73ac0f6aa1d3f6599940f2c6a2f87
```

### 7.3. Client

Plaintexts:

```
hello payload         00
SYN frame, stream 3   0200000003000000
CONNECT request       10030b6578616d706c652e636f6d01bb
PSH frame, stream 3   020210000300000010030b6578616d706c652e636f6d01bb
PING control payload  010000002a
```

Chunks, encrypted with `client_key`:

```
hello chunk (nonces 0, 1)  9a4a77df2069e5f0c47c6ba606ea9b8573d22e7278aad7af0bbe2e894c23b4f55b8994
SYN chunk (nonces 2, 3)    313b29dc680a93edee6c933cbdaf0380faffc5497655bbd32fc441c4d53d12878decab1b8bccb64ddf00
PSH chunk (nonces 4, 5)    5a71608a641eaef1e3e4b24a804592c4ff0017cbc3865eb09457b006600ca6fe75e75b7b96da6b15d41bbc6356cc4cd5dcb3a5fd2380e49a793e
PING chunk (nonces 6, 7)   af29bbd8d1f67e3e4234b8edc3d09f1dd5a6243c1cde37730db375994b51bc93b2887960635e03
```

First flight, `client_salt || hello chunk || SYN chunk || PSH chunk`:

```
000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f9a4a77df2069e5f0c47c6ba606ea9b8573d22e7278aad7af0bbe2e894c23b4f55b8994313b29dc680a93edee6c933cbdaf0380faffc5497655bbd32fc441c4d53d12878decab1b8bccb64ddf005a71608a641eaef1e3e4b24a804592c4ff0017cbc3865eb09457b006600ca6fe75e75b7b96da6b15d41bbc6356cc4cd5dcb3a5fd2380e49a793e
```

### 7.4. Server

Plaintexts:

```
acknowledgement payload            00
PSH frame with ACK, stream 3       020201000300000020
PSH frame with RESULT 0, stream 3  0202030003000000210000
```

Chunks, encrypted with `server_key`:

```
acknowledgement chunk (nonces 0, 1)  60cd0b8330d8e0ac38995ad69e7d2fb17a75564395a818937bb53be5a2001e77a9b2ed
ACK chunk (nonces 2, 3)              ff35e6a7fd510a3444e0668b54235e7dfda67c08521ed52ccba47fe481495140a7be67433f1a9da664f4d9
RESULT chunk (nonces 4, 5)           ddee82fbd141c69c31eeb85aa919af4764f89f76401c92c036734bfe7f8a04ded8bf93d2704bf730cd6c42f6b5
```

First flight, `server_salt || acknowledgement chunk || ACK chunk || RESULT chunk`:

```
202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f60cd0b8330d8e0ac38995ad69e7d2fb17a75564395a818937bb53be5a2001e77a9b2edff35e6a7fd510a3444e0668b54235e7dfda67c08521ed52ccba47fe481495140a7be67433f1a9da664f4d9ddee82fbd141c69c31eeb85aa919af4764f89f76401c92c036734bfe7f8a04ded8bf93d2704bf730cd6c42f6b5
```
