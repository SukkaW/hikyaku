<h1 align="center">🏃‍♂️ hikyaku</h1>
<p align="center"><sup>(飛脚, ひきゃく, <em>courier</em> in Japanese)</sup></p>
<p align="center">A high-performance multiplexing proxy that stays lightning fast and highly reliable everywhere,<br />from fiber broadband to lossy in-flight Wi-Fi.</p>

- 0-RTT saves a full round trip on every new connection, about 600 ms on satellite in-flight Wi-Fi.
- Connections open lazily as traffic grows, keep a spare ready, and are reused across requests.
- Every request goes to whichever connection is answering fastest right now.
- Every request doubles as a health check, telling a dead connection apart from a dead website.
- Keepalives follow each connection's real round trip: frequent while it's in use, rare when idle.
- A connection stuck on a slow route or QoS class is swapped for a fresh one.
- If a connection dies before a TLS request is answered, the request moves to a healthy one without the app noticing.
- Short outages don't drop open connections, and traffic resumes the moment the link comes back.
- Server restarts are graceful: open streams finish while new requests already go to the new server.
- After a pause, the first bytes of a response leave in a single TCP segment instead of waiting for a full record.
- Replay protection needs no clock, so a device with the wrong time still connects.

# Protocol Specification

[PROTOCOL.md](PROTOCOL.md)

# Usage

## Install

Requires Go 1.27.3 or later.

```sh
go install github.com/SukkaW/hikyaku/cmd/hikyaku-server@latest
go install github.com/SukkaW/hikyaku/cmd/hikyaku-client@latest
```

## Server

Run the server on a host the client can reach over TCP. Any string works as the pre-shared key.

```sh
hikyaku-server -psk 'your-psk'
```

| Flag      | Default    | Description                                      |
| --------- | ---------- | ------------------------------------------------ |
| `-listen` | `:8444`    | Address to listen on for clients                 |
| `-psk`    | (required) | Pre-shared key; the client must use the same one |

On Linux, the server uses BBR congestion control when the kernel provides it (for example after `modprobe tcp_bbr`); otherwise it logs a warning and uses the system default.

## Client

The client is a local SOCKS5 proxy, for both TCP (`CONNECT`) and UDP (`UDP ASSOCIATE`).

```sh
hikyaku-client -server 203.0.113.10:8444 -psk 'your-psk'
curl --socks5-hostname 127.0.0.1:1080 https://example.com
```

| Flag        | Default          | Description                                              |
| ----------- | ---------------- | -------------------------------------------------------- |
| `-listen`   | `127.0.0.1:1080` | SOCKS5 listen address                                    |
| `-server`   | (required)       | Server address, `host:port`                              |
| `-psk`      | (required)       | Pre-shared key, the same as the server's                 |
| `-carriers` | `4`              | Maximum number of connections to the server (at least 1) |

## Surge for Mac

Surge can start the client itself as an [External Proxy Program](https://manual.nssurge.com/policies/external.html):

```ini
[Proxy]
hikyaku = external, exec = "/path/to/hikyaku-client", args = "-server", args = "203.0.113.10:8444", args = "-psk", args = "your-psk", local-port = 1080, addresses = 203.0.113.10, udp-relay = true
```

- `exec` is the full path to `hikyaku-client`; after `go install`, that is `$(go env GOPATH)/bin/hikyaku-client`.
- `local-port` must match the port of the client's `-listen`, `127.0.0.1:1080` by default.
- `addresses` takes the server's IP address, not a hostname.
- `udp-relay = true` sends UDP through hikyaku as well.

# License

[MIT](LICENSE)

----

**hikyaku** © [Sukka](https://github.com/SukkaW), Authored and maintained by Sukka with help from contributors ([list](https://github.com/SukkaW/hikyaku/graphs/contributors)).

> [Personal Website](https://skk.moe) · [Blog](https://blog.skk.moe) · GitHub [@SukkaW](https://github.com/SukkaW) · Telegram Channel [@SukkaChannel](https://t.me/SukkaChannel) · Mastodon [@sukka@acg.mn](https://acg.mn/@sukka) · Twitter [@isukkaw](https://twitter.com/isukkaw) · BlueSky [@skk.moe](https://bsky.app/profile/skk.moe)

<p align="center">
  <a href="https://github.com/sponsors/SukkaW/">
    <img src="https://sponsor.cdn.skk.moe/sponsors.svg"/>
  </a>
</p>
