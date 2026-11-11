<h1 align="center">🏃‍♂️ hikyaku</h1>
<p align="center"><sup>(飛脚, ひきゃく, <em>courier</em> in Japanese)</sup></p>
<p align="center">A high-performance multiplexing proxy that stays lightning fast and highly reliable everywhere,<br />from fiber broadband to lossy in-flight Wi-Fi.</p>

- With **0-RTT**, every new connection saves a full round trip, especially noticeable on high-latency links like the 600 ms on satellite in-flight Wi-Fi.
- With **dynamic record sizing**, responses start arriving sooner, especially on lossy links, while large downloads still run at full speed.
- A **warm connection pool** has connections ready before you need them and reuses them across requests.
- Every request takes the fastest, least busy connection, powered by **latency-aware load balancing**.
- **Adaptive keepalives and healthchecks** keeps congestion windows open, continuously monitored for responsiveness and reliability, without consuming unnecessary resources.
- Connections stuck on a slow route or QoS throttling are replaced automatically by **slow-path escape**.
- During flapping connections, **transparent failover** moves TLS handshakes still in flight to a healthy one, so apps barely notice.
- **Outage tolerance** keeps open connections alive through outages under 30 seconds.
- **Graceful server restarts** give in-flight requests time to finish while new ones go to the new server.
- **Encrypted DNS support for server-side** because why not.
- **Smart and intelligent DNS** with progressive parallelization, concurrent dialing, aggressive versions of happy eyeball v2 with simultaneous racing, optimistic caching, and more.

> [!NOTE]
> hikyaku is designed for network acceleration and reliability improvements. Circumventing censorship was not, is not, and will never be, part of hikyaku's design goals.

# Protocol Specification

[PROTOCOL.md](PROTOCOL.md)

# Usage

## Install

### Homebrew

```sh
brew install sukkaw/tap/hikyaku
```

### GitHub Releases

Prebuilt binaries for macOS, Linux and Windows (x86-64 and ARM64) and FreeBSD (x86-64) are available on [GitHub Releases](https://github.com/SukkaW/hikyaku/releases/latest). Each archive contains both `hikyaku-server` and `hikyaku-client`.

### Go

Requires Go 1.27.3 or later.

```sh
go install github.com/SukkaW/hikyaku/cmd/hikyaku-server@latest
go install github.com/SukkaW/hikyaku/cmd/hikyaku-client@latest
```

## Server

Run the server on a host the client can reach over TCP. Any string works as the pre-shared key.

```sh
# minimal server example
hikyaku-server -psk 'your-psk'
# full configuration example
hikyaku-server -listen ':8444' -psk 'your-psk' -dns 'https://1.1.1.1/dns-query' -dns 'tls://8.8.8.8' -dns 'h3://1.0.0.1/dns-query'
```

| Flag      | Default             | Description                                                                                                                                                                                          |
| --------- | ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-listen` | `:8444`             | Address to listen on for clients                                                                                                                                                                     |
| `-psk`    | (required)          | Pre-shared key; the client must use the same one                                                                                                                                                     |
| `-dns`    | the system resolver | DNS server for destinations; repeat it to use several. An IP, or a `udp://`, `tcp://`, `tls://`, `https://`, `quic://`, `h3://` or `sdns://` address. The server's hosts file is still checked first |

On Linux, the server uses BBR congestion control when the kernel has BBR and either the server runs as root, or `bbr` is the system default or listed in `net.ipv4.tcp_allowed_congestion_control`. Otherwise it logs a warning for each client connection and uses the system default.

## Client

The client exposes a local SOCKS5 proxy, supporting both TCP (`CONNECT`) and UDP (`UDP ASSOCIATE`).

```sh
# minimal client example
hikyaku-client -server 203.0.113.10:8444 -psk 'your-psk'
# full client example with custom listen port
hikyaku-client -listen 127.0.0.1:11080 -server 203.0.113.10:8444 -psk 'your-psk' -carriers 8

# test the SOCKS5 proxy through the client
curl --socks5-hostname 127.0.0.1:1080 https://example.com
```

| Flag        | Default          | Description                                                     |
| ----------- | ---------------- | --------------------------------------------------------------- |
| `-listen`   | `127.0.0.1:1080` | SOCKS5 listen address                                           |
| `-server`   | (required)       | Server address, `host:port`                                     |
| `-psk`      | (required)       | Pre-shared key, the same as the server's                        |
| `-carriers` | `4`              | Maximum number of active connections to the server (at least 1) |

## Surge for Mac

Surge can start the client itself as an [External Proxy Program](https://manual.nssurge.com/policies/external.html):

```ini
[Proxy]
hikyaku = external, exec = "/path/to/hikyaku-client", args = "-listen", args = "127.0.0.1:11080", args = "-server", args = "203.0.113.10:8444", args = "-psk", args = "your-psk", local-port = 11080, addresses = 203.0.113.10, udp-relay = true
```

- `exec` is the full path to `hikyaku-client`; after `go install`, it is in `GOBIN`, or in `bin` under the first `GOPATH` entry when `GOBIN` is unset.
- `local-port` must match the port of the client's `-listen`.
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
