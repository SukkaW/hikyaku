#!/usr/bin/env python3
"""Compare hikyaku, satmux and TUIC v5 over netsim's emulated paths.

Run as root on Linux (see tools/netsim/Dockerfile). Every system gets its own netsim
instance per profile, with its server and the test website in that instance's server
namespace and its client in the client namespace. It measures through each system's
SOCKS5 port: cold start, a 6-way burst of small HTTPS fetches like a page load,
downloads sized to about 8 s at the profile's download cap (1-10 MB), and small
fetches during a download. --quick, for prototyping, replaces the downloads and the
loaded test with one short download that has two small fetches made during it.

Runs on paths capped at 100 Mbit/s or less all run at once, since they spend their
time waiting on delay; faster paths keep the CPU busy, so they run one at a time in a
lane of their own alongside.

Every system's client starts START_OFFSET_S after its netsim is ready, so seeded
outages and spikes reach each system at the same point in its test, and TUIC uses the
congestion control the kernel gives hikyaku's server. --repeat runs everything again
with fresh seeds and reports medians with their range.
"""
import argparse
import concurrent.futures
import json
import os
import secrets
import signal
import statistics
import subprocess
import sys
import threading
import time


def each_way(percent):
    """netsim's -loss is per round trip; convert a per-direction loss rate to it."""
    return f"{(1 - (1 - percent / 100) ** 2) * 100:.6f}"


# name: (label, netsim arguments). The first four are synthetic paths, with 620 ms /
# 10% uniform loss kept as a harsh stress test. The rest approximate measured in-flight
# Wi-Fi: GEO round trips of 620-750 ms with little random loss, and Starlink with
# 30-65 ms; outage, spike and queue settings are engineering estimates. geo-congested-fq
# is congested GEO behind a bottleneck that queues each connection separately; whether
# real cabin links do so is unknown. geo-deepfq gives each connection a 1.5 s queue, deep
# enough that a busy carrier's replies wait behind its own data for seconds.
PROFILES = {
    "10ms": ("10 ms, 0% loss, 500 Mbit/s", ["-rtt", "10ms", "-down-rate", "500", "-up-rate", "500"]),
    "50ms": ("50 ms, 0% loss, 500 Mbit/s", ["-rtt", "50ms", "-down-rate", "500", "-up-rate", "500"]),
    "200ms": ("200 ms, 5% loss, 100 Mbit/s", ["-rtt", "200ms", "-loss", "5", "-down-rate", "100", "-up-rate", "100"]),
    "620ms": ("Harsh stress: 620 ms, 10% uniform loss, 5 Mbit/s",
              ["-rtt", "620ms", "-loss", "10", "-down-rate", "5", "-up-rate", "5"]),
    "geo-healthy": ("Healthy GEO: 700 ms, 0.05% loss each way, 50/4 Mbit/s",
                    ["-rtt", "700ms", "-loss", each_way(0.05), "-down-rate", "50", "-up-rate", "4", "-queue", "100ms"]),
    "geo-congested": ("Congested GEO: 620 ms, queue drops only, 5.9/3.9 Mbit/s",
                      ["-rtt", "620ms", "-down-rate", "5.9", "-up-rate", "3.9", "-queue", "250ms"]),
    "geo-congested-fq": ("Congested GEO, queued per connection: 620 ms, 5.9/3.9 Mbit/s",
                         ["-rtt", "620ms", "-down-rate", "5.9", "-up-rate", "3.9", "-queue", "250ms", "-fq"]),
    "geo-deepfq": ("Congested GEO, 1.5 s queue per connection: 620 ms, 5.9/3.9 Mbit/s",
                   ["-rtt", "620ms", "-down-rate", "5.9", "-up-rate", "3.9", "-queue", "1500ms", "-fq"]),
    "geo-impaired": ("Impaired GEO: 750 ms, 0.1% loss each way, ~10 s blackouts every ~90 s, 5/1 Mbit/s",
                     ["-rtt", "750ms", "-loss", each_way(0.1), "-outage-every", "90s", "-outage-for", "10s",
                      "-down-rate", "5", "-up-rate", "1", "-queue", "250ms"]),
    "starlink": ("Starlink continental: 30 ms, 0.02% loss each way, 125/15 Mbit/s, cabin spikes, 15 s pauses",
                 ["-rtt", "30ms", "-loss", each_way(0.02), "-down-rate", "125", "-up-rate", "15", "-queue", "150ms",
                  "-spike-every", "60s", "-spike-for", "600ms", "-spike-min", "200ms", "-spike-max", "450ms",
                  "-pause-every", "15s", "-pause-down", "45ms", "-pause-up", "68ms"]),
    "starlink-oceanic": ("Starlink oceanic: 65 ms, 0.02% loss each way, ~1 s blackout every ~10 min, 65/24 Mbit/s",
                         ["-rtt", "65ms", "-loss", each_way(0.02), "-outage-every", "10m", "-outage-for", "1s",
                          "-down-rate", "65", "-up-rate", "24", "-queue", "150ms"]),
}
# hikyaku client log lines that mark a carrier health decision.
CARRIER_EVENTS = {"slow": "slower than", "acknowledgement overdue": "acknowledgement overdue", "silent": "silent for",
                  "no acknowledgement": "no acknowledgement", "link probing": "probing"}
SMALL = "https://localhost:9443/"
BIG = "http://localhost:9001/big?n={}"
# A 100 KB page fetched after the user has been reading for a while: the carrier is
# warm but idle, so the server's TCP may have shrunk its congestion window.
AFTER_IDLE_URL = "https://localhost:9443/big?n=100000"
AFTER_IDLE_S = 5
BULK_SECONDS = 8
BULK_LIMIT_S = 20
QUICK_BULK_LIMIT_S = 6
PARALLEL_MAX_MBPS = 100
START_OFFSET_S = 3
# Linux congestion control names and TUIC's equivalents.
TUIC_CONGESTION = {"bbr": "bbr", "cubic": "cubic", "reno": "new_reno"}
ENV = {k: v for k, v in os.environ.items() if "proxy" not in k.lower()}
HERE = os.path.dirname(os.path.abspath(__file__))

procs = []
procs_lock = threading.Lock()


class Run:
    """One system on one profile, on its own netsim instance and test website."""

    def __init__(self, profile, system, instance, repeat=0):
        self.profile, self.system, self.instance, self.repeat = profile, system, instance, repeat
        suffix = f"-{instance}" if instance else ""
        self.client_ns, self.server_ns = f"hk-client{suffix}", f"hk-server{suffix}"
        self.client = f"10.200.{instance}.1"
        self.server = f"10.200.{instance}.2"
        self.client_dev = f"hkc{instance}" if instance else "hkc"
        self.server_dev = f"hks{instance}" if instance else "hks"
        self.procs = []

    def path(self, name):
        again = f"-r{self.repeat + 1}" if opts.repeat > 1 else ""
        return os.path.join(opts.out, f"{self.profile}-{self.system}{again}-{name}")


def say(msg):
    print(msg, flush=True)


def spawn(run, args, name, side=None, env=None):
    log = open(run.path(name + ".log"), "w")
    ns = {"client": run.client_ns, "server": run.server_ns}.get(side)
    cmd = (["ip", "netns", "exec", ns] if ns else []) + args
    p = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env={**ENV, **(env or {})},
                         start_new_session=True)
    with procs_lock:
        procs.append(p)
    run.procs.append(p)
    return p


def stop(p):
    if p.poll() is None:
        os.killpg(p.pid, signal.SIGTERM)
        try:
            p.wait(5)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)
            p.wait()
    with procs_lock:
        if p in procs:
            procs.remove(p)


def wait_for(run, name, needle, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with open(run.path(name + ".log")) as f:
            if needle in f.read():
                return
        time.sleep(0.1)
    raise RuntimeError(f"{name} did not print {needle!r} within {timeout}s")


def wait_listening(run, proto, port, timeout=10):
    """Wait until a server in run's server namespace listens on port."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = subprocess.run(["ip", "netns", "exec", run.server_ns, "ss", "-Hltn" if proto == "tcp" else "-Hlun",
                            f"sport = :{port}"], capture_output=True, text=True)
        if r.stdout.strip():
            return
        time.sleep(0.05)
    raise RuntimeError(f"{run.system} server is not listening on {proto} port {port} after {timeout}s")


def congestion_control(run):
    """The algorithm hikyaku's server gets: BBR when the kernel offers it, since hikyaku
    asks for it, otherwise the kernel's default, which satmux always uses."""
    def read(name):
        return subprocess.run(["ip", "netns", "exec", run.server_ns, "cat", f"/proc/sys/net/ipv4/{name}"],
                              capture_output=True, text=True, check=True).stdout.split()
    available, default = read("tcp_available_congestion_control"), read("tcp_congestion_control")[0]
    return ("bbr" if "bbr" in available else default), default


def curl(run, socks, url, max_time):
    cmd = ["ip", "netns", "exec", run.client_ns, "curl", "-sS", "-o", "/dev/null", "-w", "%{time_total} %{size_download}",
           "--max-time", str(max_time), "--cacert", os.path.join(opts.certs, "ca.pem"), "--socks5-hostname", socks, url]
    r = subprocess.run(cmd, capture_output=True, text=True, env=ENV, timeout=max_time + 15)
    parts = r.stdout.split()
    seconds = float(parts[0]) if parts else None
    size = int(float(parts[1])) if len(parts) > 1 else 0
    return r.returncode == 0, seconds, size


def first_byte(run, socks, url, max_time):
    """Fetch url and return whether it worked, the time to first byte and the total."""
    cmd = ["ip", "netns", "exec", run.client_ns, "curl", "-sS", "-o", "/dev/null", "-w",
           "%{time_starttransfer} %{time_total}", "--max-time", str(max_time), "--cacert",
           os.path.join(opts.certs, "ca.pem"), "--socks5-hostname", socks, url]
    r = subprocess.run(cmd, capture_output=True, text=True, env=ENV, timeout=max_time + 15)
    parts = r.stdout.split()
    if r.returncode != 0 or len(parts) != 2:
        return False, None, None
    return True, float(parts[0]), float(parts[1])


def after_idle(run, socks, result):
    time.sleep(AFTER_IDLE_S)
    ok, ttfb, total = first_byte(run, socks, AFTER_IDLE_URL, 30)
    result["after_idle_ttfb_s"] = round(ttfb, 3) if ok else None
    result["after_idle_s"] = round(total, 3) if ok else None


def certs():
    ca, leaf = os.path.join(opts.certs, "ca.pem"), os.path.join(opts.certs, "leaf.pem")
    if os.path.exists(ca) and os.path.exists(leaf):
        return
    os.makedirs(opts.certs, exist_ok=True)
    d = opts.certs
    run = lambda *a: subprocess.run(a, check=True, capture_output=True, cwd=d)
    run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key", "-out", "ca.pem",
        "-days", "30", "-subj", "/CN=hikyaku netsim CA")
    run("openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", "leaf.key", "-out", "leaf.csr",
        "-subj", "/CN=localhost")
    with open(os.path.join(d, "leaf.ext"), "w") as f:
        f.write("subjectAltName=DNS:localhost,IP:127.0.0.1\n")
    run("openssl", "x509", "-req", "-in", "leaf.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial",
        "-out", "leaf.pem", "-days", "30", "-extfile", "leaf.ext")


# Each system: start its server (hk-server) and wait until it listens, then return a
# function that starts its client (hk-client), the client's SOCKS5 address, and the
# congestion control the system's server-to-client traffic uses.
def hikyaku(run, cc):
    psk = secrets.token_urlsafe(24)
    spawn(run, [os.path.join(opts.hikyaku, "hikyaku-server"), "-listen", f"{run.server}:8445", "-psk", psk],
          "hikyaku-server", "server")
    wait_listening(run, "tcp", 8445)
    client = [os.path.join(opts.hikyaku, "hikyaku-client"), "-listen", "127.0.0.1:1080", "-server",
              f"{run.server}:8445", "-psk", psk, "-carriers", "4"]
    return lambda: spawn(run, client, "hikyaku-client", "client"), "127.0.0.1:1080", cc[0]


def satmux(run, cc):
    psk = secrets.token_hex(32)
    spawn(run, ["python3", opts.satmux, "server", "--host", run.server, "--port", "8444"], "satmux-server", "server",
          {"SATMUX_PSK": psk})
    wait_listening(run, "tcp", 8444)
    client = ["python3", opts.satmux, "client", "--host", run.server, "--port", "8444", "--carriers", "12",
              "--socks-port", "1081", "--http-port", "18080", "--fast-port", "18081",
              "--gh-sock", run.path("gh.sock")]
    return lambda: spawn(run, client, "satmux-client", "client", {"SATMUX_PSK": psk}), "127.0.0.1:1081", cc[1]


def tuic(run, cc):
    uuid, password = "11111111-2222-3333-4444-555555555555", secrets.token_urlsafe(24)
    if cc[0] not in TUIC_CONGESTION:
        say(f"warning: TUIC has no {cc[0]}; using cubic")
    congestion = TUIC_CONGESTION.get(cc[0], "cubic")
    server_toml, client_toml = run.path("tuic-server.toml"), run.path("tuic-client.toml")
    with open(server_toml, "w") as f:
        f.write(f'log_level = "info"\nserver = "{run.server}:7444"\n[users]\n"{uuid}" = "{password}"\n'
                f'[tls]\ncertificate = "{opts.certs}/leaf.pem"\nprivate_key = "{opts.certs}/leaf.key"\n'
                f'alpn = ["h3"]\nhostname = "localhost"\n[experimental]\ndrop_loopback = false\n')
    with open(client_toml, "w") as f:
        f.write(f'log_level = "info"\n[relay]\nserver = "localhost:7444"\nip = "{run.server}"\nuuid = "{uuid}"\n'
                f'password = "{password}"\ncertificates = ["{opts.certs}/ca.pem"]\nalpn = ["h3"]\n'
                f'congestion_control = "{congestion}"\n[local]\nserver = "127.0.0.1:1082"\n')
    spawn(run, [os.path.join(opts.tuic, "tuic-server"), "-c", server_toml], "tuic-server", "server")
    wait_listening(run, "udp", 7444)
    client = [os.path.join(opts.tuic, "tuic-client"), "-c", client_toml]
    return lambda: spawn(run, client, "tuic-client", "client"), "127.0.0.1:1082", congestion


SYSTEMS = {"hikyaku": hikyaku, "satmux": satmux, "tuic": tuic}


def measure(run, start_client, socks, size):
    result = {}
    t0 = time.time()
    start_client()
    while True:
        ok, _, _ = curl(run, socks, SMALL, 20)
        if ok:
            result["cold_s"] = round(time.time() - t0, 2)
            break
        if time.time() - t0 > 60:
            result["cold_s"] = None
            break
        time.sleep(0.1)

    def burst():
        with concurrent.futures.ThreadPoolExecutor(6) as ex:
            return list(ex.map(lambda _: curl(run, socks, SMALL, 30), range(6)))

    burst()  # let pools that grow on demand reach their working size
    time.sleep(0.3 if opts.quick else 1)

    b = burst()
    result["burst_max_s"] = round(max(s for ok, s, _ in b if ok), 3) if all(ok for ok, _, _ in b) else None
    result["burst_failures"] = sum(1 for ok, _, _ in b if not ok)
    result["bulk_mb"] = round(size / 1e6, 1)

    if opts.quick:
        # One download, with two small fetches made during it, stands in for both the
        # timed downloads and the loaded test.
        with concurrent.futures.ThreadPoolExecutor(1) as ex:
            download = ex.submit(curl, run, socks, BIG.format(size), QUICK_BULK_LIMIT_S)
            time.sleep(1)
            loaded = [curl(run, socks, SMALL, 30) for _ in range(2)]
            _, seconds, received = download.result()
        result["bulk_mbps"] = round(received * 8 / seconds / 1e6 if seconds else 0.0, 2)
        result["bulk_runs_mbps"] = [result["bulk_mbps"]]
        loaded_times = [s for ok, s, _ in loaded if ok]
        result["loaded_median_s"] = round(statistics.median(loaded_times), 3) if loaded_times else None
        result["loaded_failures"] = sum(1 for ok, _, _ in loaded if not ok)
        after_idle(run, socks, result)
        return result

    # Throughput is bytes over time, so a download that hits the limit still counts.
    rates = []
    for _ in range(3):
        _, seconds, received = curl(run, socks, BIG.format(size), BULK_LIMIT_S)
        rates.append(received * 8 / seconds / 1e6 if seconds else 0.0)
    result["bulk_mbps"] = round(statistics.median(rates), 2)
    result["bulk_runs_mbps"] = [round(r, 2) for r in rates]

    # Twice the timed size, so the link stays busy through the small fetches.
    background = subprocess.Popen(
        ["ip", "netns", "exec", run.client_ns, "curl", "-sS", "-o", "/dev/null", "--max-time", str(2 * BULK_LIMIT_S),
         "--socks5-hostname", socks, BIG.format(2 * size)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        env=ENV, start_new_session=True)
    time.sleep(2)
    loaded = [curl(run, socks, SMALL, 30) for _ in range(3)]
    os.killpg(background.pid, signal.SIGTERM)
    background.wait()
    loaded_times = [s for ok, s, _ in loaded if ok]
    result["loaded_median_s"] = round(statistics.median(loaded_times), 3) if loaded_times else None
    result["loaded_failures"] = sum(1 for ok, _, _ in loaded if not ok)
    after_idle(run, socks, result)
    return result


def execute(run, netsim_args, size):
    try:
        spawn(run, [opts.netsim, "-id", str(run.instance), *netsim_args], "netsim")
        wait_for(run, "netsim", "ready", 10)
        ready_at = time.time()
        # Server TCP settings under test apply to this run's server namespace only.
        for setting in opts.server_sysctl:
            subprocess.run(["ip", "netns", "exec", run.server_ns, "sysctl", "-qw", setting], check=True)
        if opts.initcwnd:
            # The client must also advertise a window that large, as macOS does by default.
            subprocess.run(["ip", "-n", run.server_ns, "route", "change", run.client, "dev", run.server_dev,
                            "initcwnd", str(opts.initcwnd)], check=True)
            subprocess.run(["ip", "-n", run.client_ns, "route", "change", run.server, "dev", run.client_dev,
                            "initrwnd", str(opts.initcwnd)], check=True)
        spawn(run, ["python3", os.path.join(HERE, "origin.py"), os.path.join(opts.certs, "leaf.pem"),
                    os.path.join(opts.certs, "leaf.key")], "origin", "server")
        wait_for(run, "origin", "origin ready", 10)
        start_client, socks, congestion = SYSTEMS[run.system](run, congestion_control(run))
        # Seeded outages and spikes count from netsim's start, so every system starts
        # measuring at the same point in that pattern.
        wait = ready_at + START_OFFSET_S - time.time()
        if wait > 0:
            time.sleep(wait)
        else:
            say(f"warning: {run.profile} {run.system} took {START_OFFSET_S - wait:.1f}s to start, "
                f"past the {START_OFFSET_S}s start offset")
        result = measure(run, start_client, socks, size)
        result["congestion_control"] = congestion
        if run.system == "hikyaku":
            with open(run.path("hikyaku-client.log")) as f:
                log = f.read()
            result["carrier_events"] = {k: log.count(v) for k, v in CARRIER_EVENTS.items()}
        return result
    finally:
        for p in run.procs[::-1]:
            stop(p)


def failures(r):
    return (r["burst_failures"] + r["loaded_failures"] + (r["cold_s"] is None) +
            (r.get("after_idle_s", 0) is None))


def combine(repeats):
    """One result per system and profile: a single run as it is, or the medians of the
    repeats with each value's range under "range"."""
    if len(repeats) == 1:
        return repeats[0]
    out = {"repeats": repeats, "range": {}, "congestion_control": repeats[0]["congestion_control"]}
    for key in ("cold_s", "burst_max_s", "bulk_mbps", "loaded_median_s", "after_idle_ttfb_s", "after_idle_s"):
        values = [r[key] for r in repeats if r.get(key) is not None]
        out[key] = round(statistics.median(values), 3) if values else None
        if values:
            out["range"][key] = (min(values), max(values))
    out["bulk_mb"] = repeats[0]["bulk_mb"]
    out["failures"] = sum(failures(r) for r in repeats)
    if "carrier_events" in repeats[0]:
        out["carrier_events"] = {k: sum(r["carrier_events"][k] for r in repeats) for k in repeats[0]["carrier_events"]}
    return out


def table(results):
    head = ("| Profile | System | Cold start | 6-way burst | Download | Small fetch during download "
            "| 100 KB after 5 s idle (first byte / total) | Congestion control | Failures |")
    lines = [head, "|" + "---|" * 9]
    for profile, systems in results.items():
        for system, r in systems.items():
            spread = r.get("range", {})

            def fmt(key, unit="s"):
                if r.get(key) is None:
                    return "fail"
                low_high = f" ({spread[key][0]}–{spread[key][1]})" if key in spread else ""
                return f"{r[key]} {unit}{low_high}"
            lines.append(f"| {profile} | {system} | {fmt('cold_s')} | {fmt('burst_max_s')} | {fmt('bulk_mbps', 'Mbit/s')} "
                         f"({r['bulk_mb']} MB) | {fmt('loaded_median_s')} "
                         f"| {fmt('after_idle_ttfb_s')} / {fmt('after_idle_s')} | {r['congestion_control']} "
                         f"| {r['failures'] if 'failures' in r else failures(r)} |")
    return "\n".join(lines)


def main():
    global opts
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--netsim", default="/usr/local/bin/netsim")
    ap.add_argument("--hikyaku", default="/usr/local/bin", help="directory with hikyaku-server and hikyaku-client")
    ap.add_argument("--tuic", default="/usr/local/bin", help="directory with tuic-server and tuic-client")
    ap.add_argument("--satmux", default="/opt/satmux/satmux.py")
    ap.add_argument("--certs", default="/tmp/netsim-certs")
    ap.add_argument("--out", default="/tmp/netsim-out")
    ap.add_argument("--profiles", default=",".join(PROFILES))
    ap.add_argument("--systems", default=",".join(SYSTEMS))
    ap.add_argument("--sequential", action="store_true", help="run one system and profile at a time")
    ap.add_argument("--quick", action="store_true", help="fewer samples, about 15 s per GEO run, for prototyping")
    ap.add_argument("--server-sysctl", action="append", default=[], metavar="KEY=VALUE",
                    help="sysctl to set in each server namespace, e.g. net.ipv4.tcp_slow_start_after_idle=0")
    ap.add_argument("--initcwnd", type=int, default=0, help="initial congestion window for server-to-client routes")
    ap.add_argument("--seed", type=int, default=None,
                    help="base seed for netsim's loss, outage and spike patterns (random by default); each profile "
                         "adds its position in --profiles, and every system on a profile gets the same pattern")
    ap.add_argument("--repeat", type=int, default=1, help="run everything this many times, each with fresh seeds, "
                    "and report medians with their range")
    ap.add_argument("--jobs", type=int, default=0, help="at most this many runs at once on paths capped at "
                    f"{PARALLEL_MAX_MBPS} Mbit/s or less (0: no limit)")
    opts = ap.parse_args()
    os.makedirs(opts.out, exist_ok=True)
    certs()
    seed = opts.seed if opts.seed is not None else secrets.randbelow(2**32)

    plans = []  # (run, label, netsim arguments, download size, capped)
    profiles, systems = opts.profiles.split(","), opts.systems.split(",")
    if len(profiles) * len(systems) * opts.repeat > 255:
        sys.exit("too many runs: netsim has 255 instances")
    for repeat in range(opts.repeat):
        for position, name in enumerate(profiles):
            label, netsim_args = PROFILES[name]
            rate = lambda flag, args=netsim_args: float(args[args.index(flag) + 1])
            size = int(min(max(rate("-down-rate") * 1e6 / 8 * BULK_SECONDS, 1e6), 10e6))
            capped = max(rate("-down-rate"), rate("-up-rate")) <= PARALLEL_MAX_MBPS
            netsim_args = [*netsim_args, "-seed", str(seed + position + 1000 * repeat)]
            for system in systems:
                plans.append((Run(name, system, len(plans) + 1, repeat), label, netsim_args, size, capped))

    measured = {}

    def go(plan):
        run, label, netsim_args, size, _ = plan
        result = execute(run, netsim_args, size)
        measured[(label, run.system, run.repeat)] = result
        again = f" (repeat {run.repeat + 1})" if opts.repeat > 1 else ""
        say(f"== {label}: {run.system}{again}: {result}")

    mode = "quick" if opts.quick else "full"
    say(f"== {len(plans)} runs ({mode}), seed {seed}{', one at a time' if opts.sequential else ''}"
        f"{f', at most {opts.jobs} capped runs at once' if opts.jobs > 0 and not opts.sequential else ''}")
    try:
        if opts.sequential:
            for plan in plans:
                go(plan)
        else:
            capped = [p for p in plans if p[4]]
            fast = [p for p in plans if not p[4]]
            with concurrent.futures.ThreadPoolExecutor(min(len(capped), opts.jobs or len(capped)) or 1) as ex, \
                    concurrent.futures.ThreadPoolExecutor(1) as lane:
                futures = [ex.submit(go, p) for p in capped]
                futures.append(lane.submit(lambda: [go(p) for p in fast]))
                for f in futures:
                    f.result()
    finally:
        for p in procs[::-1]:
            stop(p)
    results = {}
    for run, label, *_ in plans:
        if run.repeat == 0:
            repeats = [measured[(label, run.system, r)] for r in range(opts.repeat) if (label, run.system, r) in measured]
            if repeats:
                results.setdefault(label, {})[run.system] = combine(repeats)
    with open(os.path.join(opts.out, "results.json"), "w") as f:
        json.dump({"mode": mode, "seed": seed, "repeat": opts.repeat, "results": results}, f, indent=2)
    report = table(results)
    with open(os.path.join(opts.out, "results.md"), "w") as f:
        f.write(report + "\n")
    say(report)


if __name__ == "__main__":
    sys.exit(main())
