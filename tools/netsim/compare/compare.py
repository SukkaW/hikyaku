#!/usr/bin/env python3
"""Compare hikyaku, satmux and TUIC v5 over netsim's emulated paths.

Run as root on Linux (see tools/netsim/Dockerfile). For each profile it starts
netsim, runs every server and the test website in the hk-server namespace and every
client in hk-client, and measures through each system's SOCKS5 port: cold start,
20 warm small HTTPS fetches, a 6-way burst, 10 MB downloads, and small fetches
during a download.
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
import time

PROFILES = {
    "10ms": ("10ms", 0, 500),
    "50ms": ("50ms", 0, 500),
    "200ms": ("200ms", 5, 100),
    "620ms": ("620ms", 10, 5),
}
SERVER = "10.200.0.2"
SMALL = "https://localhost:9443/"
BIG = "http://localhost:9001/big?n=10000000"
ENV = {k: v for k, v in os.environ.items() if "proxy" not in k.lower()}
HERE = os.path.dirname(os.path.abspath(__file__))

procs = []


def say(msg):
    print(msg, flush=True)


def spawn(args, name, ns=None, env=None):
    log = open(os.path.join(opts.out, name + ".log"), "w")
    cmd = (["ip", "netns", "exec", ns] if ns else []) + args
    p = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env={**ENV, **(env or {})},
                         start_new_session=True)
    procs.append(p)
    return p


def stop(p):
    if p.poll() is None:
        os.killpg(p.pid, signal.SIGTERM)
        try:
            p.wait(5)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)
            p.wait()
    procs.remove(p)


def wait_for(name, needle, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with open(os.path.join(opts.out, name + ".log")) as f:
            if needle in f.read():
                return
        time.sleep(0.1)
    raise RuntimeError(f"{name} did not print {needle!r} within {timeout}s")


def curl(socks, url, max_time):
    cmd = ["ip", "netns", "exec", "hk-client", "curl", "-sS", "-o", "/dev/null", "-w", "%{time_total} %{size_download}",
           "--max-time", str(max_time), "--cacert", os.path.join(opts.certs, "ca.pem"), "--socks5-hostname", socks, url]
    r = subprocess.run(cmd, capture_output=True, text=True, env=ENV, timeout=max_time + 15)
    parts = r.stdout.split()
    seconds = float(parts[0]) if parts else None
    size = int(float(parts[1])) if len(parts) > 1 else 0
    return r.returncode == 0, seconds, size


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


# Each system: start its server (hk-server), return a function that starts its client
# (hk-client), and the client's SOCKS5 address.
def hikyaku():
    psk = secrets.token_urlsafe(24)
    spawn([os.path.join(opts.hikyaku, "hikyaku-server"), "-listen", f"{SERVER}:8445", "-allow-private",
           "-psk", psk], "hikyaku-server", "hk-server")
    client = [os.path.join(opts.hikyaku, "hikyaku-client"), "-listen", "127.0.0.1:1080", "-server", f"{SERVER}:8445",
              "-psk", psk, "-carriers", "4"]
    return lambda: spawn(client, "hikyaku-client", "hk-client"), "127.0.0.1:1080"


def satmux():
    psk = secrets.token_hex(32)
    spawn(["python3", opts.satmux, "server", "--host", SERVER, "--port", "8444"], "satmux-server", "hk-server",
          {"SATMUX_PSK": psk})
    client = ["python3", opts.satmux, "client", "--host", SERVER, "--port", "8444", "--carriers", "12",
              "--socks-port", "1081", "--http-port", "18080", "--fast-port", "18081",
              "--gh-sock", os.path.join(opts.out, "satmux-gh.sock")]
    return lambda: spawn(client, "satmux-client", "hk-client", {"SATMUX_PSK": psk}), "127.0.0.1:1081"


def tuic():
    uuid, password = "11111111-2222-3333-4444-555555555555", secrets.token_urlsafe(24)
    server_toml, client_toml = os.path.join(opts.out, "tuic-server.toml"), os.path.join(opts.out, "tuic-client.toml")
    with open(server_toml, "w") as f:
        f.write(f'log_level = "info"\nserver = "{SERVER}:7444"\n[users]\n"{uuid}" = "{password}"\n'
                f'[tls]\ncertificate = "{opts.certs}/leaf.pem"\nprivate_key = "{opts.certs}/leaf.key"\n'
                f'alpn = ["h3"]\nhostname = "localhost"\n[experimental]\ndrop_loopback = false\n')
    with open(client_toml, "w") as f:
        f.write(f'log_level = "info"\n[relay]\nserver = "localhost:7444"\nip = "{SERVER}"\nuuid = "{uuid}"\n'
                f'password = "{password}"\ncertificates = ["{opts.certs}/ca.pem"]\nalpn = ["h3"]\n'
                f'congestion_control = "bbr"\n[local]\nserver = "127.0.0.1:1082"\n')
    spawn([os.path.join(opts.tuic, "tuic-server"), "-c", server_toml], "tuic-server", "hk-server")
    client = [os.path.join(opts.tuic, "tuic-client"), "-c", client_toml]
    return lambda: spawn(client, "tuic-client", "hk-client"), "127.0.0.1:1082"


SYSTEMS = {"hikyaku": hikyaku, "satmux": satmux, "tuic": tuic}


def measure(start_client, socks):
    result = {}
    t0 = time.time()
    start_client()
    while True:
        ok, _, _ = curl(socks, SMALL, 20)
        if ok:
            result["cold_s"] = round(time.time() - t0, 2)
            break
        if time.time() - t0 > 60:
            result["cold_s"] = None
            break
        time.sleep(0.1)

    def burst():
        with concurrent.futures.ThreadPoolExecutor(6) as ex:
            return list(ex.map(lambda _: curl(socks, SMALL, 30), range(6)))

    burst()  # let pools that grow on demand reach their working size
    time.sleep(1)

    warm = [curl(socks, SMALL, 30) for _ in range(20)]
    times = sorted(s for ok, s, _ in warm if ok)
    result["warm_median_s"] = round(statistics.median(times), 3) if times else None
    result["warm_p90_s"] = round(times[max(0, int(len(times) * 0.9) - 1)], 3) if times else None
    result["warm_failures"] = sum(1 for ok, _, _ in warm if not ok)

    b = burst()
    result["burst_max_s"] = round(max(s for ok, s, _ in b if ok), 3) if all(ok for ok, _, _ in b) else None
    result["burst_failures"] = sum(1 for ok, _, _ in b if not ok)

    rates = []
    for _ in range(3):
        _, seconds, size = curl(socks, BIG, 60)
        rates.append(size * 8 / seconds / 1e6 if seconds else 0.0)
    result["bulk_mbps"] = round(statistics.median(rates), 2)
    result["bulk_runs_mbps"] = [round(r, 2) for r in rates]

    background = subprocess.Popen(
        ["ip", "netns", "exec", "hk-client", "curl", "-sS", "-o", "/dev/null", "--max-time", "60",
         "--socks5-hostname", socks, BIG], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=ENV,
        start_new_session=True)
    time.sleep(2)
    loaded = [curl(socks, SMALL, 30) for _ in range(3)]
    os.killpg(background.pid, signal.SIGTERM)
    background.wait()
    loaded_times = [s for ok, s, _ in loaded if ok]
    result["loaded_median_s"] = round(statistics.median(loaded_times), 3) if loaded_times else None
    result["loaded_failures"] = sum(1 for ok, _, _ in loaded if not ok)
    return result


def table(results):
    head = ("| Profile | System | Cold start | Warm median | Warm p90 | 6-way burst | 10 MB download "
            "| Small fetch during download | Failures |")
    lines = [head, "|" + "---|" * 9]
    fmt = lambda v, unit="s": "fail" if v is None else f"{v} {unit}"
    for profile, systems in results.items():
        for system, r in systems.items():
            failures = r["warm_failures"] + r["burst_failures"] + r["loaded_failures"]
            lines.append(f"| {profile} | {system} | {fmt(r['cold_s'])} | {fmt(r['warm_median_s'])} "
                         f"| {fmt(r['warm_p90_s'])} | {fmt(r['burst_max_s'])} | {r['bulk_mbps']} Mbit/s "
                         f"| {fmt(r['loaded_median_s'])} | {failures} |")
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
    opts = ap.parse_args()
    os.makedirs(opts.out, exist_ok=True)
    certs()

    results = {}
    try:
        for name in opts.profiles.split(","):
            rtt, loss, rate = PROFILES[name]
            label = f"{rtt}, {loss}% loss, {rate} Mbit/s"
            sim = spawn([opts.netsim, "-rtt", rtt, "-loss", str(loss), "-rate", str(rate)], f"netsim-{name}")
            wait_for(f"netsim-{name}", "ready", 10)
            origin = spawn(["python3", os.path.join(HERE, "origin.py"), os.path.join(opts.certs, "leaf.pem"),
                            os.path.join(opts.certs, "leaf.key")], "origin", "hk-server")
            wait_for("origin", "origin ready", 10)
            for system in opts.systems.split(","):
                say(f"== {label}: {system}")
                started = len(procs)
                start_client, socks = SYSTEMS[system]()
                time.sleep(1)
                results.setdefault(label, {})[system] = measure(start_client, socks)
                say(f"   {results[label][system]}")
                for p in procs[started:][::-1]:
                    stop(p)
                time.sleep(1)
            stop(origin)
            stop(sim)
            with open(os.path.join(opts.out, "results.json"), "w") as f:
                json.dump(results, f, indent=2)
    finally:
        for p in procs[::-1]:
            stop(p)
    report = table(results)
    with open(os.path.join(opts.out, "results.md"), "w") as f:
        f.write(report + "\n")
    say(report)


if __name__ == "__main__":
    sys.exit(main())
