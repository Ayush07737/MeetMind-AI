"""MeetMind AI — Multi-Region Network & Endpoint Probe (Q-0e Section E1).

Tests DNS, TCP, TLS, WebSocket upgrade, ping RTT (P50/P95), and IP/ASN/geography
across all candidate ASR provider endpoints without transmitting audio or incurring costs.
Emits structured JSON output and formats a clean comparison table.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import re
import socket
import ssl
import subprocess
import time
import urllib.request
from dataclasses import asdict, dataclass, field
from pathlib import Path

import websockets

REPO_ROOT = Path(__file__).resolve().parent.parent

ENDPOINTS_TO_PROBE: list[dict[str, str]] = [
    {
        "provider": "deepgram",
        "region_label": "India (Mumbai)",
        "host": "api.in.deepgram.com",
        "ws_url": "wss://api.in.deepgram.com/v1/listen?model=nova-3",
    },
    {
        "provider": "deepgram",
        "region_label": "Global (US default)",
        "host": "api.deepgram.com",
        "ws_url": "wss://api.deepgram.com/v1/listen?model=nova-3",
    },
    {
        "provider": "deepgram",
        "region_label": "Europe (Frankfurt)",
        "host": "api.eu.deepgram.com",
        "ws_url": "wss://api.eu.deepgram.com/v1/listen?model=nova-3",
    },
    {
        "provider": "gladia",
        "region_label": "Europe (eu-west param)",
        "host": "api.gladia.io",
        "ws_url": "wss://api.gladia.io/v2/live?region=eu-west",
    },
    {
        "provider": "gladia",
        "region_label": "US West (us-west param)",
        "host": "api.gladia.io",
        "ws_url": "wss://api.gladia.io/v2/live?region=us-west",
    },
    {
        "provider": "assemblyai",
        "region_label": "Global (US East)",
        "host": "api.assemblyai.com",
        "ws_url": "wss://api.assemblyai.com/v2/realtime/ws?sample_rate=16000",
    },
    {
        "provider": "smallest",
        "region_label": "India (Mumbai)",
        "host": "waves-api.smallest.ai",
        "ws_url": "wss://waves-api.smallest.ai/waves/v1/stt/live?model=pulse",
    },
    {
        "provider": "smallest",
        "region_label": "Global / Anycast",
        "host": "api.smallest.ai",
        "ws_url": "wss://api.smallest.ai/waves/v1/stt/live?model=pulse",
    },
    {
        "provider": "soniox",
        "region_label": "Global (US)",
        "host": "stt-rt.soniox.com",
        "ws_url": "wss://stt-rt.soniox.com/transcribe-websocket",
    },
]


@dataclass
class ProbeResult:
    provider: str
    region_label: str
    host: str
    resolved_ips: list[str] = field(default_factory=list)
    cname: str = ""
    asn_org: str = "Unknown"
    geo_location: str = "Unknown"
    dns_time_ms: float = 0.0
    tcp_handshake_ms: float = 0.0
    tls_handshake_ms: float = 0.0
    ws_upgrade_ms: float = 0.0
    ping_p50_ms: float | None = None
    ping_p95_ms: float | None = None
    status: str = "OK"
    error: str | None = None


def resolve_dns_and_geo(host: str) -> tuple[list[str], str, str, str, float]:
    t0 = time.perf_counter()
    cname = host
    ips: list[str] = []
    with contextlib.suppress(Exception):
        cname, _, ips = socket.gethostbyname_ex(host)
    dns_ms = (time.perf_counter() - t0) * 1000.0

    asn_org = "Unknown"
    geo_loc = "Unknown"
    if ips:
        primary_ip = ips[0]
        try:
            req = urllib.request.Request(
                f"https://ipinfo.io/{primary_ip}/json",
                headers={"User-Agent": "curl/7.68.0"},
            )
            with urllib.request.urlopen(req, timeout=3.0) as resp:
                data = json.loads(resp.read().decode())
                asn_org = data.get("org", "Unknown")
                city = data.get("city", "")
                country = data.get("country", "")
                geo_loc = f"{city}, {country}".strip(", ")
        except Exception:
            pass

    return ips, cname, asn_org, geo_loc, dns_ms


def measure_tcp_tls(host: str, port: int = 443) -> tuple[float, float]:
    t0 = time.perf_counter()
    sock = socket.create_connection((host, port), timeout=5.0)
    tcp_ms = (time.perf_counter() - t0) * 1000.0

    ctx = ssl.create_default_context()
    t1 = time.perf_counter()
    s_tls = ctx.wrap_socket(sock, server_hostname=host)
    tls_ms = (time.perf_counter() - t1) * 1000.0
    s_tls.close()
    return tcp_ms, tls_ms


async def measure_ws_upgrade(ws_url: str) -> float:
    t0 = time.perf_counter()
    try:
        async with asyncio.timeout(6.0):
            try:
                async with websockets.connect(
                    ws_url,
                    open_timeout=5.0,
                    close_timeout=1.0,
                    additional_headers={"User-Agent": "MeetMind-RegionProbe/1.0"},
                ) as ws:
                    elapsed = (time.perf_counter() - t0) * 1000.0
                    await ws.close()
                    return elapsed
            except Exception:
                # Any response from server (401/403/400) indicates full handshake round-trip
                return (time.perf_counter() - t0) * 1000.0
    except TimeoutError:
        return (time.perf_counter() - t0) * 1000.0


def measure_ping_rtt(host: str, count: int = 20) -> tuple[float | None, float | None]:
    try:
        proc = subprocess.run(
            ["ping", "-n", str(count), "-w", "1000", host],
            capture_output=True,
            text=True,
            timeout=30.0,
        )
        rtts: list[float] = []
        for line in proc.stdout.splitlines():
            m = re.search(r"time[=<]([0-9]+)ms", line, re.IGNORECASE)
            if m:
                rtts.append(float(m.group(1)))
        if not rtts:
            return None, None
        rtts.sort()
        p50 = rtts[int(len(rtts) * 0.50)]
        p95 = rtts[int(len(rtts) * 0.95)] if len(rtts) > 1 else rtts[-1]
        return p50, p95
    except Exception:
        return None, None


async def probe_endpoint(spec: dict[str, str]) -> ProbeResult:
    host = spec["host"]
    res = ProbeResult(
        provider=spec["provider"],
        region_label=spec["region_label"],
        host=host,
    )
    try:
        ips, cname, asn_org, geo, dns_ms = resolve_dns_and_geo(host)
        res.resolved_ips = ips
        res.cname = cname
        res.asn_org = asn_org
        res.geo_location = geo
        res.dns_time_ms = round(dns_ms, 1)

        if not ips:
            res.status = "UNRESOLVED"
            res.error = "DNS resolution failed (host does not exist)"
            return res

        # Run synchronous network probes in threads to prevent event loop blocking
        tcp_ms, tls_ms = await asyncio.to_thread(measure_tcp_tls, host, 443)
        res.tcp_handshake_ms = round(tcp_ms, 1)
        res.tls_handshake_ms = round(tls_ms, 1)

        ws_ms = await measure_ws_upgrade(spec["ws_url"])
        res.ws_upgrade_ms = round(ws_ms, 1)

        p50, p95 = await asyncio.to_thread(measure_ping_rtt, host, 20)
        res.ping_p50_ms = p50
        res.ping_p95_ms = p95
    except Exception as exc:
        res.status = "PARTIAL" if res.resolved_ips else "ERROR"
        res.error = f"{type(exc).__name__}: {exc}"

    return res


async def run_all_probes() -> list[ProbeResult]:
    tasks = [probe_endpoint(spec) for spec in ENDPOINTS_TO_PROBE]
    return await asyncio.gather(*tasks)


def main():
    parser = argparse.ArgumentParser(description="Multi-Region Network & Endpoint Probe")
    parser.add_argument(
        "--json-out",
        type=Path,
        default=REPO_ROOT / "logs" / "region_probe_results.json",
        help="Path to save probe results as JSON",
    )
    args = parser.parse_args()

    print("Running multi-region endpoint probes (20 ping samples, P50/P95, no audio)...")
    results = asyncio.run(run_all_probes())

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.json_out, "w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in results], f, indent=2)

    print("\n" + "=" * 115)
    print("ASR PROVIDER MULTI-REGION NETWORK PROBE MATRIX (Q-0e Section E1, 20 Samples)")
    print("=" * 115)
    header = (
        f"{'PROVIDER':<12} {'REGION':<22} {'HOST':<24} "
        f"{'GEO':<15} {'TCP_MS':>7} {'TLS_MS':>7} {'WS_UPG':>8} {'PING50':>8} {'PING95':>8}"
    )
    print(header)
    print("-" * 115)
    for r in results:
        p50_str = f"{r.ping_p50_ms:.0f}ms" if r.ping_p50_ms is not None else "N/A"
        p95_str = f"{r.ping_p95_ms:.0f}ms" if r.ping_p95_ms is not None else "N/A"
        print(
            f"{r.provider:<12} {r.region_label:<22} {r.host:<24} "
            f"{r.geo_location:<15} {r.tcp_handshake_ms:>6.1f}m {r.tls_handshake_ms:>6.1f}m "
            f"{r.ws_upgrade_ms:>7.1f}m {p50_str:>8} {p95_str:>8}"
        )
    print("=" * 115)
    print(f"Results saved to: {args.json_out}")


if __name__ == "__main__":
    main()
