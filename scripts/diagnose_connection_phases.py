"""MeetMind AI — Deepgram Connection Phases Diagnostic (Session D2b/D2d).

Instruments:
1. DNS Resolution (IPv4 vs IPv6 vs AF_UNSPEC)
2. TCP Connect (forced IPv4 vs IPv6 vs Happy Eyeballs)
3. TLS 1.3 Handshake
4. WebSocket HTTP 101 Upgrade
5. Reconnect time (drop to first chunk accepted)

Compares:
- api.in.deepgram.com (India endpoint)
- api.deepgram.com (Global endpoint)

Security Invariants:
- Never prints or logs DEEPGRAM_API_KEY.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import socket
import ssl
import statistics
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

PORT = 443


def compute_percentiles(vals: list[float]) -> dict[str, str]:
    if not vals:
        return {"p50": "0.000", "p95": "0.000", "max": "0.000", "mean": "0.000"}
    s = sorted(vals)
    n = len(s)
    return {
        "p50": f"{s[int(n * 0.50)]:.3f}",
        "p95": f"{s[min(n - 1, int(n * 0.95))]:.3f}",
        "max": f"{s[-1]:.3f}",
        "mean": f"{statistics.mean(s):.3f}",
    }


def test_dns_resolution(host: str, samples: int = 20) -> dict[str, Any]:
    print("=" * 80)
    print(f" 1. DNS RESOLUTION PHASES — {host}")
    print("=" * 80)
    unspec_times: list[float] = []
    v4_times: list[float] = []
    v6_times: list[float] = []

    v4_addrs: list[str] = []
    v6_addrs: list[str] = []

    for _ in range(samples):
        # AF_UNSPEC
        t0 = time.perf_counter()
        try:
            res_unspec = socket.getaddrinfo(
                host, PORT, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM
            )
            unspec_times.append((time.perf_counter() - t0) * 1000.0)
            for item in res_unspec:
                addr = item[4][0]
                if item[0] == socket.AF_INET and addr not in v4_addrs:
                    v4_addrs.append(addr)
                elif item[0] == socket.AF_INET6 and addr not in v6_addrs:
                    v6_addrs.append(addr)
        except Exception:
            pass

        # Forced AF_INET (IPv4)
        t0 = time.perf_counter()
        try:
            socket.getaddrinfo(host, PORT, family=socket.AF_INET, type=socket.SOCK_STREAM)
            v4_times.append((time.perf_counter() - t0) * 1000.0)
        except Exception:
            pass

        # Forced AF_INET6 (IPv6)
        t0 = time.perf_counter()
        try:
            socket.getaddrinfo(host, PORT, family=socket.AF_INET6, type=socket.SOCK_STREAM)
            v6_times.append((time.perf_counter() - t0) * 1000.0)
        except Exception:
            pass

    u_p = compute_percentiles(unspec_times)
    v4_p = compute_percentiles(v4_times)
    v6_p = compute_percentiles(v6_times)

    print(f"Host:                     {host}")
    print(f"Resolved IPv4 Addresses: {v4_addrs}")
    print(f"Resolved IPv6 Addresses: {v6_addrs}")
    print(f"DNS AF_UNSPEC (default):  P50={u_p['p50']}ms | P95={u_p['p95']}ms | Mean={u_p['mean']}ms")
    print(f"DNS Forced AF_INET (v4):  P50={v4_p['p50']}ms | P95={v4_p['p95']}ms | Mean={v4_p['mean']}ms")
    print(f"DNS Forced AF_INET6 (v6): P50={v6_p['p50']}ms | P95={v6_p['p95']}ms | Mean={v6_p['mean']}ms")
    print("-" * 80)
    return {"v4_addrs": v4_addrs, "v6_addrs": v6_addrs}


def test_tcp_connect_modes(host: str, v4_addrs: list[str], v6_addrs: list[str], samples: int = 20) -> None:
    print("=" * 80)
    print(f" 2. TCP CONNECT LATENCY BY PROTOCOL FAMILY — {host}")
    print("=" * 80)

    # 1. Default hostname connect
    default_times: list[float] = []
    for _ in range(samples):
        t0 = time.perf_counter()
        try:
            with socket.create_connection((host, PORT), timeout=10.0):
                default_times.append((time.perf_counter() - t0) * 1000.0)
        except Exception as e:
            default_times.append(9999.0)
            print(f"  Default connect error: {e}")

    # 2. Forced IPv4 connect
    v4_times: list[float] = []
    target_v4 = v4_addrs[0] if v4_addrs else host
    for _ in range(samples):
        t0 = time.perf_counter()
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(10.0)
                s.connect((target_v4, PORT))
                v4_times.append((time.perf_counter() - t0) * 1000.0)
        except Exception as e:
            v4_times.append(9999.0)
            print(f"  Forced IPv4 connect error: {e}")

    # 3. Forced IPv6 connect (if any)
    v6_times: list[float] = []
    if v6_addrs:
        target_v6 = v6_addrs[0]
        for _ in range(samples):
            t0 = time.perf_counter()
            try:
                with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
                    s.settimeout(5.0)
                    s.connect((target_v6, PORT))
                    v6_times.append((time.perf_counter() - t0) * 1000.0)
            except Exception as e:
                dur = (time.perf_counter() - t0) * 1000.0
                v6_times.append(dur)
                print(f"  Forced IPv6 connect to {target_v6} timed out after {dur:.1f}ms: {e}")

    def_p = compute_percentiles(default_times)
    v4_p = compute_percentiles(v4_times)
    print(f"Default Hostname Connect: P50={def_p['p50']}ms | P95={def_p['p95']}ms | Mean={def_p['mean']}ms")
    print(f"Forced AF_INET (IPv4):    P50={v4_p['p50']}ms | P95={v4_p['p95']}ms | Mean={v4_p['mean']}ms")
    if v6_times:
        v6_p = compute_percentiles(v6_times)
        print(f"Forced AF_INET6 (IPv6):   P50={v6_p['p50']}ms | P95={v6_p['p95']}ms | Mean={v6_p['mean']}ms")
    print("-" * 80)


async def test_asyncio_happy_eyeballs(host: str, samples: int = 20) -> None:
    print("=" * 80)
    print(f" 3. ASYNCIO HAPPY EYEBALLS (RFC 6555 / RFC 8305) — {host}")
    print("=" * 80)

    # Standard asyncio.open_connection
    he_times: list[float] = []
    for _ in range(samples):
        t0 = time.perf_counter()
        try:
            r, w = await asyncio.open_connection(host, PORT, happy_eyeballs_delay=0.25)
            he_times.append((time.perf_counter() - t0) * 1000.0)
            w.close()
            await w.wait_closed()
        except Exception as e:
            print(f"  Happy eyeballs connect error: {e}")

    # Asyncio forced IPv4
    v4_async_times: list[float] = []
    for _ in range(samples):
        t0 = time.perf_counter()
        try:
            r, w = await asyncio.open_connection(host, PORT, family=socket.AF_INET)
            v4_async_times.append((time.perf_counter() - t0) * 1000.0)
            w.close()
            await w.wait_closed()
        except Exception as e:
            print(f"  Asyncio IPv4 connect error: {e}")

    he_p = compute_percentiles(he_times)
    v4_p = compute_percentiles(v4_async_times)
    print(f"Asyncio Happy Eyeballs: P50={he_p['p50']}ms | P95={he_p['p95']}ms | Mean={he_p['mean']}ms")
    print(f"Asyncio Forced AF_INET: P50={v4_p['p50']}ms | P95={v4_p['p95']}ms | Mean={v4_p['mean']}ms")
    print("-" * 80)


async def test_complete_connection_phases(host: str, api_key: str, samples: int = 20) -> dict[str, dict[str, str]]:
    print("=" * 80)
    print(f" 4. DETAILED BREAKDOWN PER PHASE (DNS, TCP, TLS, WS CONNECT) — {host}")
    print("=" * 80)
    import websockets

    dns_times: list[float] = []
    tcp_times: list[float] = []
    tls_times: list[float] = []
    ws_times: list[float] = []
    total_times: list[float] = []

    ssl_context = ssl.create_default_context()

    for idx in range(samples):
        t_all0 = time.perf_counter()

        # Phase 1: DNS
        t0 = time.perf_counter()
        addr_info = socket.getaddrinfo(host, PORT, family=socket.AF_INET, type=socket.SOCK_STREAM)
        ip = addr_info[0][4][0]
        t_dns = (time.perf_counter() - t0) * 1000.0
        dns_times.append(t_dns)

        # Phase 2: TCP Connect (forced IPv4 via asyncio)
        t0 = time.perf_counter()
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, PORT, family=socket.AF_INET),
            timeout=15.0,
        )
        t_tcp = (time.perf_counter() - t0) * 1000.0
        tcp_times.append(t_tcp)

        # Phase 3: TLS 1.3 Handshake via asyncio
        t0 = time.perf_counter()
        loop = asyncio.get_running_loop()
        transport = writer.transport
        protocol = transport.get_protocol()
        new_transport = await asyncio.wait_for(
            loop.start_tls(
                transport, protocol, ssl_context, server_side=False, server_hostname=host
            ),
            timeout=15.0,
        )
        t_tls = (time.perf_counter() - t0) * 1000.0
        tls_times.append(t_tls)
        new_transport.close()

        # Phase 4: Full WebSocket connect (over websockets library with IPv4)
        t0 = time.perf_counter()
        headers = {"Authorization": f"Token {api_key}"}
        url = f"wss://{host}/v1/listen?model=nova-3&encoding=linear16&sample_rate=16000"
        async with websockets.connect(
            url,
            additional_headers=headers,
            family=socket.AF_INET,
            open_timeout=15.0,
        ) as ws:
            t_ws = (time.perf_counter() - t0) * 1000.0
            ws_times.append(t_ws)
            with contextlib.suppress(Exception):
                await ws.send('{"type": "CloseStream"}')

        t_total = (time.perf_counter() - t_all0) * 1000.0
        total_times.append(t_total)

        print(
            f"  [{host} Sample {idx + 1:2d}/{samples}] DNS: {t_dns:6.2f}ms | TCP: {t_tcp:6.2f}ms | "
            f"TLS: {t_tls:6.2f}ms | WS Upgrade: {t_ws:6.2f}ms | Total: {t_total:6.2f}ms"
        )
        await asyncio.sleep(0.2)

    print("-" * 80)
    print(f"PHASE SUMMARY (PERCENTILES) — {host} (N={samples}):")
    summary: dict[str, dict[str, str]] = {}
    for name, data in [
        ("DNS (IPv4)", dns_times),
        ("TCP Connect (IPv4)", tcp_times),
        ("TLS 1.3 Handshake", tls_times),
        ("Full WS Connect", ws_times),
        ("Total Session Setup", total_times),
    ]:
        p = compute_percentiles(data)
        summary[name] = p
        print(
            f"  {name:22s}: P50={p['p50']:>7s}ms | P95={p['p95']:>7s}ms | "
            f"Max={p['max']:>7s}ms | Mean={p['mean']:>7s}ms"
        )
    print("=" * 80)
    return summary


async def run_diagnostics_for_host(host: str, api_key: str, samples: int = 20) -> dict[str, Any]:
    print("\n" + "#" * 80)
    print(f"# RUNNING CONNECTION PHASE DIAGNOSTICS FOR: {host} ({samples} SAMPLES)")
    print("#" * 80)
    addrs = test_dns_resolution(host, samples=samples)
    test_tcp_connect_modes(host, addrs["v4_addrs"], addrs["v6_addrs"], samples=samples)
    await test_asyncio_happy_eyeballs(host, samples=samples)
    summary = await test_complete_connection_phases(host, api_key, samples=samples)
    return {"host": host, "summary": summary}


async def main() -> None:
    parser = argparse.ArgumentParser(description="Deepgram Connection Phases Diagnostic")
    parser.add_argument("--samples", type=int, default=20, help="Number of samples (default: 20)")
    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="Specific host to test (e.g. api.in.deepgram.com or api.deepgram.com)",
    )
    args = parser.parse_args()

    api_key = os.environ.get("DEEPGRAM_API_KEY", "").strip()
    if not api_key:
        print("[ERROR] DEEPGRAM_API_KEY is not set.")
        return

    if args.host:
        hosts = [args.host]
    else:
        hosts = ["api.in.deepgram.com", "api.deepgram.com"]

    results = []
    for h in hosts:
        res = await run_diagnostics_for_host(h, api_key, samples=args.samples)
        results.append(res)

    if len(results) > 1:
        print("\n" + "=" * 80)
        print(" COMPARATIVE SUMMARY (P50 / P95):")
        print("=" * 80)
        print(f"{'Metric':<24} | {'api.in.deepgram.com':<24} | {'api.deepgram.com':<24}")
        print("-" * 80)
        s_in = results[0]["summary"]
        s_gl = results[1]["summary"]
        for metric in s_in:
            in_str = f"P50={s_in[metric]['p50']} P95={s_in[metric]['p95']}"
            gl_str = f"P50={s_gl[metric]['p50']} P95={s_gl[metric]['p95']}"
            print(f"{metric:<24} | {in_str:<24} | {gl_str:<24}")
        print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
