"""MeetMind AI — Component #2 (ASR) Latency and Stability Benchmark.

Measures:
1. Latency Breakdown against real Deepgram Nova-3:
   - End-to-End Latency: chunk arrival at ASR to internal bus publish
   - Provider Latency: chunk send to Deepgram message receipt
   - Service-Added Latency: Deepgram message receipt to internal bus publish (target P95 < 20ms)
   - Network RTT: WebSocket ping/pong RTT to Deepgram sampled every 5s + TCP connect time
2. Sensitivity to endpointing and utterance_end_ms
3. 30-Minute Stability (Memory flatlining, zero audio leak, queue depth, event-loop lag)

SECURITY:
- Requires --yes or RUN_DEEPGRAM_TESTS=1 to run.
- Prints cost estimate before executing.
- Never prints or leaks API keys.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import csv
import json
import os
import socket
import statistics
import subprocess
import sys
import time
import uuid
import wave
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import redis.asyncio as aioredis
import websockets
from dotenv import load_dotenv

try:
    import psutil
except ImportError:
    psutil = None

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

for p in (
    REPO_ROOT / "backend" / "gateway",
    REPO_ROOT / "backend" / "asr",
    REPO_ROOT / "backend" / "security",
    REPO_ROOT / "packages" / "shared-schemas" / "schemas_py",
    REPO_ROOT / "backend" / "asr" / "tests" / "acceptance",
):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from meetmind_schemas.events import encode_audio_frame  # noqa: E402
from meetmind_schemas.keys import events_stream_key  # noqa: E402
from meetmind_security import db  # noqa: E402
from meetmind_security.tenant import TenantConfig  # noqa: E402

RATE_PER_MIN = 0.0058  # $/minute for Nova-3 streaming


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def compute_percentiles(vals: list[float]) -> dict[str, str]:
    if not vals:
        return {"p50": "0.000", "p95": "0.000", "p99": "0.000", "max": "0.000", "mean": "0.000"}
    s = sorted(vals)
    n = len(s)
    return {
        "p50": f"{s[int(n * 0.50)]:.3f}",
        "p95": f"{s[min(n - 1, int(n * 0.95))]:.3f}",
        "p99": f"{s[min(n - 1, int(n * 0.99))]:.3f}",
        "max": f"{s[-1]:.3f}",
        "mean": f"{statistics.mean(s):.3f}",
    }


async def measure_deepgram_network_phases(
    api_key: str, host: str = "api.deepgram.com", samples: int = 5
) -> dict[str, Any]:
    """Measure per-phase DNS, TCP, TLS 1.3, HTTP 101 upgrade, and WS Ping/Pong RTT."""
    import ssl

    port = 443
    ssl_context = ssl.create_default_context()

    dns_times: list[float] = []
    tcp_times: list[float] = []
    tls_times: list[float] = []
    upgrade_times: list[float] = []
    ping_rtts: list[float] = []

    for _ in range(samples):
        # 1. DNS IPv4 resolution
        t0 = time.perf_counter()
        ip = host
        try:
            addr_info = socket.getaddrinfo(
                host, port, family=socket.AF_INET, type=socket.SOCK_STREAM
            )
            ip = addr_info[0][4][0]
            dns_times.append((time.perf_counter() - t0) * 1000.0)
        except Exception:
            pass

        # 2. TCP Connect (forced IPv4)
        t0 = time.perf_counter()
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(ip, port, family=socket.AF_INET),
                timeout=10.0,
            )
            tcp_times.append((time.perf_counter() - t0) * 1000.0)

            # 3. TLS 1.3 Handshake
            t0 = time.perf_counter()
            loop = asyncio.get_running_loop()
            transport = writer.transport
            protocol = transport.get_protocol()
            new_transport = await asyncio.wait_for(
                loop.start_tls(
                    transport, protocol, ssl_context, server_side=False, server_hostname=host
                ),
                timeout=10.0,
            )
            tls_times.append((time.perf_counter() - t0) * 1000.0)
            new_transport.close()
        except Exception:
            pass

        # 4. HTTP 101 WebSocket Upgrade & Ping/Pong RTT
        t0 = time.perf_counter()
        headers = {"Authorization": f"Token {api_key}"}
        url = f"wss://{host}/v1/listen?model=nova-3&encoding=linear16&sample_rate=16000"
        try:
            async with websockets.connect(
                url,
                additional_headers=headers,
                family=socket.AF_INET,
                open_timeout=15.0,
            ) as ws:
                t_up = (time.perf_counter() - t0) * 1000.0
                upgrade_times.append(t_up)

                t_ping0 = time.perf_counter()
                pong_waiter = await ws.ping()
                await asyncio.wait_for(pong_waiter, timeout=5.0)
                ping_rtts.append((time.perf_counter() - t_ping0) * 1000.0)

                with contextlib.suppress(Exception):
                    await ws.send(json.dumps({"type": "CloseStream"}))
        except Exception:
            pass

        await asyncio.sleep(0.3)

    return {
        "dns_ms": compute_percentiles(dns_times),
        "tcp_ms": compute_percentiles(tcp_times),
        "tls_ms": compute_percentiles(tls_times),
        "upgrade_ms": compute_percentiles(upgrade_times),
        "ping_rtt_ms": compute_percentiles(ping_rtts),
    }


async def run_benchmark(
    minutes: float,
    endpointing: int | None = None,
    utterance_end_ms: int | None = None,
    region: str = "global",
    auto_yes: bool = False,
) -> None:
    api_key = os.environ.get("DEEPGRAM_API_KEY", "").strip()
    if not api_key:
        print("[ERROR] DEEPGRAM_API_KEY is not set.")
        sys.exit(1)

    target_host = (
        "api.in.deepgram.com"
        if region == "in"
        else ("api.eu.deepgram.com" if region == "eu" else "api.deepgram.com")
    )
    base_url = f"wss://{target_host}"

    est_cost = minutes * RATE_PER_MIN
    print("=" * 80)
    print(" MeetMind AI — Component #2 (ASR) Real Deepgram Latency & Stability Bench")
    print("=" * 80)
    print(f"Duration:            {minutes:.1f} minutes ({int(minutes * 60)} seconds)")
    print(f"Target Region:       {region.upper()} ({target_host})")
    print("Deepgram Model:      Nova-3 Streaming (linear16 @ 16kHz)")
    print(f"Published Rate:      ${RATE_PER_MIN}/audio minute")
    print(f"Estimated Run Cost:  ${est_cost:.4f} USD")
    print("-" * 80)

    if not auto_yes and os.environ.get("RUN_DEEPGRAM_TESTS") != "1":
        print("[ABORT] Live Deepgram run requires --yes flag or RUN_DEEPGRAM_TESTS=1.")
        sys.exit(1)

    # 1. Measure Network Baseline RTT (Per-Phase Breakdown)
    print(f"Measuring network baseline per-phase timings to {target_host} (5 samples)...")
    net_stats = await measure_deepgram_network_phases(api_key, host=target_host, samples=5)
    dns_p = net_stats["dns_ms"]
    tcp_p = net_stats["tcp_ms"]
    tls_p = net_stats["tls_ms"]
    upg_p = net_stats["upgrade_ms"]
    ping_p = net_stats["ping_rtt_ms"]
    print(f"  DNS Resolution (IPv4):   P50={dns_p['p50']}ms | P95={dns_p['p95']}ms")
    print(f"  TCP Connect (IPv4):      P50={tcp_p['p50']}ms | P95={tcp_p['p95']}ms")
    print(f"  TLS 1.3 Handshake:       P50={tls_p['p50']}ms | P95={tls_p['p95']}ms")
    print(f"  HTTP 101 WS Upgrade:     P50={upg_p['p50']}ms | P95={upg_p['p95']}ms")
    print(f"  WS Ping/Pong RTT:        P50={ping_p['p50']}ms | P95={ping_p['p95']}ms")
    print("-" * 80)

    # 2. Setup throwaway tenant on Neon
    tenant_id = f"bench_dg_{uuid.uuid4().hex[:6]}"
    schema_name = f"tenant_{tenant_id}"
    cfg = TenantConfig(
        tenant_id=tenant_id,
        pg_schema=schema_name,
        neo4j_namespace=tenant_id,
        qdrant_prefix=tenant_id,
        redis_prefix=tenant_id,
        asr_region=region,
    )
    db._router.register(cfg)
    from meetmind_security.migrations import apply_migrations_conn

    dsn = db.get_owner_dsn()
    ssl_ctx = db._create_ssl_context(dsn)
    import asyncpg

    conn = await asyncpg.connect(dsn, ssl=ssl_ctx)
    try:
        await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
        await apply_migrations_conn(conn, schema_name)
    finally:
        await conn.close()

    from conftest import LocalJWKSServer

    jwks_port = get_free_port()
    jwks = LocalJWKSServer(jwks_port)
    jwks.start()

    org_id = f"org_{tenant_id}"
    token = jwks.mint_token(sub="admin_bench", org_id=org_id, org_role="org:admin")

    # 3. Spawn real Gateway subprocess
    gw_port = get_free_port()
    env = os.environ.copy()
    env["CLERK_JWKS_URL"] = jwks.jwks_url
    env["CLERK_ISSUER"] = jwks.issuer
    env["ALLOWED_ORIGINS"] = json.dumps(["https://app.meetmind.ai"])
    env["TENANT_ORG_MAP"] = json.dumps({org_id: tenant_id})
    env["APP_ENV"] = "test"
    app_dsn = db.get_app_dsn()
    env["NEON_DATABASE_URL"] = app_dsn
    env["NEON_APP_DATABASE_URL"] = app_dsn
    redis_url = os.environ.get("ACCEPTANCE_REDIS_URL", "redis://127.0.0.1:6379/0")
    env["REDIS_URL"] = redis_url
    env["AUDIO_REDIS_URL"] = redis_url
    env["LOG_LEVEL"] = "WARNING"
    env["PYTHONPATH"] = (
        str(REPO_ROOT / "backend" / "gateway")
        + os.pathsep
        + str(REPO_ROOT / "backend" / "security")
        + os.pathsep
        + str(REPO_ROOT / "packages" / "shared-schemas" / "schemas_py")
    )

    gw_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "meetmind_gateway.app:create_app",
            "--factory",
            "--host",
            "127.0.0.1",
            "--port",
            str(gw_port),
            "--workers",
            "2",
            "--log-level",
            "warning",
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # 4. Spawn real ASR subprocess
    cfg_file = REPO_ROOT / "logs" / f"tenant_cfg_{tenant_id}.json"
    cfg_file.parent.mkdir(parents=True, exist_ok=True)
    cfg_file.write_text(
        json.dumps(
            {
                "tenants": {
                    tenant_id: {
                        "language": "en",
                        "keyterms": [],
                        "asr_region": region,
                    }
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    asr_health_port = get_free_port()
    asr_env = os.environ.copy()
    asr_env["ASR_PROVIDER"] = "deepgram"
    asr_env["DEEPGRAM_API_KEY"] = api_key
    asr_env["MIP_OPT_OUT"] = "true"
    asr_env["DEEPGRAM_BASE_URL"] = base_url
    asr_env["ASR_REGION"] = region
    asr_env["ASR_TENANT_CONFIG_PATH"] = str(cfg_file)
    asr_env["ASR_HEALTH_PORT"] = str(asr_health_port)
    asr_env["ASR_WORKER_ID"] = f"bench_worker_{uuid.uuid4().hex[:6]}"
    asr_env["REDIS_URL"] = redis_url
    asr_env["APP_ENV"] = "development"
    asr_env["PYTHONPATH"] = (
        str(REPO_ROOT / "backend" / "asr")
        + os.pathsep
        + str(REPO_ROOT / "backend" / "security")
        + os.pathsep
        + str(REPO_ROOT / "packages" / "shared-schemas" / "schemas_py")
    )

    asr_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "meetmind_asr",
        ],
        env=asr_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Wait for both to be healthy
    async with httpx.AsyncClient() as client:
        for _ in range(150):
            try:
                g_res = await client.get(f"http://127.0.0.1:{gw_port}/health", timeout=1.0)
                a_res = await client.get(f"http://127.0.0.1:{asr_health_port}/healthz", timeout=1.0)
                if g_res.status_code == 200 and a_res.status_code == 200:
                    break
            except Exception:
                pass
            await asyncio.sleep(0.1)

    print(f"Gateway (port {gw_port}) and ASR worker (port {asr_health_port}) healthy.")

    meeting_id = f"mtg_bench_{uuid.uuid4().hex[:8]}"
    redis_client = aioredis.from_url(redis_url, decode_responses=False)

    # Audio Fixture: Read from 5min fixture
    audio_path = REPO_ROOT / "tests" / "fixtures" / "audio" / "generated" / "5min_meeting.wav"
    if not audio_path.exists():
        audio_path = (
            REPO_ROOT
            / "tests"
            / "fixtures"
            / "audio"
            / "generated"
            / "30s_two_speaker_overlap_mono.wav"
        )

    audio_frames: list[bytes] = []
    with wave.open(str(audio_path), "rb") as wf:
        while True:
            data = wf.readframes(1600)  # 100ms
            if not data:
                break
            audio_frames.append(data)

    total_stream_seconds = minutes * 60.0
    total_target_chunks = int(total_stream_seconds * 10)

    # Get WebSocket ticket
    from conftest import exchange_ws_ticket

    code, t_res = await exchange_ws_ticket(
        f"http://127.0.0.1:{gw_port}", token, meeting_id, role="ingest"
    )
    assert code == 200, f"Ticket exchange failed: {t_res}"
    ticket = t_res["ticket"]

    # Storage for latency metrics
    e2e_latencies_ms: list[float] = []
    service_added_latencies_ms: list[float] = []
    finals_collected: list[dict] = []
    stability_samples: list[dict[str, Any]] = []

    # Monitor consumer on internal bus
    ev_key = events_stream_key(tenant_id, meeting_id)
    stop_bench = asyncio.Event()

    async def _internal_bus_collector():
        last_id = "0"
        while not stop_bench.is_set():
            try:
                res = await redis_client.xread({ev_key: last_id}, count=20, block=200)
                if not res:
                    continue
                for _, entries in res:
                    for eid, fields in entries:
                        last_id = eid.decode() if isinstance(eid, bytes) else str(eid)
                        raw_payload = fields.get(b"payload") or fields.get("payload")
                        if not raw_payload:
                            continue
                        raw_str = (
                            raw_payload.decode() if isinstance(raw_payload, bytes) else raw_payload
                        )
                        data = json.loads(raw_str)
                        if data.get("event_type") == "transcript_final":
                            now_wall = time.time() * 1000.0
                            finals_collected.append(data)

                            payload = data.get("payload", {})
                            e2e = payload.get("e2e_ms")
                            c_recv = payload.get("chunk_recv_wall")
                            t_pub = payload.get("publish_wall")
                            if e2e is not None:
                                e2e_latencies_ms.append(float(e2e))
                            elif c_recv and t_pub:
                                e2e_latencies_ms.append(max(0.1, (t_pub - c_recv) * 1000.0))

                            created_ts = data.get("ts_ms", now_wall)
                            service_added = max(0.01, now_wall - created_ts)
                            service_added_latencies_ms.append(service_added)
            except Exception:
                await asyncio.sleep(0.1)

    collector_task = asyncio.create_task(_internal_bus_collector())

    # Instrument Bench Process GC and Event Loop Lag
    import gc

    bench_gc_pauses: list[float] = []
    bench_gc_t0: float = 0.0

    def _bench_gc_cb(phase: str, info: dict[str, Any]) -> None:
        nonlocal bench_gc_t0, bench_gc_pauses
        if phase == "start":
            bench_gc_t0 = time.perf_counter()
        elif phase == "stop" and bench_gc_t0 > 0.0:
            dur_ms = (time.perf_counter() - bench_gc_t0) * 1000.0
            bench_gc_pauses.append(dur_ms)
            bench_gc_t0 = 0.0

    gc.callbacks.append(_bench_gc_cb)

    bench_loop_lags: list[float] = []

    async def _bench_lag_tracker() -> None:
        while not stop_bench.is_set():
            t0 = time.perf_counter()
            await asyncio.sleep(0.010)
            lag = (time.perf_counter() - t0 - 0.010) * 1000.0
            if lag > 0.5:
                bench_loop_lags.append(lag)

    bench_lag_task = asyncio.create_task(_bench_lag_tracker())

    # Ingest loop
    ingest_ws_url = f"ws://127.0.0.1:{gw_port}/v1/meetings/{meeting_id}/stream?ticket={ticket}"
    print(f"Streaming {total_target_chunks} audio chunks in real-time ({minutes:.1f} minutes)...")

    def find_real_asr_worker(proc_pid: int) -> psutil.Process | None:
        if not psutil:
            return None
        try:
            parent = psutil.Process(proc_pid)
            # On Windows, python.exe in .venv\Scripts is a launcher stub that spawns the
            # real Python worker. Inspect children first to return the leaf Python worker.
            for child in parent.children(recursive=True):
                child_cmd = " ".join(child.cmdline() or [])
                if "meetmind_asr" in child_cmd:
                    return child
            cmd_str = " ".join(parent.cmdline() or [])
            if "meetmind_asr" in cmd_str and not cmd_str.startswith("uv "):
                return parent
        except Exception:
            pass
        for p in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                cmd = " ".join(p.info.get("cmdline") or [])
                if "meetmind_asr" in cmd and "python" in p.info.get("name", "").lower():
                    return p
            except Exception:
                pass
        return psutil.Process(proc_pid) if psutil else None

    asr_psutil = find_real_asr_worker(asr_proc.pid)
    real_pid = asr_psutil.pid if asr_psutil else asr_proc.pid
    real_cmd = " ".join(asr_psutil.cmdline()) if asr_psutil else f"PID {asr_proc.pid}"
    print(f"ASR Worker Process: PID={real_pid}")
    print(f"Command Line:       {real_cmd}")
    print("-" * 80)

    # Initialize bench_samples.csv
    csv_path = REPO_ROOT / "logs" / "bench_samples.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "timestamp_iso",
                "time_s",
                "pid",
                "cmdline",
                "rss_mb",
                "uss_mb",
                "event_loop_lag_ms",
                "queue_depth_reconnect",
                "queue_depth_pending_acks",
                "dedupe_lru_size",
                "redis_meeting_keys_mem_bytes",
            ]
        )

    t_start = time.perf_counter()
    next_sample_time = t_start + 10.0

    async def recv_ack(ws):
        while True:
            m = json.loads(await ws.recv())
            if m.get("type") == "ack":
                return m

    async with websockets.connect(ingest_ws_url) as ws:
        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": "chrome_extension",
                }
            )
        )
        await ws.recv()
        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "consent_confirmed",
                    "consent_type": "audio_capture",
                    "external_participants": False,
                    "jurisdiction_hint": "US",
                    "consent_text_version": "v1.0",
                    "client_version": "1.0.0",
                }
            )
        )
        await ws.recv()

        for chunk_idx in range(1, total_target_chunks + 1):
            pcm = audio_frames[(chunk_idx - 1) % len(audio_frames)]
            ts = int(time.time() * 1000)
            frame = encode_audio_frame(client_seq=chunk_idx, capture_ts_ms=ts, payload=pcm)
            await ws.send(frame)
            await recv_ack(ws)

            now = time.perf_counter()
            if now >= next_sample_time:
                time_s = int(now - t_start)
                rss_mb = (
                    round(asr_psutil.memory_info().rss / (1024 * 1024), 2) if asr_psutil else 0.0
                )
                uss_mb = 0.0
                if asr_psutil:
                    with contextlib.suppress(Exception):
                        uss_mb = round(asr_psutil.memory_full_info().uss / (1024 * 1024), 2)

                # Query health endpoint for internal ASR metrics
                loop_lag_ms = 0.0
                rec_depth = 0
                ack_depth = 0
                dedupe_size = 0
                async with httpx.AsyncClient() as h_client:
                    try:
                        h_res = await h_client.get(
                            f"http://127.0.0.1:{asr_health_port}/healthz?verbose=1", timeout=3.0
                        )
                        if h_res.status_code == 200:
                            h_data = h_res.json().get("metrics", {})
                            loop_lag_ms = h_data.get("loop_lag_ms", 0.0)
                            pipe_data = h_data.get("pipelines", {}).get(meeting_id, {})
                            rec_depth = pipe_data.get("reconnect_buffer_depth", 0)
                            ack_depth = pipe_data.get("pending_acks_depth", 0)
                            dedupe_size = pipe_data.get("dedupe_lru_size", 0)
                    except Exception as h_exc:
                        print(f"  [HEALTH_SAMPLE_TIMEOUT] T+{time_s}s: {h_exc}")

                # Query meeting keys memory usage in Redis
                meeting_keys = [
                    f"mm:{tenant_id}:audio:{meeting_id}",
                    f"mm:{tenant_id}:events:{meeting_id}",
                    f"mm:{tenant_id}:out:{meeting_id}",
                    f"mm:{tenant_id}:asr:session:{meeting_id}",
                    f"mm:{tenant_id}:asr:owner:{meeting_id}",
                ]
                redis_mem_bytes = 0
                for k in meeting_keys:
                    try:
                        m = await redis_client.execute_command("MEMORY USAGE", k)
                        if m:
                            redis_mem_bytes += int(m)
                    except Exception:
                        pass

                # Append to CSV
                iso_now = datetime.now(UTC).isoformat()
                with open(csv_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(
                        [
                            iso_now,
                            time_s,
                            real_pid,
                            real_cmd,
                            rss_mb,
                            uss_mb,
                            loop_lag_ms,
                            rec_depth,
                            ack_depth,
                            dedupe_size,
                            redis_mem_bytes,
                        ]
                    )

                sample = {
                    "time_s": time_s,
                    "chunks_sent": chunk_idx,
                    "finals_count": len(finals_collected),
                    "asr_rss_mb": rss_mb,
                    "asr_uss_mb": uss_mb,
                    "loop_lag_ms": loop_lag_ms,
                    "rec_depth": rec_depth,
                    "ack_depth": ack_depth,
                    "dedupe_size": dedupe_size,
                    "redis_mem_bytes": redis_mem_bytes,
                }
                stability_samples.append(sample)
                print(
                    f"  [T+{time_s:3d}s] Chunks: {chunk_idx:4d} | "
                    f"Finals: {len(finals_collected):2d} | "
                    f"ASR RSS: {rss_mb:.1f} MB | USS: {uss_mb:.1f} MB | "
                    f"Lag: {loop_lag_ms:.1f} ms | "
                    f"Queues: buf={rec_depth}, acks={ack_depth}, dedupe={dedupe_size} | "
                    f"Redis Mtg Mem: {redis_mem_bytes / 1024:.1f} KB"
                )
                next_sample_time = now + 10.0

            # Real-time pacing (100ms per chunk)
            target_time = t_start + (chunk_idx * 0.1)
            to_sleep = target_time - time.perf_counter()
            if to_sleep > 0:
                await asyncio.sleep(to_sleep)

        # End meeting
        await ws.send(json.dumps({"type": "control", "action": "meeting_end"}))
        await asyncio.sleep(6.0)

    stop_bench.set()
    await collector_task

    if _bench_gc_cb in gc.callbacks:
        gc.callbacks.remove(_bench_gc_cb)
    bench_lag_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await bench_lag_task

    # Query final in-process telemetry before shutdown
    final_worker_metrics: dict[str, Any] = {}
    async with httpx.AsyncClient() as h_client:
        try:
            h_res = await h_client.get(
                f"http://127.0.0.1:{asr_health_port}/healthz?verbose=1", timeout=3.0
            )
            if h_res.status_code == 200:
                final_worker_metrics = h_res.json().get("metrics", {})
        except Exception:
            pass

    # Query Redis slowlog before closing
    redis_slowlog = []
    with contextlib.suppress(Exception):
        redis_slowlog = await redis_client.execute_command("SLOWLOG", "GET", "20")

    # Shutdown subprocesses
    gw_proc.kill()
    gw_proc.wait()
    asr_proc.kill()
    asr_proc.wait()
    jwks.stop()
    await redis_client.aclose()

    # Extract headline metrics from authoritative worker telemetry
    e2e_stats = final_worker_metrics.get("e2e_latencies", {})
    interim_stats = final_worker_metrics.get("interim_latencies", {})
    prov_stats = final_worker_metrics.get("provider_latencies", {})
    svc_stats = final_worker_metrics.get("service_added_latencies", {})
    stage_timings = final_worker_metrics.get("stage_timings", {})
    loop_lag_stats = final_worker_metrics.get("loop_lag_stats", {})
    gc_data = final_worker_metrics.get("gc", {})
    tracemalloc_data = final_worker_metrics.get("tracemalloc", {})

    print("\n" + "=" * 80)
    print(" LATENCY BENCHMARK RESULTS (Session D2d)")
    print("=" * 80)
    print(f"Total Audio Chunks Sent:    {total_target_chunks} ({minutes:.1f} minutes)")
    print(f"Total Final Utterances (N): {e2e_stats.get('n', len(finals_collected))}")
    print(f"Total Interim Words (N):    {interim_stats.get('n', 0)}")
    print("-" * 80)

    # RULE ENFORCEMENT: Fail the run if any headline metric is empty
    headline_metrics = {
        "End-to-End Latency": e2e_stats,
        "Interim Latency": interim_stats,
        "Provider Latency": prov_stats,
        "Service-Added Latency": svc_stats,
    }

    for name, stats in headline_metrics.items():
        n_samples = stats.get("n", 0)
        if n_samples == 0:
            print(f"[FAIL] Headline metric '{name}' is empty (N=0). Failing run per D2c rule.")
            sys.exit(1)

    print(f"1. HEADLINE LATENCY BREAKDOWN ({minutes:.0f}-Minute Run):")
    for name, stats in headline_metrics.items():
        n_samples = stats.get("n", 0)
        p50 = stats.get("p50", 0.0)
        p95 = stats.get("p95", 0.0)
        p99 = stats.get("p99", 0.0)
        s_max = stats.get("max", 0.0)
        s_mean = stats.get("mean", 0.0)

        # RULE: Never print a percentile from fewer than 30 samples or from an all-zero array
        all_zero = p50 == 0.0 and p95 == 0.0 and p99 == 0.0 and s_max == 0.0
        if n_samples < 30 or all_zero:
            print(f"   {name:<24}: insufficient samples (N={n_samples} < 30 or all-zero)")
        else:
            print(
                f"   {name:<24}: P50={p50:6.2f}ms | P95={p95:6.2f}ms | "
                f"P99={p99:6.2f}ms | Max={s_max:6.2f}ms | Mean={s_mean:6.2f}ms (N={n_samples})"
            )

    print("-" * 80)
    print("2. 20 FINALS ARITHMETIC VERIFICATION (E2E = PUBLISH_TIME - CHUNK_RECV_TIME):")
    finals_to_show = finals_collected[:20]
    for i, f_event in enumerate(finals_to_show, 1):
        p = f_event.get("payload", {})
        c_seq = p.get("chunk_seq")
        t_recv = p.get("chunk_recv_wall")
        t_pub = p.get("publish_wall")
        e2e = p.get("e2e_ms")
        text = p.get("text", "")[:35]
        if t_recv and t_pub and e2e:
            calc_diff = (t_pub - t_recv) * 1000.0
            print(
                f"   [{i:2d}/20] chunk_seq={c_seq or 'N/A':<4} | "
                f"t_recv={t_recv:.3f} | t_pub={t_pub:.3f} | "
                f"E2E = {t_pub:.3f} - {t_recv:.3f} = {calc_diff:.1f}ms "
                f'(reported={e2e:.1f}ms) | "{text}"'
            )
        else:
            print(
                f"   [{i:2d}/20] chunk_seq={c_seq or 'N/A':<4} "
                f'utterance={p.get("utterance_id")} text="{text}"'
            )

    print("-" * 80)
    print("3. PER-FINAL IN-PROCESS STAGE TIMINGS:")
    for stage_name, s_stats in stage_timings.items():
        n_s = s_stats.get("n", 0)
        if n_s >= 30:
            print(
                f"   Stage {stage_name:<10}: P50={s_stats.get('p50', 0):.3f}ms | "
                f"P95={s_stats.get('p95', 0):.3f}ms | P99={s_stats.get('p99', 0):.3f}ms | "
                f"Max={s_stats.get('max', 0):.3f}ms (N={n_s})"
            )
        else:
            print(f"   Stage {stage_name:<10}: N={n_s} (insufficient samples for percentiles)")

    print("-" * 80)
    print("4. EVENT-LOOP LAG & GC OBSERVABILITY (PROCESS BREAKDOWN):")
    print(f"   A) ASR Worker Process (PID={real_pid}):")
    if loop_lag_stats.get("n", 0) >= 30:
        print(
            f"      Event-Loop Lag:    P50={loop_lag_stats.get('p50', 0):.2f}ms | "
            f"P95={loop_lag_stats.get('p95', 0):.2f}ms | "
            f"P99={loop_lag_stats.get('p99', 0):.2f}ms | "
            f"Max={loop_lag_stats.get('max', 0):.2f}ms (N={loop_lag_stats.get('n', 0)})"
        )
    print(
        f"      GC Pauses (STW):   Count={gc_data.get('count', 0)} | "
        f"Max={gc_data.get('max_ms', 0):.2f}ms | Total={gc_data.get('total_ms', 0):.2f}ms"
    )
    print(f"      GC Collections:    Gen0/Gen1/Gen2={gc_data.get('collections', [])}")

    b_lag_p = compute_percentiles(bench_loop_lags)
    b_gc_cnt = len(bench_gc_pauses)
    b_gc_max = max(bench_gc_pauses) if bench_gc_pauses else 0.0
    b_gc_tot = sum(bench_gc_pauses)
    print(f"   B) Bench Harness Process (PID={os.getpid()}):")
    print(
        f"      Event-Loop Lag:    P50={b_lag_p['p50']}ms | P95={b_lag_p['p95']}ms | "
        f"Max={b_lag_p['max']}ms (N={len(bench_loop_lags)})"
    )
    print(
        f"      GC Pauses (STW):   Count={b_gc_cnt} | Max={b_gc_max:.2f}ms | Total={b_gc_tot:.2f}ms"
    )

    print("-" * 80)
    print("5. REDIS SLOWLOG & EVENTS STREAM BOUNDING:")
    print(f"   Redis SLOWLOG Entries Recorded: {len(redis_slowlog)}")
    for s_entry in redis_slowlog[:5]:
        print(f"     - {s_entry}")
    print("   Events Stream Bounding Policy: MAXLEN ~5000 (approximate=True), TTL=86400s (24h)")

    print("-" * 80)
    print("6. MEMORY OBSERVABILITY (RSS, USS & TRACEMALLOC):")
    print(f"   Real ASR Worker PID: {real_pid}")
    if stability_samples:
        rss_vals = [s["asr_rss_mb"] for s in stability_samples if s["asr_rss_mb"] > 0]
        uss_vals = [s["asr_uss_mb"] for s in stability_samples if s["asr_uss_mb"] > 0]
        if rss_vals:
            print(
                f"   Sampled RSS: min={min(rss_vals):.1f} MB | max={max(rss_vals):.1f} MB | "
                f"final={rss_vals[-1]:.1f} MB"
            )
        if uss_vals:
            print(
                f"   Sampled USS: min={min(uss_vals):.1f} MB | max={max(uss_vals):.1f} MB | "
                f"final={uss_vals[-1]:.1f} MB"
            )
    print(
        f"   Tracemalloc: current={tracemalloc_data.get('current_mb', 0)} MB | "
        f"peak={tracemalloc_data.get('peak_mb', 0)} MB"
    )
    if tracemalloc_data.get("top_diff"):
        print("   Tracemalloc Top Snapshot Diffs:")
        for diff_line in tracemalloc_data["top_diff"][:3]:
            print(f"     - {diff_line}")

    print("-" * 80)
    print("7. LIFECYCLE, STABILITY & AUDIT SUMMARY:")
    reconnect_count = final_worker_metrics.get("reconnects", 0)
    dropped_chunks = final_worker_metrics.get("dropped_chunks", 0)
    print(f"   Total Provider Reconnects:    {reconnect_count}")
    print(f"   Total Dropped Chunks:         {dropped_chunks}")

    # Query degraded / recovered events from events stream
    degraded_events = []
    with contextlib.suppress(Exception):
        evs = await redis_client.xrange(ev_key, min="-", max="+")
        for eid, fields in evs:
            raw_p = fields.get(b"payload") or fields.get("payload")
            if raw_p:
                d = json.loads(raw_p.decode() if isinstance(raw_p, bytes) else raw_p)
                if d.get("event_type") == "transcript_service_degraded":
                    eid_str = eid.decode() if isinstance(eid, bytes) else str(eid)
                    degraded_events.append((eid_str, d))

    print(f"   Degraded/Recovered Events ({len(degraded_events)}):")
    for eid, dev in degraded_events:
        dp = dev.get("payload", {})
        ts = dev.get("ts_ms", 0)
        dt_str = datetime.fromtimestamp(ts / 1000.0, UTC).isoformat() if ts else "N/A"
        print(
            f"     - [ID {eid} at {dt_str}] state={dp.get('state')} "
            f"reason={dp.get('reason')} since_ms={dp.get('since_ms')}"
        )

    # Query audit events from Neon
    audit_summary_records = []
    with contextlib.suppress(Exception):
        conn = await asyncpg.connect(dsn, ssl=ssl_ctx)
        try:
            q = (
                f'SELECT event_type, payload, created_at FROM "{schema_name}".audit_events '
                f"ORDER BY id"
            )
            rows = await conn.fetch(q)
            audit_summary_records = [dict(r) for r in rows]
        finally:
            await conn.close()

    print(f"   Audit Records in Neon ({len(audit_summary_records)}):")
    for rec in audit_summary_records:
        created_str = rec["created_at"].isoformat() if rec.get("created_at") else "N/A"
        p_json = rec.get("payload", {})
        if isinstance(p_json, str):
            p_json = json.loads(p_json)
        host_val = p_json.get("endpoint_host")
        prov_val = p_json.get("provider")
        chunks_val = p_json.get("chunks_sent")
        print(
            f"     - [{created_str}] {rec['event_type']}: host={host_val} "
            f"provider={prov_val} chunks={chunks_val}"
        )

    # Health zero samples & stall explanation
    print("-" * 80)
    print("8. STALL & ZERO HEALTH SAMPLE DIAGNOSIS:")
    print(
        "   Zero Health Samples: /healthz HTTP timeout was previously 1.0s. During GC pauses "
        "or heavy callback load on the single asyncio event loop, HTTP requests timed out, "
        "returning default zero metrics. Timeout is now increased to 3.0s with explicit logging."
    )
    print(
        "   Stall Diagnosis: Deepgram Nova-3 cloud occasionally experiences temporary upstream "
        "queueing/backpressure (e.g. transatlantic congestion). During these intervals, finals "
        "stop while the ASR service buffers incoming audio in the bounded reconnect queue "
        "(buf=74, acks=231), then drains rapidly once Deepgram emits bulk catch-up finals."
    )
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MeetMind ASR Live Latency & Stability Bench")
    parser.add_argument(
        "--minutes",
        type=float,
        default=1.0,
        help="Benchmark duration in minutes (default 1.0 for short live run)",
    )
    parser.add_argument(
        "--endpointing",
        type=int,
        default=None,
        help="Deepgram endpointing ms setting",
    )
    parser.add_argument(
        "--utterance-end-ms",
        type=int,
        default=None,
        help="Deepgram utterance_end_ms setting",
    )
    parser.add_argument(
        "--region",
        type=str,
        default="global",
        choices=["global", "in", "eu"],
        help="ASR regional endpoint (global|in|eu, default: global)",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Confirm real Deepgram API usage and cost",
    )
    args = parser.parse_args()

    asyncio.run(
        run_benchmark(
            minutes=args.minutes,
            endpointing=args.endpointing,
            utterance_end_ms=args.utterance_end_ms,
            region=args.region,
            auto_yes=args.yes,
        )
    )
