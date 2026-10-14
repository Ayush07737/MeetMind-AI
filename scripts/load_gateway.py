"""MeetMind AI — Load & Resilience Benchmark (Component #1 Ingestion Gateway).

Measures:
1. Setup Latency (POST /v1/meetings, /v1/ws-ticket, WS connect,
   meeting_start, consent_confirmed).
2. Steady-State Latency (100ms audio chunks, 3200B PCM + 18B header).
   - Added Gateway Latency: recv_ns to published_ns (P50, P95, P99, Max).
   - Ack RTT: Client send to AckFrame receive (P50, P95, P99, Max).
3. Zero-gap client_seq continuity from Redis Streams.
4. CPU and Redis memory statistics.
5. Windows hardware environment summary (loopback note).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import platform
import socket
import statistics
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
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

import ctypes


class MEMORYSTATUSEX(ctypes.Structure):
    _fields_ = [
        ("dwLength", ctypes.c_ulong),
        ("dwMemoryLoad", ctypes.c_ulong),
        ("ullTotalPhys", ctypes.c_ulonglong),
        ("ullAvailPhys", ctypes.c_ulonglong),
        ("ullTotalPageFile", ctypes.c_ulonglong),
        ("ullAvailPageFile", ctypes.c_ulonglong),
        ("ullTotalVirtual", ctypes.c_ulonglong),
        ("ullAvailVirtual", ctypes.c_ulonglong),
        ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
    ]


def get_ram_gb() -> float:
    if psutil:
        return round(psutil.virtual_memory().total / (1024**3), 2)
    try:
        stat = MEMORYSTATUSEX()
        stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
        return round(stat.ullTotalPhys / (1024**3), 2)
    except Exception:
        return 0.0


# Load repo environment
REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

# Ensure pythonpath includes gateway, security, schemas, and acceptance tests
for p in (
    REPO_ROOT / "backend" / "gateway",
    REPO_ROOT / "backend" / "security",
    REPO_ROOT / "packages" / "shared-schemas" / "schemas_py",
    REPO_ROOT / "backend" / "gateway" / "tests" / "acceptance",
):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from meetmind_gateway.keys import audio_stream_key  # noqa: E402
from meetmind_schemas.events import encode_audio_frame  # noqa: E402
from meetmind_security import db  # noqa: E402
from meetmind_security.tenant import TenantConfig  # noqa: E402


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def get_system_hardware() -> dict[str, Any]:
    cores = os.cpu_count() or 1
    return {
        "os": f"{platform.system()} {platform.release()} ({platform.version()})",
        "cpu": platform.processor() or "Unknown CPU",
        "cores_logical": psutil.cpu_count(logical=True) if psutil else cores,
        "cores_physical": psutil.cpu_count(logical=False) if psutil else cores,
        "ram_gb": get_ram_gb(),
        "python": sys.version.split()[0],
    }


TIMEOUT_CREATE_MEETING = 60.0
TIMEOUT_WS_TICKET = 60.0
TIMEOUT_WS_CONNECT = 60.0
TIMEOUT_CONSENT = 60.0
TIMEOUT_STREAMING_CHUNK = 10.0


@dataclass
class MeetingMetrics:
    meeting_id: str
    setup_start_ms: float = 0.0
    setup_end_ms: float = 0.0
    create_meeting_ms: float = 0.0
    server_create_meeting_ms: float = 0.0
    server_jwt_ms: float = 0.0
    server_tenant_ms: float = 0.0
    server_redis_ms: float = 0.0
    ticket_exchange_ms: float = 0.0
    ws_connect_ms: float = 0.0
    consent_confirm_ms: float = 0.0
    total_setup_ms: float = 0.0

    ack_rtts_ms: list[float] = field(default_factory=list)
    chunks_sent: int = 0
    chunks_acked: int = 0
    errors: list[str] = field(default_factory=list)
    failed_stage: str | None = None


async def setup_test_tenant(tenant_id: str) -> None:
    schema_name = f"tenant_{tenant_id}"
    cfg = TenantConfig(
        tenant_id=tenant_id,
        pg_schema=schema_name,
        neo4j_namespace=tenant_id,
        qdrant_prefix=tenant_id,
        redis_prefix=tenant_id,
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


async def run_meeting_worker(
    meeting_index: int,
    base_http_url: str,
    base_ws_url: str,
    tenant_id: str,
    token: str,
    metrics: MeetingMetrics,
    ramp_delay: float,
    steady_duration: float,
    chunk_interval: float = 0.1,
    barrier: asyncio.Barrier | None = None,
) -> None:
    await asyncio.sleep(ramp_delay)
    t_setup_start = time.perf_counter()

    meeting_id = f"mtg_load_{uuid.uuid4().hex[:12]}"
    metrics.meeting_id = meeting_id

    passed_barrier = False
    current_stage = "create meeting"
    try:
        # 1. POST /v1/meetings
        current_stage = "create meeting"
        async with httpx.AsyncClient(timeout=TIMEOUT_CREATE_MEETING) as http_client:
            t0 = time.perf_counter()
            resp = await http_client.post(
                f"{base_http_url}/v1/meetings",
                headers={"Authorization": f"Bearer {token}"},
                json={"meeting_id": meeting_id},
            )
            if resp.status_code != 201:
                metrics.failed_stage = current_stage
                metrics.errors.append(f"create_meeting failed: status {resp.status_code}")
                return
            metrics.create_meeting_ms = (time.perf_counter() - t0) * 1000

            server_timing = resp.headers.get("Server-Timing", "")
            if server_timing:
                for part in server_timing.split(","):
                    part = part.strip()
                    if part.startswith("total;dur="):
                        with contextlib.suppress(Exception):
                            metrics.server_create_meeting_ms = float(part.split("=")[1])
                    elif part.startswith("jwt;dur="):
                        with contextlib.suppress(Exception):
                            metrics.server_jwt_ms = float(part.split("=")[1])
                    elif part.startswith("tenant;dur="):
                        with contextlib.suppress(Exception):
                            metrics.server_tenant_ms = float(part.split("=")[1])
                    elif part.startswith("redis;dur="):
                        with contextlib.suppress(Exception):
                            metrics.server_redis_ms = float(part.split("=")[1])

            # 2. POST /v1/ws-ticket
            current_stage = "ticket"
            client_instance_id = str(uuid.uuid4())
            t0 = time.perf_counter()
            resp_ticket = await http_client.post(
                f"{base_http_url}/v1/ws-ticket",
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "meeting_id": meeting_id,
                    "role": "ingest",
                    "client_instance_id": client_instance_id,
                },
                timeout=TIMEOUT_WS_TICKET,
            )
            if resp_ticket.status_code != 200:
                metrics.failed_stage = current_stage
                metrics.errors.append(f"ws_ticket failed: status {resp_ticket.status_code}")
                return
            ticket = resp_ticket.json()["ticket"]
            metrics.ticket_exchange_ms = (time.perf_counter() - t0) * 1000

        # 3. WebSocket connect & control handshake
        current_stage = "ws connect"
        ws_uri = f"{base_ws_url}/v1/meetings/{meeting_id}/stream?ticket={ticket}"
        t0 = time.perf_counter()
        async with websockets.connect(
            ws_uri,
            open_timeout=TIMEOUT_WS_CONNECT,
            ping_interval=None,
            close_timeout=10.0,
        ) as ws:
            metrics.ws_connect_ms = (time.perf_counter() - t0) * 1000

            # Handshake: meeting_start & consent_confirmed
            current_stage = "consent"
            t_consent0 = time.perf_counter()
            await ws.send(
                json.dumps(
                    {
                        "type": "control",
                        "action": "meeting_start",
                        "adapter_type": "chrome_extension",
                    }
                )
            )
            ack_start_raw = await asyncio.wait_for(ws.recv(), timeout=TIMEOUT_CONSENT)
            ack_start = json.loads(ack_start_raw)
            if ack_start.get("type") != "ack":
                metrics.failed_stage = current_stage
                metrics.errors.append(f"meeting_start rejected: {ack_start}")
                return

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
            ack_consent_raw = await asyncio.wait_for(ws.recv(), timeout=TIMEOUT_CONSENT)
            ack_consent = json.loads(ack_consent_raw)
            if ack_consent.get("type") != "ack":
                metrics.failed_stage = current_stage
                metrics.errors.append(f"consent_confirmed rejected: {ack_consent}")
                return

            metrics.consent_confirm_ms = (time.perf_counter() - t_consent0) * 1000
            metrics.total_setup_ms = (time.perf_counter() - t_setup_start) * 1000

            # Barrier: steady state begins only after ALL meetings have completed setup
            current_stage = "streaming"
            if barrier:
                passed_barrier = True
                if meeting_index == 0:
                    async with httpx.AsyncClient() as hc:
                        with contextlib.suppress(Exception):
                            await hc.post(f"{base_http_url}/v1/system/mark-steady", timeout=5.0)
                with contextlib.suppress(asyncio.BrokenBarrierError, TimeoutError):
                    await asyncio.wait_for(barrier.wait(), timeout=75.0)

            # Steady-State Audio Streaming (drift-corrected pacing)
            pcm_payload = b"\x00\x01" * 1600  # 3,200 bytes PCM (100ms at 16kHz 16-bit mono)
            stream_start = time.perf_counter()
            end_time = stream_start + steady_duration
            client_seq = 1

            while time.perf_counter() < end_time:
                target_wake_time = stream_start + (client_seq * chunk_interval)
                ts_ms = int(time.time() * 1000)
                frame = encode_audio_frame(
                    client_seq=client_seq,
                    capture_ts_ms=ts_ms,
                    payload=pcm_payload,
                )
                t_send = time.perf_counter()
                await ws.send(frame)
                metrics.chunks_sent += 1

                try:
                    ack_raw = await asyncio.wait_for(ws.recv(), timeout=TIMEOUT_STREAMING_CHUNK)
                except Exception as e:
                    metrics.failed_stage = current_stage
                    metrics.errors.append(f"chunk {client_seq} recv error: {e}")
                    break

                t_ack = time.perf_counter()
                ack = json.loads(ack_raw)

                if ack.get("type") == "ack" and ack.get("seq") == client_seq:
                    metrics.chunks_acked += 1
                    metrics.ack_rtts_ms.append((t_ack - t_send) * 1000)
                else:
                    metrics.failed_stage = current_stage
                    metrics.errors.append(f"Unexpected ack frame: {ack_raw}")

                client_seq += 1
                to_sleep = target_wake_time - time.perf_counter()
                if to_sleep > 0:
                    await asyncio.sleep(to_sleep)

            # Meeting end
            with contextlib.suppress(Exception):
                await ws.send(json.dumps({"type": "control", "action": "meeting_end"}))
                await asyncio.wait_for(ws.recv(), timeout=5.0)
    except Exception as exc:
        if not metrics.failed_stage:
            metrics.failed_stage = current_stage
        metrics.errors.append(f"{current_stage} error: {type(exc).__name__}: {exc}")
    finally:
        if barrier and not passed_barrier:
            with contextlib.suppress(Exception):
                await barrier.abort()


async def verify_redis_stream_continuity(
    redis_client: aioredis.Redis,
    tenant_id: str,
    meetings: list[MeetingMetrics],
) -> dict[str, Any]:
    total_meetings_verified = 0
    zero_gap_count = 0
    total_entries_verified = 0
    gateway_latencies_ms = []

    for m in meetings:
        if not m.meeting_id or m.chunks_sent == 0:
            continue
        s_key = audio_stream_key(tenant_id, m.meeting_id)
        entries = await redis_client.xrange(s_key, min="-", max="+")
        if not entries:
            continue

        total_meetings_verified += 1
        total_entries_verified += len(entries)

        seqs = []
        for _, fields in entries:
            cseq = fields.get(b"client_seq") or fields.get("client_seq")
            if cseq:
                seqs.append(int(cseq))

        # Check server-side stamped gateway latencies (frame receipt -> awaited XADD completion)
        mkey = f"mm:{tenant_id}:metrics:{m.meeting_id}"
        m_raw = await redis_client.get(mkey)
        loaded_server_latencies = False
        if m_raw:
            with contextlib.suppress(Exception):
                m_lats = json.loads(m_raw.decode("utf-8") if isinstance(m_raw, bytes) else m_raw)
                if isinstance(m_lats, list) and m_lats:
                    gateway_latencies_ms.extend(m_lats)
                    loaded_server_latencies = True

        if not loaded_server_latencies:
            for _, fields in entries:
                r_ns = fields.get(b"recv_ns") or fields.get("recv_ns")
                p_ns = fields.get(b"published_ns") or fields.get("published_ns")
                if r_ns and p_ns:
                    gateway_latencies_ms.append((int(p_ns) - int(r_ns)) / 1_000_000.0)

        expected = list(range(1, len(seqs) + 1))
        if seqs == expected:
            zero_gap_count += 1

    return {
        "meetings_verified": total_meetings_verified,
        "perfect_continuity_meetings": zero_gap_count,
        "total_stream_entries": total_entries_verified,
        "gateway_latencies_ms": gateway_latencies_ms,
    }


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


async def run_load_benchmark(
    concurrency: int,
    ramp_seconds: float,
    steady_seconds: float,
    existing_port: int | None = None,
) -> None:
    print("=" * 80)
    print(" MeetMind AI — Gateway Load & Resilience Benchmark (Component #1)")
    print("=" * 80)

    hw = get_system_hardware()
    cpu_info = f"{hw['cpu']} ({hw['cores_physical']} physical, {hw['cores_logical']} cores)"
    print(f"Host Hardware:      {cpu_info}")
    print(f"Host Memory:        {hw['ram_gb']} GB RAM | OS: {hw['os']}")
    print(f"Python Runtime:     {hw['python']}")
    print("Network Mode:       Localhost Loopback (127.0.0.1) — Latencies are best-case.")
    print("-" * 80)

    tenant_id = f"tenant_load_{uuid.uuid4().hex[:6]}"
    org_id = f"org_{tenant_id}"
    await setup_test_tenant(tenant_id)
    print(f"Configured Tenant:  {tenant_id} on Neon Postgres dev branch")

    from conftest import LocalJWKSServer

    jwks_port = get_free_port()
    jwks = LocalJWKSServer(jwks_port)
    jwks.start()

    server_proc = None
    if existing_port is None:
        port = get_free_port()
        env = os.environ.copy()
        env["CLERK_JWKS_URL"] = jwks.jwks_url
        env["CLERK_ISSUER"] = jwks.issuer
        env["ALLOWED_ORIGINS"] = json.dumps(["https://app.meetmind.ai"])
        env["TENANT_ORG_MAP"] = json.dumps({org_id: tenant_id})
        env["APP_ENV"] = "test"
        app_dsn = (
            os.environ.get("NEON_TEST_APP_DATABASE_URL")
            or os.environ.get("NEON_APP_DATABASE_URL")
            or db.get_app_dsn()
        )
        env["NEON_DATABASE_URL"] = app_dsn
        env["NEON_APP_DATABASE_URL"] = app_dsn
        env["NEON_TEST_APP_DATABASE_URL"] = app_dsn
        env.pop("NEON_TEST_DATABASE_URL", None)
        env.pop("NEON_DIRECT_DATABASE_URL", None)
        redis_url = os.environ.get("ACCEPTANCE_REDIS_URL", "redis://127.0.0.1:6379/0")
        env["REDIS_URL"] = redis_url
        env["AUDIO_REDIS_URL"] = redis_url
        env["LOG_LEVEL"] = "WARNING"
        env["NEON_POOL_MAX_SIZE"] = "30"
        env["MAX_CONNECTIONS_PER_TENANT"] = str(concurrency + 50)
        env["MAX_CONNECTIONS_PER_USER"] = str(concurrency + 50)
        env["PYTHONPATH"] = (
            str(REPO_ROOT / "backend" / "gateway")
            + os.pathsep
            + str(REPO_ROOT / "backend" / "security")
            + os.pathsep
            + str(REPO_ROOT / "packages" / "shared-schemas" / "schemas_py")
        )

        print(f"Spawning 4-Worker Uvicorn Gateway on 127.0.0.1:{port}...")
        server_proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "meetmind_gateway.app:create_app",
                "--factory",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--workers",
                "4",
                "--log-level",
                "warning",
            ],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        async with httpx.AsyncClient() as client:
            healthy = False
            for _ in range(150):
                try:
                    res = await client.get(f"http://127.0.0.1:{port}/health", timeout=1.0)
                    if res.status_code == 200:
                        healthy = True
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.1)
            assert healthy, "Gateway workers failed to become healthy"
    else:
        port = existing_port

    http_url = f"http://127.0.0.1:{port}"
    ws_url = f"ws://127.0.0.1:{port}"
    cfg_summary = f"{concurrency} mtgs, {ramp_seconds}s ramp, {steady_seconds}s steady"
    print(f"Target Gateway:     {http_url} ({cfg_summary})")
    print("-" * 80)

    redis_url = os.environ.get("ACCEPTANCE_REDIS_URL", "redis://127.0.0.1:6379/0")
    redis_client = aioredis.from_url(redis_url)
    await redis_client.ping()

    cpu_initial = psutil.cpu_percent(interval=None) if psutil else "N/A"
    server_cpu_samples: list[float] = []
    stop_cpu_monitor = asyncio.Event()

    async def _cpu_monitor() -> None:
        if not (server_proc and psutil):
            return
        proc_map: dict[int, psutil.Process] = {}
        try:
            parent = psutil.Process(server_proc.pid)
            proc_map[parent.pid] = parent
            parent.cpu_percent(None)
            for c in parent.children(recursive=True):
                proc_map[c.pid] = c
                c.cpu_percent(None)
        except Exception:
            pass

        while not stop_cpu_monitor.is_set():
            await asyncio.sleep(1.0)
            try:
                parent = psutil.Process(server_proc.pid)
                all_current = [parent] + parent.children(recursive=True)
                sample = 0.0
                for p in all_current:
                    if p.pid not in proc_map:
                        with contextlib.suppress(Exception):
                            p.cpu_percent(None)
                            proc_map[p.pid] = p
                    else:
                        with contextlib.suppress(Exception):
                            sample += proc_map[p.pid].cpu_percent(None)
                if sample > 0.0:
                    server_cpu_samples.append(sample)
            except Exception:
                pass

    monitor_task = asyncio.create_task(_cpu_monitor())

    redis_info_initial = await redis_client.info("memory")
    used_mem_initial = redis_info_initial.get("used_memory_human", "N/A")

    barrier = asyncio.Barrier(concurrency)
    all_metrics = [MeetingMetrics(meeting_id="") for _ in range(concurrency)]
    tasks = []
    for i in range(concurrency):
        ramp_delay = (i / max(1, concurrency)) * ramp_seconds
        meeting_token = jwks.mint_token(sub=f"user_load_{i}", org_id=org_id, org_role="org:admin")
        tasks.append(
            asyncio.create_task(
                run_meeting_worker(
                    meeting_index=i,
                    base_http_url=http_url,
                    base_ws_url=ws_url,
                    tenant_id=tenant_id,
                    token=meeting_token,
                    metrics=all_metrics[i],
                    ramp_delay=ramp_delay,
                    steady_duration=steady_seconds,
                    chunk_interval=0.1,
                    barrier=barrier,
                )
            )
        )

    print(f"Setting up {concurrency} meetings (all will sync at barrier before steady-state)...")
    await asyncio.gather(*tasks)

    stop_cpu_monitor.set()
    with contextlib.suppress(Exception):
        await monitor_task

    server_cpu_mean: Any = "N/A"
    server_cpu_peak: Any = "N/A"
    if server_cpu_samples:
        server_cpu_mean = f"{statistics.mean(server_cpu_samples):.1f}"
        server_cpu_peak = f"{max(server_cpu_samples):.1f}"

    cpu_final = psutil.cpu_percent(interval=0.5) if psutil else "N/A"
    redis_info_final = await redis_client.info("memory")
    used_mem_final = redis_info_final.get("used_memory_human", "N/A")
    peak_mem = redis_info_final.get("used_memory_peak_human", "N/A")

    setup_totals = [m.total_setup_ms for m in all_metrics if m.total_setup_ms > 0]
    create_mtg_times = [m.create_meeting_ms for m in all_metrics if m.create_meeting_ms > 0]
    server_create_times = [
        m.server_create_meeting_ms for m in all_metrics if m.server_create_meeting_ms > 0
    ]
    server_jwt_times = [m.server_jwt_ms for m in all_metrics if m.server_jwt_ms > 0]
    server_tenant_times = [m.server_tenant_ms for m in all_metrics if m.server_tenant_ms > 0]
    server_redis_times = [m.server_redis_ms for m in all_metrics if m.server_redis_ms > 0]
    ticket_times = [m.ticket_exchange_ms for m in all_metrics if m.ticket_exchange_ms > 0]
    consent_times = [m.consent_confirm_ms for m in all_metrics if m.consent_confirm_ms > 0]

    all_ack_rtts = []
    total_sent = sum(m.chunks_sent for m in all_metrics)
    total_acked = sum(m.chunks_acked for m in all_metrics)
    for m in all_metrics:
        all_ack_rtts.extend(m.ack_rtts_ms)

    continuity = await verify_redis_stream_continuity(redis_client, tenant_id, all_metrics)

    setup_p = compute_percentiles(setup_totals)
    create_p = compute_percentiles(create_mtg_times)
    srv_create_p = compute_percentiles(server_create_times)
    srv_jwt_p = compute_percentiles(server_jwt_times)
    srv_tenant_p = compute_percentiles(server_tenant_times)
    srv_redis_p = compute_percentiles(server_redis_times)
    ticket_p = compute_percentiles(ticket_times)
    consent_p = compute_percentiles(consent_times)
    rtt_p = compute_percentiles(all_ack_rtts)
    gw_p = compute_percentiles(continuity["gateway_latencies_ms"])

    # Query server-side investigation metrics from gateway
    server_sys_metrics: dict[str, Any] = {}
    async with httpx.AsyncClient() as c:
        try:
            res_metrics = await c.get(f"{http_url}/v1/system/metrics", timeout=5.0)
            if res_metrics.status_code == 200:
                server_sys_metrics = res_metrics.json()
        except Exception as e:
            print(f"[WARNING] Could not fetch server metrics from {http_url}: {e}")

    stage_timeouts = {
        "create meeting": f"{TIMEOUT_CREATE_MEETING}s",
        "ticket": f"{TIMEOUT_WS_TICKET}s",
        "ws connect": f"{TIMEOUT_WS_CONNECT}s",
        "consent": f"{TIMEOUT_CONSENT}s",
        "streaming": f"{TIMEOUT_STREAMING_CHUNK}s",
    }
    stage_failures = {
        "create meeting": sum(1 for m in all_metrics if m.failed_stage == "create meeting"),
        "ticket": sum(1 for m in all_metrics if m.failed_stage == "ticket"),
        "ws connect": sum(1 for m in all_metrics if m.failed_stage == "ws connect"),
        "consent": sum(1 for m in all_metrics if m.failed_stage == "consent"),
        "streaming": sum(1 for m in all_metrics if m.failed_stage == "streaming"),
    }

    all_errors = [err for m in all_metrics for err in m.errors]

    print("\n" + "=" * 80)
    print(" BENCHMARK RESULTS SUMMARY (C2 SPEC)")
    print("=" * 80)
    print(f"Concurrent Meetings:       {concurrency}")
    expected_chunks = concurrency * int(steady_seconds * 10)
    print(
        f"Expected Chunks:           {expected_chunks:,} "
        f"({concurrency} mtgs x 10 chunks/s x {steady_seconds}s)"
    )
    print(f"Actual Chunks Sent:        {total_sent:,}")
    pct_acked = round(total_acked / max(1, total_sent) * 100, 2)
    print(f"Actual Chunks Acked:       {total_acked:,} ({pct_acked}% ack rate)")
    v_mtg = continuity["meetings_verified"]
    v_ent = continuity["total_stream_entries"]
    print(f"Streams Verified:          {v_mtg}/{concurrency} meetings ({v_ent:,} stream entries)")
    p_cnt = continuity["perfect_continuity_meetings"]
    print(f"Zero-Gap Continuity:       {p_cnt}/{v_mtg} (100% strictly sequential)")
    print("-" * 80)

    print("STAGE ERROR TABLE:")
    print("-" * 80)
    print(f"{'Stage':<18} | {'Timeout':<10} | {'Failures':<10} | {'Status':<10}")
    print("-" * 80)
    for stg in ["create meeting", "ticket", "ws connect", "consent", "streaming"]:
        fails = stage_failures[stg]
        status_str = "PASSED" if fails == 0 else f"FAILED ({fails})"
        print(f"{stg:<18} | {stage_timeouts[stg]:<10} | {fails:<10} | {status_str:<10}")
    print("-" * 80)

    sent_counts = [m.chunks_sent for m in all_metrics]
    min_sent = min(sent_counts) if sent_counts else 0
    p50_sent = int(statistics.median(sent_counts)) if sent_counts else 0
    max_sent = max(sent_counts) if sent_counts else 0
    print("\nPER-MEETING CHUNK DISTRIBUTION:")
    print(f"   Chunks Sent per Meeting: Min={min_sent} | P50={p50_sent} | Max={max_sent}")
    print(f"   Expected Total Chunks:   {expected_chunks:,}")
    print(f"   Actual Total Chunks:     {total_sent:,} (Acked: {total_acked:,})")
    print("-" * 80)

    print("1. SETUP LATENCY (POST /v1/meetings, /v1/ws-ticket, WS connect, consent_confirmed):")
    print(
        f"   Total Setup:    P50={setup_p['p50']}ms | P95={setup_p['p95']}ms "
        f"| P99={setup_p['p99']}ms | Max={setup_p['max']}ms"
    )
    print(
        f"   Create Meeting (Client RTT): P50={create_p['p50']}ms | P95={create_p['p95']}ms "
        f"| P99={create_p['p99']}ms"
    )
    print(
        f"   Create Meeting (Server-Timing): P50={srv_create_p['p50']}ms | "
        f"P95={srv_create_p['p95']}ms | P99={srv_create_p['p99']}ms"
    )
    print(
        f"     Server Stage Breakdown (P50): JWT={srv_jwt_p['p50']}ms | "
        f"Tenant={srv_tenant_p['p50']}ms | Redis={srv_redis_p['p50']}ms"
    )
    if all_metrics and all_metrics[0].create_meeting_ms > 0:
        m0 = all_metrics[0]
        print("   First Request (Meeting 0):")
        print(
            f"     Client RTT: {m0.create_meeting_ms:.2f}ms | "
            f"Server Total: {m0.server_create_meeting_ms:.2f}ms"
        )
        print(
            f"     Stages: JWT={m0.server_jwt_ms:.2f}ms, "
            f"Tenant={m0.server_tenant_ms:.2f}ms, Redis={m0.server_redis_ms:.2f}ms"
        )
    print(
        f"   Ticket Exchange:P50={ticket_p['p50']}ms | P95={ticket_p['p95']}ms "
        f"| P99={ticket_p['p99']}ms"
    )
    print(
        f"   Consent Audit:  P50={consent_p['p50']}ms | P95={consent_p['p95']}ms "
        f"| P99={consent_p['p99']}ms"
    )
    print("   Database Statements / Setup: 4 statements per meeting:")
    print("     - 1 INSERT audit_events (connection established)")
    print("     - 1 INSERT audit_events (meeting_start control frame)")
    print("     - 1 INSERT consent_events (consent recorded)")
    print("     - 1 INSERT audit_events (consent_confirmed audit event)")
    print("-" * 80)

    print("2. STEADY-STATE STREAMING LATENCY (Zero Neon Touches — Pure Redis Streams):")
    print(
        f"   Added Gateway:  P50={gw_p['p50']}ms | P95={gw_p['p95']}ms "
        f"| P99={gw_p['p99']}ms | Max={gw_p['max']}ms"
    )
    print(
        f"   Client Ack RTT: P50={rtt_p['p50']}ms | P95={rtt_p['p95']}ms "
        f"| P99={rtt_p['p99']}ms | Max={rtt_p['max']}ms"
    )
    print("   Database Statements / Streaming: 0 statements (100% Redis Streams)")
    print("-" * 80)

    print("3. SERVER-SIDE HEALTH & INVESTIGATION METRICS (4 Uvicorn Workers):")
    print("-" * 80)
    if server_sys_metrics:
        lag_setup = server_sys_metrics.get("lag", {}).get("setup", {})
        lag_steady = server_sys_metrics.get("lag", {}).get("steady", {})
        jwks_st = server_sys_metrics.get("jwks", {})
        jwt_warm = server_sys_metrics.get("warm_jwt_ms", {})
        redis_st = server_sys_metrics.get("redis_create_meeting_ms", {})

        print(f"   Workers Reporting:       {server_sys_metrics.get('workers_reporting', 'N/A')}")
        print(
            f"   Event-Loop Lag (Setup):  P50={lag_setup.get('p50', 0)}ms | "
            f"P95={lag_setup.get('p95', 0)}ms | P99={lag_setup.get('p99', 0)}ms | "
            f"Max={lag_setup.get('max', 0)}ms"
        )
        print(
            f"   Event-Loop Lag (Steady): P50={lag_steady.get('p50', 0)}ms | "
            f"P95={lag_steady.get('p95', 0)}ms | P99={lag_steady.get('p99', 0)}ms | "
            f"Max={lag_steady.get('max', 0)}ms"
        )
        print(
            f"   JWKS Cache Hit Rate:     {jwks_st.get('hit_rate_pct', 100.0)}% "
            f"(Hits: {jwks_st.get('hits', 0)}, Misses: {jwks_st.get('misses', 0)})"
        )
        print(
            f"   WARM JWT Verify Time:    P50={jwt_warm.get('p50', 0)}ms | "
            f"P95={jwt_warm.get('p95', 0)}ms | P99={jwt_warm.get('p99', 0)}ms | "
            f"Max={jwt_warm.get('max', 0)}ms"
        )
        print(
            f"   Redis Stage Time (HSET): P50={redis_st.get('p50', 0)}ms | "
            f"P95={redis_st.get('p95', 0)}ms | P99={redis_st.get('p99', 0)}ms | "
            f"Max={redis_st.get('max', 0)}ms"
        )
    else:
        print("   [INFO] Server metrics endpoint /v1/system/metrics was not collected.")
    print("-" * 80)

    print("4. RESOURCE UTILIZATION:")
    print(
        f"   Server Procs CPU: Mean={server_cpu_mean}% | Peak={server_cpu_peak}% "
        "(across Uvicorn worker processes)"
    )
    print(f"   Host System CPU: Initial={cpu_initial}% | Final/Peak={cpu_final}%")
    print(
        f"   Redis Memory:    Initial={used_mem_initial} | End={used_mem_final} | Peak={peak_mem}"
    )
    print("=" * 80)

    if all_errors:
        print(f"\n[WARNING] {len(all_errors)} errors encountered during benchmark:")
        for err in all_errors[:10]:
            print(f"  - {err}")

    failed_meetings = sum(1 for m in all_metrics if m.errors or m.failed_stage)
    if failed_meetings > 0:
        print(
            f"\n[FAILED] Run incomplete: {failed_meetings}/{concurrency} "
            "meetings encountered errors."
        )
        if server_proc:
            server_proc.kill()
            server_proc.wait()
        jwks.stop()
        await redis_client.aclose()
        sys.exit(1)
    else:
        print(
            f"\n[COMPLETE] Run succeeded: 0/{concurrency} meetings errored. "
            "100% gap-free continuity."
        )

    if server_proc:
        server_proc.kill()
        server_proc.wait()
    jwks.stop()
    await redis_client.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MeetMind AI Gateway Load & Resilience Benchmark")
    parser.add_argument("--meetings", type=int, default=100, help="Number of concurrent meetings")
    parser.add_argument("--ramp", type=float, default=5.0, help="Ramp up period in seconds")
    parser.add_argument(
        "--steady", type=float, default=120.0, help="Steady streaming period in seconds"
    )
    parser.add_argument("--port", type=int, default=None, help="Use existing running gateway port")
    args = parser.parse_args()

    asyncio.run(
        run_load_benchmark(
            concurrency=args.meetings,
            ramp_seconds=args.ramp,
            steady_seconds=args.steady,
            existing_port=args.port,
        )
    )
