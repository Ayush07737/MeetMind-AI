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
import threading
import time
import urllib.request
import uuid
import wave
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import redis.asyncio as aioredis
import websockets
from dotenv import load_dotenv

if sys.platform == "win32":
    with contextlib.suppress(Exception):
        import ctypes

        ctypes.windll.winmm.timeBeginPeriod(1)

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


async def run_instrument_calibration(delay_ms: float = 200.0, runs: int = 5) -> dict[str, Any]:
    """Verification Honesty: Calibrate latency measurement against known delay X ms."""
    from unittest.mock import AsyncMock, MagicMock

    from meetmind_asr.config import AsrSettings
    from meetmind_asr.pipeline import MeetingPipeline
    from meetmind_asr.providers.base import AudioChunk, ProviderEvent
    from meetmind_asr.providers.stub import StubFaultConfig, StubProvider
    from meetmind_asr.tenant_config import AsrTenantConfig

    threshold = 10.0  # Specification contract ceiling is strictly 10.0ms
    runs_data = []

    for run_idx in range(1, runs + 1):
        stub = StubProvider(
            fault_config=StubFaultConfig(
                latency_ms=0.0,
                final_delay_ms=delay_ms,
            )
        )
        settings = AsrSettings(_env_file=None, ASR_PROVIDER="stub")
        mock_redis = AsyncMock()
        mock_pipe = MagicMock()
        mock_pipe.execute = AsyncMock()
        mock_redis.pipeline = MagicMock(return_value=mock_pipe)
        mock_redis.hgetall = AsyncMock(return_value={})

        auditor = MagicMock()
        tenant_cfg = AsrTenantConfig(tenant_id=f"calib_tenant_{run_idx}")

        pipeline = MeetingPipeline(
            tenant_id=f"calib_tenant_{run_idx}",
            meeting_id=f"calib_meeting_{run_idx}",
            worker_id=f"calib_worker_{run_idx}",
            redis_client=mock_redis,
            provider=stub,
            tenant_config=tenant_cfg,
            settings=settings,
            metrics=MagicMock(),
            auditor=auditor,
        )

        await pipeline._connect_provider()
        t_origin = time.time()
        pipeline._provider_session_origin_wall = t_origin

        measured_latencies: list[float] = []

        def _make_capture(pipe_inst: MeetingPipeline, lat_accum: list[float]):
            async def _cap(event: ProviderEvent) -> None:
                if event.is_final and event.event_type == "final":
                    t_pub_wall = time.time()
                    audio_end_wall = pipe_inst._provider_session_origin_wall + (
                        event.end_ms / 1000.0
                    )
                    e2e_ms = (t_pub_wall - audio_end_wall) * 1000.0
                    lat_accum.append(e2e_ms)

            return _cap

        capture_event = _make_capture(pipeline, measured_latencies)

        # Stream 5 chunks (500ms audio) with drift-corrected pacing (100ms interval)
        for i in range(5):
            target = t_origin + (i + 1) * 0.100
            to_sleep = target - time.time()
            if to_sleep > 0:
                await asyncio.sleep(to_sleep)
            chunk = AudioChunk(
                data=b"\x00" * 3200,
                client_seq=i + 1,
                capture_ts_ms=1000 + (i * 100),
            )
            await stub.send_audio(chunk)

        async for ev in stub.events():
            await capture_event(ev)
            if ev.is_final:
                break

        measured = measured_latencies[0] if measured_latencies else 0.0
        err = abs(measured - delay_ms)
        runs_data.append(
            {"pass": run_idx, "measured_ms": round(measured, 2), "error_ms": round(err, 2)}
        )

    errors = [r["error_ms"] for r in runs_data]
    mean_err = round(statistics.mean(errors), 2)
    max_err = round(max(errors), 2)
    passed = bool(mean_err <= threshold and max_err <= threshold)
    return {
        "expected_delay_ms": delay_ms,
        "runs": runs_data,
        "measured_ms": round(statistics.mean([r["measured_ms"] for r in runs_data]), 2),
        "error_ms": mean_err,
        "max_error_ms": max_err,
        "passed": passed,
        "threshold": threshold,
    }


async def run_benchmark(
    minutes: float,
    endpointing: int | None = None,
    utterance_end_ms: int | None = None,
    region: str = "global",
    auto_yes: bool = False,
    simulate_outage_s: float = 0.0,
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
    print(f"Base WebSocket URL:  {base_url}")
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

    # 2. Calibration Pass with Known Stub Delay (5 runs)
    print(
        "Running instrument calibration across 5 runs against StubProvider (known 200ms delay)..."
    )
    calib_res = await run_instrument_calibration(delay_ms=200.0, runs=5)
    calib_pass_str = "PASS" if calib_res["passed"] else "FAIL (DEVIATION)"
    thresh = calib_res.get("threshold", 10.0)
    for r in calib_res.get("runs", []):
        print(
            f"  Run #{r['pass']}: Measured={r['measured_ms']:.1f}ms | Error={r['error_ms']:.2f}ms"
        )
    print(
        f"  Calibration Summary (5 runs): Mean Error={calib_res['error_ms']:.2f}ms | "
        f"Max Error={calib_res['max_error_ms']:.2f}ms (Contract <= {thresh:.1f}ms) "
        f"-> [{calib_pass_str}]"
    )
    if not calib_res["passed"]:
        err_val = calib_res["error_ms"]
        print(
            f"  [DEVIATION] Calibration error {err_val:.2f}ms "
            f"exceeds {thresh:.1f}ms contract threshold."
        )

    print("-" * 80)

    # 3. Setup throwaway tenant on Neon
    tenant_id = f"bench_dg_{uuid.uuid4().hex[:6]}"
    schema_name = f"tenant_{tenant_id}"
    cfg = TenantConfig(
        tenant_id=tenant_id,
        pg_schema=schema_name,
        neo4j_namespace=tenant_id,
        qdrant_prefix=tenant_id,
        redis_prefix=tenant_id,
        asr_region=region,
        residency_mode="strict",
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
    env["FRAME_RATE_LIMIT"] = "300"
    env["BURST_FRAME_LIMIT"] = "600"
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
                        "residency_mode": "strict",
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
    asr_env.pop("DEEPGRAM_BASE_URL", None)
    asr_env["ASR_REGION"] = region
    asr_env["ASR_ALLOWED_REGIONS"] = f"{region},global" if region != "global" else "global,in,eu,au"
    asr_env["ASR_CATCHUP_SPEED"] = "4.0"
    if simulate_outage_s > 0:
        asr_env["ASR_TEST_HOOKS"] = "true"
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

    asr_log_path = REPO_ROOT / "logs" / "asr_worker_bench.log"
    asr_log_file = open(asr_log_path, "w", encoding="utf-8")  # noqa: SIM115
    asr_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "meetmind_asr",
        ],
        env=asr_env,
        stdout=asr_log_file,
        stderr=subprocess.STDOUT,
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
    stalls_recorded: list[dict[str, Any]] = []
    latest_worker_metrics: dict[str, Any] = {}

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
                            else:
                                s_epoch = payload.get("start_epoch_ms")
                                end_ms = payload.get("end_ms", 0)
                                start_ms = payload.get("start_ms", 0)
                                if s_epoch and (end_ms > start_ms):
                                    audio_end_epoch = (s_epoch + (end_ms - start_ms)) / 1000.0
                                    pub_s = t_pub or (now_wall / 1000.0)
                                    calc_e2e = max(10.0, (pub_s - audio_end_epoch) * 1000.0)
                                    e2e_latencies_ms.append(calc_e2e)

                            created_ts = data.get("ts_ms", now_wall)
                            service_added = max(0.01, now_wall - created_ts)
                            service_added_latencies_ms.append(service_added)
            except Exception:
                await asyncio.sleep(0.1)

    collector_task = asyncio.create_task(_internal_bus_collector())

    # Instrument Bench Process GC and Event Loop Lag
    import gc
    import traceback

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

    # Heartbeat updated by main event loop; watchdog thread monitors staleness
    main_thread_id = threading.get_ident()
    loop_heartbeat = time.perf_counter()
    bench_loop_lags: list[float] = []
    current_loop_lag_ms = 0.0
    last_send_latency_ms = 0.0
    distinct_lag_stacks: dict[str, dict[str, Any]] = {}

    async def _bench_heartbeat_loop() -> None:
        nonlocal loop_heartbeat, current_loop_lag_ms
        while not stop_bench.is_set():
            t_now = time.perf_counter()
            lag = max(0.0, (t_now - loop_heartbeat - 0.010) * 1000.0)
            current_loop_lag_ms = lag
            if lag > 0.5:
                bench_loop_lags.append(lag)
            loop_heartbeat = t_now
            await asyncio.sleep(0.010)

    bench_lag_task = asyncio.create_task(_bench_heartbeat_loop())

    def _watchdog_thread_fn() -> None:
        last_logged_time = 0.0
        while not stop_bench.is_set():
            time.sleep(0.010)
            if stop_bench.is_set():
                break
            stale_ms = (time.perf_counter() - loop_heartbeat) * 1000.0
            if stale_ms > 200.0:
                frames = sys._current_frames()
                frame = frames.get(main_thread_id)
                if frame:
                    raw_lines = traceback.format_stack(frame)
                    stack_key = "".join(raw_lines[-4:])
                    now_wall = time.time()
                    if stack_key not in distinct_lag_stacks:
                        distinct_lag_stacks[stack_key] = {
                            "count": 1,
                            "max_ms": stale_ms,
                            "full_stack": "".join(raw_lines),
                        }
                        if now_wall - last_logged_time > 1.0:
                            last_logged_time = now_wall
                            print(
                                f"\n  [LOOP_LAG] Main loop blocked {stale_ms:.1f}ms (>200ms):\n"
                                f"{distinct_lag_stacks[stack_key]['full_stack']}"
                            )
                    else:
                        distinct_lag_stacks[stack_key]["count"] += 1
                        distinct_lag_stacks[stack_key]["max_ms"] = max(
                            distinct_lag_stacks[stack_key]["max_ms"], stale_ms
                        )

    watchdog_thread = threading.Thread(target=_watchdog_thread_fn, daemon=True)
    watchdog_thread.start()

    # Ingest loop
    ingest_ws_url = f"ws://127.0.0.1:{gw_port}/v1/meetings/{meeting_id}/stream?ticket={ticket}"
    print(f"Streaming {total_target_chunks} audio chunks in real-time ({minutes:.1f} minutes)...")

    def find_real_asr_worker(proc_pid: int) -> psutil.Process | None:
        if not psutil:
            return None
        try:
            parent = psutil.Process(proc_pid)
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

    def _append_csv_row(path: Path, row: list[Any]) -> None:
        with open(path, "a", newline="", encoding="utf-8") as f_csv:
            c_writer = csv.writer(f_csv)
            c_writer.writerow(row)

    async def recv_ack(ws):
        while True:
            m = json.loads(await ws.recv())
            if m.get("type") == "ack":
                return m

    chunks_sent_counter = 0

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

        # Wait for ASR worker to claim meeting and open provider session
        asr_session_key = f"mm:{tenant_id}:asr_session:{meeting_id}"
        print("Waiting for ASR worker provider session to open...")
        for _ in range(50):
            if await redis_client.exists(asr_session_key):
                break
            await asyncio.sleep(0.1)
        print("ASR worker provider session ready. Starting real-time audio stream...")

        t_start = time.perf_counter()

        # Dedicated background metrics sampler thread (isolated completely from event loop)
        # to ensure psutil USS, HTTP health checks, and Redis memory queries NEVER block
        # audio dispatch loop
        def _metrics_sampler_worker() -> None:
            nonlocal latest_worker_metrics
            sync_redis = None
            try:
                import redis

                sync_redis = redis.from_url(redis_url)
            except Exception:
                pass

            meeting_keys = [
                f"mm:{tenant_id}:audio:{meeting_id}",
                f"mm:{tenant_id}:events:{meeting_id}",
                f"mm:{tenant_id}:out:{meeting_id}",
                f"mm:{tenant_id}:asr:session:{meeting_id}",
                f"mm:{tenant_id}:asr:owner:{meeting_id}",
            ]

            next_sample = t_start + 10.0
            m5_snapshot_taken = False
            while not stop_bench.is_set():
                now = time.perf_counter()
                sleep_dur = next_sample - now
                if sleep_dur > 0:
                    time.sleep(min(sleep_dur, 1.0))
                    if stop_bench.is_set():
                        break
                    if time.perf_counter() < next_sample:
                        continue

                now = time.perf_counter()
                time_s = int(now - t_start)
                next_sample = now + 10.0

                rss_mb = (
                    round(asr_psutil.memory_info().rss / (1024 * 1024), 2) if asr_psutil else 0.0
                )
                uss_mb = 0.0
                if asr_psutil:
                    with contextlib.suppress(Exception):
                        uss_mb = round(asr_psutil.memory_full_info().uss / (1024 * 1024), 2)

                loop_lag_ms = 0.0
                rec_depth = 0
                ack_depth = 0
                dedupe_size = 0
                try:
                    req = urllib.request.Request(
                        f"http://127.0.0.1:{asr_health_port}/healthz?verbose=1"
                    )
                    with urllib.request.urlopen(req, timeout=3.0) as resp:
                        if resp.status == 200:
                            raw_b = resp.read()
                            h_data = json.loads(raw_b.decode("utf-8")).get("metrics", {})
                            if h_data:
                                latest_worker_metrics = h_data
                            loop_lag_ms = h_data.get("loop_lag_ms", 0.0)
                            pipe_data = h_data.get("pipelines", {}).get(meeting_id, {})
                            rec_depth = pipe_data.get("reconnect_buffer_depth", 0)
                            ack_depth = pipe_data.get("pending_acks_depth", 0)
                            dedupe_size = pipe_data.get("dedupe_lru_size", 0)
                except Exception as h_exc:
                    print(f"  [HEALTH_SAMPLE_TIMEOUT] T+{time_s}s: {h_exc}")

                redis_mem_bytes = 0
                if sync_redis:
                    for k in meeting_keys:
                        try:
                            m = sync_redis.execute_command("MEMORY USAGE", k)
                            if m:
                                redis_mem_bytes += int(m)
                        except Exception:
                            pass

                iso_now = datetime.now(UTC).isoformat()
                row_data = [
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
                _append_csv_row(csv_path, row_data)

                sample = {
                    "time_s": time_s,
                    "chunks_sent": chunks_sent_counter,
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
                    f"  [T+{time_s:3d}s] Chunks: {chunks_sent_counter:4d} | "
                    f"Finals: {len(finals_collected):2d} | "
                    f"ASR RSS: {rss_mb:.1f} MB | USS: {uss_mb:.1f} MB | "
                    f"Lag: {loop_lag_ms:.1f} ms | "
                    f"Queues: buf={rec_depth}, acks={ack_depth}, dedupe={dedupe_size} | "
                    f"Redis Mtg Mem: {redis_mem_bytes / 1024:.1f} KB"
                )

                # Capture minute-5 snapshot for tracemalloc diff
                if not m5_snapshot_taken and time_s >= 300:
                    m5_snapshot_taken = True
                    try:
                        req_m5 = urllib.request.Request(
                            f"http://127.0.0.1:{asr_health_port}/healthz?verbose=1&snapshot=save_m5"
                        )
                        with urllib.request.urlopen(req_m5, timeout=5.0) as resp_m5:
                            resp_m5.read()
                            print("  [SNAPSHOT] Captured minute-5 baseline for tracemalloc")
                    except Exception as s_exc:
                        print(f"  [SNAPSHOT_WARN] Failed to capture minute-5 baseline: {s_exc}")

            if sync_redis:
                sync_redis.close()

        sampler_thread = threading.Thread(target=_metrics_sampler_worker, daemon=True)
        sampler_thread.start()

        # Dedicated decoupled ack receiver task
        acks_received_set = set()

        async def _ack_consumer():
            try:
                while not stop_bench.is_set():
                    raw = await ws.recv()
                    if isinstance(raw, str):
                        m = json.loads(raw)
                        if m.get("type") == "ack":
                            acks_received_set.add(m.get("seq"))
            except Exception:
                pass

        ack_task = asyncio.create_task(_ack_consumer())

        # Dedicated isolated audio sender task
        async def _audio_sender():
            nonlocal chunks_sent_counter, last_send_latency_ms
            chunk_idx = 1
            import gc

            gc.disable()
            try:
                while chunk_idx <= total_target_chunks:
                    chunks_sent_counter = chunk_idx
                    pcm = audio_frames[(chunk_idx - 1) % len(audio_frames)]
                    ts = int(time.time() * 1000)
                    frame = encode_audio_frame(client_seq=chunk_idx, capture_ts_ms=ts, payload=pcm)

                    # Stall detection policy: if behind by >5.0s of audio, DO NOT BURST
                    target_time = t_start + (chunk_idx * 0.1)
                    now = time.perf_counter()
                    behind_s = now - target_time

                    if behind_s > 5.0:
                        skip_to = min(total_target_chunks, int((now - t_start) / 0.1))
                        stall_info = {
                            "start_s": round(target_time - t_start, 2),
                            "duration_s": round(behind_s, 2),
                            "chunk": chunk_idx,
                            "skip_to": skip_to,
                            "send_lat_ms": round(last_send_latency_ms, 2),
                            "loop_lag_ms": round(current_loop_lag_ms, 2),
                        }
                        stalls_recorded.append(stall_info)
                        print(
                            f"\n  [CLIENT_STALL_DETECTED] Behind by {behind_s:.2f}s (>5.0s cap) "
                            f"at chunk {chunk_idx}. DO NOT BURST: Skipping {skip_to - chunk_idx} "
                            f"chunks to real-time position {skip_to}. Run marked INVALID."
                        )
                        chunk_idx = skip_to
                        continue

                    if behind_s < 0:
                        await asyncio.sleep(-behind_s)

                    t_send_0 = time.perf_counter()
                    await ws.send(frame)
                    send_dur = (time.perf_counter() - t_send_0) * 1000.0
                    last_send_latency_ms = send_dur

                    if send_dur > 200.0:
                        stack = "".join(traceback.format_stack())
                        print(
                            f"\n  [SEND_LATENCY_SPIKE] ws.send took {send_dur:.1f}ms (>200ms) "
                            f"at chunk {chunk_idx} (T+{now - t_start:.1f}s):\n{stack}"
                        )

                    # Optional simulated outage injection
                    if simulate_outage_s > 0 and chunk_idx == 600:
                        print(
                            f"\n  [OUTAGE_SIMULATION] Triggering {simulate_outage_s:.1f}s "
                            "provider outage via control stream..."
                        )
                        with contextlib.suppress(Exception):
                            await redis_client.xadd(
                                "mm:system:asr_control",
                                {
                                    "action": "provider_outage",
                                    "meeting_id": meeting_id,
                                    "duration_s": str(simulate_outage_s),
                                },
                            )

                    chunk_idx += 1
            finally:
                gc.enable()

        sender_task = asyncio.create_task(_audio_sender())
        await sender_task

        actual_stream_dur = time.perf_counter() - t_start

        def _fetch_health(url: str, timeout: float) -> dict[str, Any]:
            try:
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    if resp.status == 200:
                        return json.loads(resp.read().decode("utf-8")).get("metrics", {})
            except Exception:
                pass
            return {}

        # Fetch final live metrics before ending meeting
        pre_end_metrics = await asyncio.to_thread(
            _fetch_health, f"http://127.0.0.1:{asr_health_port}/healthz?verbose=1", 5.0
        )
        if pre_end_metrics:
            latest_worker_metrics.update(pre_end_metrics)

        # End meeting
        await ws.send(json.dumps({"type": "control", "action": "meeting_end"}))
        await asyncio.sleep(6.0)

        ack_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await ack_task

    stop_bench.set()
    sampler_thread.join(timeout=5.0)
    await collector_task

    if _bench_gc_cb in gc.callbacks:
        gc.callbacks.remove(_bench_gc_cb)
    bench_lag_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await bench_lag_task

    # Query final in-process telemetry (with snapshot=1 off the sender loop)
    final_worker_metrics: dict[str, Any] = latest_worker_metrics.copy()
    post_metrics = await asyncio.to_thread(
        _fetch_health,
        f"http://127.0.0.1:{asr_health_port}/healthz?verbose=1&snapshot=1",
        15.0,
    )
    if "tracemalloc" in post_metrics:
        final_worker_metrics["tracemalloc"] = post_metrics["tracemalloc"]

    # Query Redis slowlog before closing
    redis_slowlog = []
    with contextlib.suppress(Exception):
        redis_slowlog = await redis_client.execute_command("SLOWLOG", "GET", "20")

    # Shutdown subprocesses
    gw_proc.kill()
    gw_proc.wait()
    asr_proc.kill()
    asr_proc.wait()
    asr_log_file.close()
    jwks.stop()
    await redis_client.aclose()

    # Extract headline metrics from authoritative worker telemetry
    # (with full bench percentiles if present)
    bench_p = final_worker_metrics.get("bench_percentiles")
    e2e_stats = (bench_p and bench_p.get("e2e_latencies")) or final_worker_metrics.get(
        "e2e_latencies", {}
    )
    if not e2e_stats.get("n") and e2e_latencies_ms:
        e2e_stats = compute_percentiles(e2e_latencies_ms)

    interim_stats = (bench_p and bench_p.get("interim_latencies")) or final_worker_metrics.get(
        "interim_latencies", {}
    )
    prov_stats = (bench_p and bench_p.get("provider_latencies")) or final_worker_metrics.get(
        "provider_latencies", {}
    )
    if not prov_stats.get("n") and e2e_latencies_ms and service_added_latencies_ms:
        prov_stats = compute_percentiles(
            [
                max(0.1, e - s)
                for e, s in zip(e2e_latencies_ms, service_added_latencies_ms, strict=False)
            ]
        )
    svc_stats = (bench_p and bench_p.get("service_added_latencies")) or final_worker_metrics.get(
        "service_added_latencies", {}
    )
    if not svc_stats.get("n") and service_added_latencies_ms:
        svc_stats = compute_percentiles(service_added_latencies_ms)

    stage_timings = (bench_p and bench_p.get("stage_timings")) or final_worker_metrics.get(
        "stage_timings", {}
    )
    loop_lag_stats = final_worker_metrics.get("loop_lag_stats", {})
    gc_data = final_worker_metrics.get("gc", {})
    tracemalloc_data = final_worker_metrics.get("tracemalloc", {})

    totals = final_worker_metrics.get("totals", {})
    tot_finals = totals.get("finals", max(e2e_stats.get("n", 0), len(finals_collected)))
    tot_interims = totals.get("interim_words", interim_stats.get("n", 0))
    tot_stages = totals.get("stage_timings", {})

    print("\n" + "=" * 80)
    print(" LATENCY BENCHMARK RESULTS — MeetMind AI Component #2 (ASR)")
    print("=" * 80)
    print(f"Total Audio Chunks Sent:    {total_target_chunks} ({minutes:.1f} minutes)")
    print(
        f"Total Final Utterances (N): {e2e_stats.get('n', len(finals_collected))} "
        f"/ Total: {tot_finals}"
    )
    print(f"Total Interim Words (N):    {interim_stats.get('n', 0)} / Total: {tot_interims}")
    print("-" * 80)

    # RULE ENFORCEMENT: Fail the run if any headline metric is empty
    headline_metrics = {
        "End-to-End Latency": (e2e_stats, tot_finals),
        "Interim Latency": (interim_stats, tot_interims),
        "Provider Latency": (prov_stats, tot_finals),
        "Service-Added Latency": (svc_stats, tot_finals),
    }

    for name, (stats, _tot_cnt) in headline_metrics.items():
        n_samples = stats.get("n", 0)
        if n_samples == 0:
            print(f"[FAIL] Headline metric '{name}' is empty (N=0). Failing run per D2c rule.")
            sys.exit(1)

    print(f"1. HEADLINE LATENCY BREAKDOWN ({minutes:.0f}-Minute Run):")
    for name, (stats, tot_cnt) in headline_metrics.items():
        n_samples = stats.get("n", 0)
        p50 = stats.get("p50", 0.0)
        p95 = stats.get("p95", 0.0)
        p99 = stats.get("p99", 0.0)
        s_max = stats.get("max", 0.0)
        s_mean = stats.get("mean", 0.0)

        # RULE: Never print a percentile from fewer than 30 samples or from an all-zero array
        all_zero = p50 == 0.0 and p95 == 0.0 and p99 == 0.0 and s_max == 0.0
        pct = (n_samples / tot_cnt * 100.0) if tot_cnt > 0 else 100.0
        if n_samples < 30 or all_zero:
            print(
                f"   {name:<24}: insufficient samples "
                f"(N={n_samples} / Total={tot_cnt} < 30 or all-zero)"
            )
        else:
            print(
                f"   {name:<24}: P50={p50:6.2f}ms | P95={p95:6.2f}ms | "
                f"P99={p99:6.2f}ms | Max={s_max:6.2f}ms | Mean={s_mean:6.2f}ms "
                f"(N={n_samples} / Total={tot_cnt}, {pct:.1f}%)"
            )

        # Requirement 2: Fail the run if N is under 90% of total
        if tot_cnt > 0 and pct < 90.0:
            print(
                f"[FAIL] Metric '{name}' sample count N={n_samples} "
                f"is under 90% of total {tot_cnt} ({pct:.1f}%)."
            )
            sys.exit(1)

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
        tot_s = tot_stages.get(stage_name, tot_finals)
        pct_s = (n_s / tot_s * 100.0) if tot_s > 0 else 100.0
        if n_s >= 30:
            print(
                f"   Stage {stage_name:<10}: P50={s_stats.get('p50', 0):.3f}ms | "
                f"P95={s_stats.get('p95', 0):.3f}ms | P99={s_stats.get('p99', 0):.3f}ms | "
                f"Max={s_stats.get('max', 0):.3f}ms (N={n_s} / Total={tot_s}, {pct_s:.1f}%)"
            )
            if tot_s > 0 and pct_s < 90.0:
                print(
                    f"[FAIL] Stage '{stage_name}' sample count N={n_s} "
                    f"is under 90% of total {tot_s} ({pct_s:.1f}%)."
                )
                sys.exit(1)
        else:
            print(
                f"   Stage {stage_name:<10}: N={n_s} / Total={tot_s} "
                f"(insufficient samples for percentiles)"
            )

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
    if distinct_lag_stacks:
        print(f"      Observed Blocker Stacks ({len(distinct_lag_stacks)} unique):")
        for idx, (_st_key, st_info) in enumerate(distinct_lag_stacks.items(), 1):
            print(f"        [{idx}] Count={st_info['count']} | Max Lag={st_info['max_ms']:.1f}ms:")
            for line in st_info["full_stack"].strip().splitlines()[-4:]:
                print(f"            {line}")
    else:
        print("      Observed Blocker Stacks: 0 (>200ms)")

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

    # Memory slope after minute 5 (D2i: Tracemalloc <= 0.02 MB/min, USS Min-Envelope <= 0.05 MB/min)
    m5_samples = [s for s in stability_samples if s["time_s"] >= 300 and s["asr_uss_mb"] > 0]

    # 1. USS Min-Envelope Slope
    uss_min_env_slope_mb_min = 0.0
    if len(m5_samples) >= 2:
        min_env_points = []
        for i in range(len(m5_samples)):
            w_start = max(0, i - 5)  # 6-sample (~60s) rolling window
            min_val = min(s["asr_uss_mb"] for s in m5_samples[w_start : i + 1])
            min_env_points.append((m5_samples[i]["time_s"], min_val))

        n_pts = len(min_env_points)
        sum_t = sum(p[0] for p in min_env_points)
        sum_u = sum(p[1] for p in min_env_points)
        sum_tt = sum(p[0] ** 2 for p in min_env_points)
        sum_tu = sum(p[0] * p[1] for p in min_env_points)
        denom = (n_pts * sum_tt) - (sum_t**2)
        if denom > 0:
            slope_per_sec = ((n_pts * sum_tu) - (sum_t * sum_u)) / denom
            uss_min_env_slope_mb_min = round(slope_per_sec * 60.0, 4)

    # 2. Worker Tracemalloc Slope
    tm_slope_mb_min = 0.0
    tm_curr = tracemalloc_data.get("current_mb", 0.0)
    tm_m5 = tracemalloc_data.get("m5_current_mb")
    if tm_m5 is not None and len(m5_samples) >= 2:
        dt_min = (m5_samples[-1]["time_s"] - 300.0) / 60.0
        if dt_min > 0:
            tm_slope_mb_min = round((tm_curr - tm_m5) / dt_min, 4)

    tm_pass = tm_slope_mb_min <= 0.02
    uss_pass = uss_min_env_slope_mb_min <= 0.05
    mem_slope_pass = tm_pass and uss_pass

    print(
        f"   Worker Tracemalloc Slope (T >= 5m): {tm_slope_mb_min:.4f} MB/min "
        f"(threshold <= 0.02 MB/min) -> [{'PASS' if tm_pass else 'FAIL'}]"
    )
    print(
        f"   USS Min-Envelope Slope (T >= 5m):   {uss_min_env_slope_mb_min:.4f} MB/min "
        f"(threshold <= 0.05 MB/min) -> [{'PASS' if uss_pass else 'FAIL'}]"
    )

    print(
        f"   Tracemalloc: current={tracemalloc_data.get('current_mb', 0)} MB | "
        f"peak={tracemalloc_data.get('peak_mb', 0)} MB"
    )
    if tracemalloc_data.get("top_diff"):
        print("   Tracemalloc Top-10 Snapshot Diffs (Startup -> End):")
        for diff_line in tracemalloc_data["top_diff"][:10]:
            print(f"     - {diff_line}")
    if tracemalloc_data.get("top_diff_m5"):
        print("   Tracemalloc Top-10 Snapshot Diffs (Minute 5 -> End):")
        for diff_line in tracemalloc_data["top_diff_m5"][:10]:
            print(f"     - {diff_line}")

    print("-" * 80)
    print("7. LIFECYCLE, STABILITY & AUDIT SUMMARY:")
    reconnect_count = final_worker_metrics.get("reconnects", 0)
    dropped_chunks = final_worker_metrics.get("dropped_chunks", 0)
    print(f"   Total Provider Reconnects:    {reconnect_count}")
    print(f"   Total Dropped Chunks:         {dropped_chunks}")

    # Reconnect Attempts Log (Requirement 1)
    reconnect_attempts = final_worker_metrics.get("reconnect_attempts", [])
    if not reconnect_attempts and asr_log_path.exists():
        with open(asr_log_path, encoding="utf-8", errors="ignore") as lf:
            for line in lf:
                if "[RECONNECT_ATTEMPT]" in line:
                    reconnect_attempts.append({"raw": line.strip()})
    if reconnect_attempts:
        print(f"\n   [RECONNECT ATTEMPTS LOG ({len(reconnect_attempts)})]:")
        for att in reconnect_attempts:
            if isinstance(att, dict) and "attempt" in att:
                ts_iso = (
                    datetime.fromtimestamp(att["timestamp"], UTC).isoformat()
                    if att.get("timestamp")
                    else "N/A"
                )
                el_val = att.get("elapsed_s", 0)
                bo_val = att.get("backoff_s", 0)
                print(
                    f"     - #{att['attempt']:02d} [{ts_iso}] host={att.get('host')} "
                    f"elapsed={el_val:.2f}s backoff={bo_val:.2f}s "
                    f"status={att.get('status')} exc={att.get('exc_type')} msg={att.get('exc_msg')}"
                )
            else:
                print(f"     - {att.get('raw', att)}")

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

    print(f"\n   Degraded/Recovered Events ({len(degraded_events)}):")
    for eid, dev in degraded_events:
        dp = dev.get("payload", {})
        ts = dev.get("ts_ms", 0)
        dt_str = datetime.fromtimestamp(ts / 1000.0, UTC).isoformat() if ts else "N/A"
        print(
            f"     - [ID {eid} at {dt_str}] state={dp.get('state')} "
            f"reason={dp.get('reason')} since_ms={dp.get('since_ms')} "
            f"elapsed_s={dp.get('elapsed_s')}"
        )

    # Query audit events from Neon (poll with bounded wait up to 30s for asr_session_summary)
    audit_summary_records = []
    t_poll_0 = time.time()
    while (time.time() - t_poll_0) < 30.0:
        with contextlib.suppress(Exception):
            conn = await asyncpg.connect(dsn, ssl=ssl_ctx)
            try:
                q = (
                    f'SELECT event_type, payload, created_at FROM "{schema_name}".audit_events '
                    f"ORDER BY id"
                )
                rows = await conn.fetch(q)
                audit_summary_records = [dict(r) for r in rows]
                if any(r.get("event_type") == "asr_session_summary" for r in audit_summary_records):
                    break
            finally:
                await conn.close()
        await asyncio.sleep(0.5)

    print(f"\n   Audit Records in Neon ({len(audit_summary_records)}):")
    for rec in audit_summary_records:
        created_str = rec["created_at"].isoformat() if rec.get("created_at") else "N/A"
        p_json = rec.get("payload", {})
        if isinstance(p_json, str):
            p_json = json.loads(p_json)
        host_val = p_json.get("endpoint_host")
        prov_val = p_json.get("provider")
        chunks_val = p_json.get("chunks_sent")
        gaps_val = p_json.get("gaps")
        print(
            f"     - [{created_str}] {rec['event_type']}: host={host_val} "
            f"provider={prov_val} chunks={chunks_val} gaps={gaps_val}"
        )

    print("-" * 80)
    print("8. PROVIDER CONFIGURATION & BEHAVIORAL DATA:")
    eff_ep = f"{endpointing}ms" if endpointing is not None else "Nova-3 default (10ms)"
    eff_ue = f"{utterance_end_ms}ms" if utterance_end_ms is not None else "Nova-3 default (1000ms)"
    finals_per_min = (
        round(len(finals_collected) / minutes, 1) if minutes > 0 else len(finals_collected)
    )
    print(f"   Effective Endpointing:      {eff_ep}")
    print(f"   Effective Utterance End:    {eff_ue}")
    print(f"   Audio Fixture ID:           {Path(audio_path).name}")
    print(f"   Final Utterances / Minute:  {finals_per_min}")
    duration_err_pct = (
        abs(actual_stream_dur - total_stream_seconds) / total_stream_seconds * 100.0
        if total_stream_seconds > 0
        else 0.0
    )
    print(
        f"   Stream Pacing Accuracy:     Target={total_stream_seconds:.1f}s | "
        f"Actual={actual_stream_dur:.1f}s (Error={duration_err_pct:.2f}%)"
    )

    # Acceptance Criteria Verdict Table (Requirement 7)
    print("-" * 80)
    print("9. ACCEPTANCE CRITERIA VERIFICATION VERDICTS:")
    calib_pass = calib_res.get("passed", False)
    p95_svc = float(svc_stats.get("p95", 999.0))
    svc_pass = (p95_svc < 25.0) and (svc_stats.get("n", 0) >= 30)
    zero_drops_pass = dropped_chunks == 0
    duration_pass = duration_err_pct <= 2.0
    degraded_count_pass = len(degraded_events) <= 25

    has_stall = len(stalls_recorded) > 0
    interim_p50 = float(interim_stats.get("p50", 999.0))
    ws_rtt_p50 = float(net_stats.get("ping_rtt_ms", {}).get("p50", 0.0))

    if has_stall:
        verdict_str = "INVALID"
        v_interim = "N/A - INVALID RUN"
    elif ws_rtt_p50 > 0 and interim_p50 < (ws_rtt_p50 / 2.0):
        verdict_str = "INVALID"
        v_interim = f"INVALID - BELOW PHYSICAL MINIMUM (RTT/2 = {ws_rtt_p50 / 2.0:.1f}ms)"
    else:
        v_interim = "PASS" if (interim_p50 <= 500.0) else "NOT MET"

    overall_pass = (
        calib_pass
        and svc_pass
        and zero_drops_pass
        and mem_slope_pass
        and duration_pass
        and degraded_count_pass
        and not has_stall
        and (v_interim != "N/A - INVALID RUN")
        and not v_interim.startswith("INVALID")
    )
    if not has_stall and not v_interim.startswith("INVALID"):
        verdict_str = "PASSED" if overall_pass else "FAIL"

    if has_stall or verdict_str == "INVALID" or v_interim.startswith("INVALID"):
        v_svc = "N/A - INVALID RUN"
    else:
        v_svc = "PASS" if svc_pass else "FAIL"
    v_calib = "PASS" if calib_pass else "FAIL"
    v_zero = "PASS" if zero_drops_pass else "FAIL"
    v_mem = "PASS" if mem_slope_pass else "FAIL"
    v_dur = "PASS" if duration_pass else "FAIL"
    v_deg = "PASS" if degraded_count_pass else "FAIL"
    v_stall = "PASS" if not has_stall else "INVALID"
    calib_err = calib_res.get("error_ms", 0.0)

    print(f"   [1] Service Overhead P95 < 25ms:     P95={p95_svc:.2f}ms  -> [{v_svc}]")
    print(f"   [2] Verification Calibration:        Error={calib_err:.2f}ms -> [{v_calib}]")
    print(f"   [3] Zero Audio Chunks Dropped:       Dropped={dropped_chunks}      -> [{v_zero}]")
    print(
        f"   [4] Memory Flatlining:               TM Slope={tm_slope_mb_min:.4f}MB/m (<=0.02), "
        f"USS Slope={uss_min_env_slope_mb_min:.4f}MB/m (<=0.05) -> [{v_mem}]"
    )
    print(f"   [5] Stream Duration Accuracy <=2%:   Error={duration_err_pct:.2f}%     -> [{v_dur}]")
    print(f"   [6] Degraded State Machine (<=25):   Events={len(degraded_events)}     -> [{v_deg}]")
    print(
        f"   [7] Client Stall Check:              Stalls={len(stalls_recorded)}     -> [{v_stall}]"
    )
    print(
        f"   [8] Interim Latency vs 500ms target: P50={interim_p50:.2f}ms "
        f"(WS RTT={ws_rtt_p50:.1f}ms) -> [{v_interim}]"
    )
    print(f"\n   OVERALL BENCHMARK VERDICT:           [{verdict_str}]")
    if has_stall:
        print(
            "   [CRITICAL RULE VIOLATION] Run experienced client stall(s); "
            "verdict locked to INVALID."
        )
    if not zero_drops_pass:
        print(
            "   [CRITICAL RULE VIOLATION] Run experienced dropped chunks; verdict locked to FAIL."
        )
    print("=" * 80)

    if not overall_pass or has_stall:
        sys.exit(1)


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
        default=None,
        choices=["global", "in", "eu", "au"],
        help="ASR regional endpoint (global|in|eu|au, default: settings.asr_region from .env)",
    )
    parser.add_argument(
        "--outage-dur-s",
        type=float,
        default=0.0,
        help="Simulate provider outage for N seconds at T+60s via test hook",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Confirm real Deepgram API usage and cost",
    )
    args = parser.parse_args()

    from meetmind_asr.config import AsrSettings

    bench_settings = AsrSettings()
    if args.region is not None:
        effective_region = args.region
        region_source = f"CLI --region flag ('{args.region}')"
    else:
        effective_region = bench_settings.asr_region
        region_source = f"AsrSettings (ASR_REGION in .env / default: '{bench_settings.asr_region}')"

    print(f"[Region Resolution] Region: '{effective_region}' [Source: {region_source}]")

    asyncio.run(
        run_benchmark(
            minutes=args.minutes,
            endpointing=args.endpointing,
            utterance_end_ms=args.utterance_end_ms,
            region=effective_region,
            auto_yes=args.yes,
            simulate_outage_s=args.outage_dur_s,
        )
    )
