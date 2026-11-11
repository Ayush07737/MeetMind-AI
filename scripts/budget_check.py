"""MeetMind AI — End-to-End Latency Budget Checker (Q2).

Runs an end-to-end simulated pipeline against real Gateway and ASR (with StubProvider),
simulating downstream intelligence stages according to docs/latency/budget.yaml allocations.
Verifies:
1. All pipeline stage timestamps are present and strictly monotonic.
2. MeetMind AI's own service overhead per stage is <= contract allowance (25.0ms default).
3. Prints a structured data table of stage percentiles (N, P50, P95).

Also supports validating historical or test benchmark logs against budget thresholds.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
import urllib.request
import uuid
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

# Ensure required module paths are on sys.path
for p in [
    REPO_ROOT / "backend" / "gateway" / "tests" / "acceptance",
    REPO_ROOT / "backend" / "gateway",
    REPO_ROOT / "backend" / "asr",
    REPO_ROOT / "backend" / "security",
    REPO_ROOT / "packages" / "shared-schemas" / "schemas_py",
]:
    sp = str(p)
    if sp not in sys.path:
        sys.path.insert(0, sp)

import asyncpg  # noqa: E402
import websockets  # noqa: E402
from conftest import LocalJWKSServer, exchange_ws_ticket, get_free_port  # noqa: E402
from meetmind_schemas.budget import load_budget  # noqa: E402
from meetmind_schemas.events import encode_audio_frame  # noqa: E402
from meetmind_schemas.keys import (  # noqa: E402
    ACTIVE_MEETINGS_SET_KEY,
    events_stream_key,
    out_stream_key,
)  # noqa: E402
from meetmind_schemas.timing import ClockEstimator, stamp, validate_monotonic  # noqa: E402
from meetmind_security import db  # noqa: E402
from meetmind_security.migrations import apply_migrations_conn  # noqa: E402
from meetmind_security.tenant import TenantConfig  # noqa: E402

if sys.platform == "win32":
    with contextlib.suppress(Exception):
        import ctypes

        ctypes.windll.winmm.timeBeginPeriod(1)

logger = logging.getLogger("budget_check")


def compute_percentiles(vals: list[float]) -> dict[str, float]:
    if not vals:
        return {"p50": 0.0, "p95": 0.0, "max": 0.0, "mean": 0.0}
    s = sorted(vals)
    n = len(s)
    return {
        "p50": round(s[int(n * 0.50)], 2),
        "p95": round(s[min(n - 1, int(n * 0.95))], 2),
        "max": round(s[-1], 2),
        "mean": round(sum(s) / n, 2),
    }


def check_benchmark_log(
    log_path: Path | str,
    budget_path: Path | str | None = None,
    verbose: bool = True,
) -> bool:
    """Validate parsed metrics in a benchmark log against docs/latency/budget.yaml."""
    budget = load_budget(budget_path)
    max_own_overhead = budget.constraints.overhead.max_own_overhead_per_stage_ms
    target_p95_finals = budget.stages["stable_trigger_text"].target_p95_ms

    p = Path(log_path)
    if not p.exists():
        if verbose:
            print(f"[FAIL] Benchmark log not found: {p}")
        return False

    raw_bytes = p.read_bytes()
    if raw_bytes.startswith((b"\xff\xfe", b"\xfe\xff")):
        content = raw_bytes.decode("utf-16", errors="replace")
    else:
        content = raw_bytes.decode("utf-8", errors="replace")

    m_svc = re.search(
        r"Service-Added Latency\s*:\s*P50=\s*([0-9.]+)ms\s*\|\s*P95=\s*([0-9.]+)ms",
        content,
    )
    m_e2e = re.search(
        r"End-to-End Latency\s*:\s*P50=\s*([0-9.]+)ms\s*\|\s*P95=\s*([0-9.]+)ms",
        content,
    )

    if not m_svc:
        if verbose:
            print("[FAIL] Could not parse 'Service-Added Latency' from benchmark log")
        return False
    if not m_e2e:
        if verbose:
            print("[FAIL] Could not parse 'End-to-End Latency' from benchmark log")
        return False

    svc_p50 = float(m_svc.group(1))
    svc_p95 = float(m_svc.group(2))
    e2e_p50 = float(m_e2e.group(1))
    e2e_p95 = float(m_e2e.group(2))

    passed_svc = svc_p95 <= max_own_overhead
    passed_finals = e2e_p95 <= target_p95_finals
    overall = passed_svc and passed_finals

    if verbose:
        print("=" * 80)
        print(f" BENCHMARK LOG BUDGET VERIFICATION: {p.name}")
        print(" [Clock Mode: ASR-receipt clock]")
        print(" NOTE: Latencies in benchmark log measure from chunk arrival at ASR worker.")
        print("       Budget targets are defined from client T0 (capture time on server clock).")
        print("       Log-mode check leaves ~70ms headroom for client-to-ASR transit legs.")
        print("=" * 80)
        print(
            f"Service-Added Latency P50: {svc_p50:.2f}ms | P95: {svc_p95:.2f}ms "
            f"(Ceiling: <= {max_own_overhead:.1f}ms) -> {'PASS' if passed_svc else 'FAIL'}"
        )
        print(
            f"Finals Latency (ASR Clock) P50: {e2e_p50:.2f}ms | P95: {e2e_p95:.2f}ms "
            f"(Ceiling: <= {target_p95_finals:.1f}ms) -> {'PASS' if passed_finals else 'FAIL'}"
        )
        print("-" * 80)
        print(f"OVERALL RESULT: {'PASS' if overall else 'FAIL'}")
        print("=" * 80)

    return overall


async def run_budget_check(
    budget_path: Path | str | None = None,
    audio_chunks: int = 1000,
    target_utterances: int = 200,
    redis_url: str = "redis://127.0.0.1:6379/0",
    client_clock_offset_ms: int = 2500,
    verbose: bool = True,
) -> bool:
    budget = load_budget(budget_path)
    sim = budget.simulated
    max_own_overhead = budget.constraints.overhead.max_own_overhead_per_stage_ms

    tenant_id = f"bcheck_{uuid.uuid4().hex[:6]}"
    schema_name = f"tenant_{tenant_id}"
    meeting_id = f"mtg_bc_{uuid.uuid4().hex[:6]}"
    user_id = "user_bcheck_tester"

    cfg = TenantConfig(
        tenant_id=tenant_id,
        pg_schema=schema_name,
        neo4j_namespace=tenant_id,
        qdrant_prefix=tenant_id,
        redis_prefix=tenant_id,
        asr_region="in",
        residency_mode="strict",
    )
    db._router.register(cfg)

    # Provision schema in Neon
    dsn = db.get_owner_dsn()
    ssl_ctx = db._create_ssl_context(dsn)
    for conn_att in range(1, 4):
        try:
            conn = await asyncpg.connect(dsn, ssl=ssl_ctx)
            try:
                await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
                await apply_migrations_conn(conn, schema_name)
                break
            finally:
                await conn.close()
        except Exception:
            if conn_att == 3:
                raise
            await asyncio.sleep(1.0)

    # Start JWKS server
    jwks_port = get_free_port()
    jwks = LocalJWKSServer(jwks_port)
    jwks.start()

    gw_port = get_free_port()
    asr_health_port = get_free_port()

    # Launch Gateway
    app_dsn = (
        os.environ.get("NEON_TEST_APP_DATABASE_URL")
        or os.environ.get("NEON_APP_DATABASE_URL")
        or db.get_app_dsn()
    )
    gw_env = os.environ.copy()
    gw_env["APP_ENV"] = "test"
    gw_env["REDIS_URL"] = redis_url
    gw_env["AUDIO_REDIS_URL"] = redis_url
    gw_env["CLERK_JWKS_URL"] = jwks.jwks_url
    gw_env["CLERK_ISSUER"] = jwks.issuer
    gw_env["ALLOWED_ORIGINS"] = json.dumps(["https://app.meetmind.ai", "http://localhost:3000"])
    gw_env["TENANT_ORG_MAP"] = json.dumps({"org_default": tenant_id})
    gw_env["ELEVATED_LISTEN_ROLES"] = json.dumps(["org:admin"])
    gw_env["NEON_DATABASE_URL"] = app_dsn
    gw_env["NEON_APP_DATABASE_URL"] = app_dsn
    gw_env["ALLOW_TOKEN_IN_DEV"] = "false"
    gw_env["FRAME_RATE_LIMIT"] = "1500"
    gw_env["BURST_FRAME_LIMIT"] = "2500"
    gw_env["BYTE_RATE_LIMIT"] = str(10 * 1024 * 1024)
    gw_env["BURST_BYTE_LIMIT"] = str(20 * 1024 * 1024)
    gw_env["CLOCK_MAX_AGE_MS"] = "300000"
    gw_env["LOG_LEVEL"] = "INFO"
    gw_env["PYTHONPATH"] = (
        str(REPO_ROOT / "backend" / "gateway")
        + os.pathsep
        + str(REPO_ROOT / "backend" / "security")
        + os.pathsep
        + str(REPO_ROOT / "packages" / "shared-schemas" / "schemas_py")
        + os.pathsep
        + gw_env.get("PYTHONPATH", "")
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
            "--log-level",
            "warning",
        ],
        env=gw_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Launch ASR Worker with StubProvider
    asr_env = os.environ.copy()
    asr_env["APP_ENV"] = "development"
    asr_env["REDIS_URL"] = redis_url
    asr_env["AUDIO_REDIS_URL"] = redis_url
    asr_env["ASR_PROVIDER"] = "stub"
    asr_env["ASR_BENCHMARK_MODE"] = "1"
    asr_env["ASR_STUB_FAULT_CONFIG"] = json.dumps({"latency_ms": 0.0})
    asr_env["ASR_HEALTH_PORT"] = str(asr_health_port)
    asr_env["NEON_DATABASE_URL"] = app_dsn
    asr_env["PYTHONPATH"] = (
        str(REPO_ROOT / "backend" / "asr")
        + os.pathsep
        + str(REPO_ROOT / "backend" / "security")
        + os.pathsep
        + str(REPO_ROOT / "packages" / "shared-schemas" / "schemas_py")
        + os.pathsep
        + asr_env.get("PYTHONPATH", "")
    )

    asr_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "meetmind_asr",
        ],
        env=asr_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    gw_logs: list[str] = []
    asr_logs: list[str] = []

    def _drain_gw(proc_stream):
        try:
            for line in iter(proc_stream.readline, b""):
                dec = line.decode("utf-8", errors="replace").strip()
                if dec:
                    gw_logs.append(dec)
        except Exception:
            pass

    def _drain_asr(proc_stream):
        try:
            for line in iter(proc_stream.readline, b""):
                dec = line.decode("utf-8", errors="replace").strip()
                if dec:
                    asr_logs.append(dec)
        except Exception:
            pass

    threading.Thread(target=_drain_gw, args=(gw_proc.stdout,), daemon=True).start()
    threading.Thread(target=_drain_gw, args=(gw_proc.stderr,), daemon=True).start()
    threading.Thread(target=_drain_asr, args=(asr_proc.stdout,), daemon=True).start()
    threading.Thread(target=_drain_asr, args=(asr_proc.stderr,), daemon=True).start()

    import redis.asyncio as aioredis

    redis_client = aioredis.from_url(redis_url, decode_responses=False)

    # Wait for gateway and ASR worker health
    t0_wait = time.time()
    gw_ready = False
    asr_ready = False
    while time.time() - t0_wait < 15.0:
        if not gw_ready:
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{gw_port}/health", timeout=1.0
                ) as resp:
                    if resp.status == 200:
                        gw_ready = True
            except Exception:
                pass
        if not asr_ready:
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{asr_health_port}/healthz", timeout=1.0
                ) as resp:
                    if resp.status == 200:
                        asr_ready = True
            except Exception:
                pass
        if gw_ready and asr_ready:
            break
        await asyncio.sleep(0.3)

    if not (gw_ready and asr_ready):
        gw_proc.terminate()
        asr_proc.terminate()
        jwks.stop()
        await redis_client.aclose()
        raise RuntimeError("Services failed to become healthy within 15 seconds")

    token = jwks.mint_token(sub=user_id, org_id="org_default", org_role="org:admin")
    status, ticket_resp = await exchange_ws_ticket(
        f"http://127.0.0.1:{gw_port}", token, meeting_id, role="ingest"
    )
    assert status == 200, f"Ticket exchange failed: {ticket_resp}"
    ticket = ticket_resp["ticket"]

    ws_uri = f"ws://127.0.0.1:{gw_port}/v1/meetings/{meeting_id}/stream?ticket={ticket}"
    events_key = events_stream_key(tenant_id, meeting_id)
    out_key = out_stream_key(tenant_id, meeting_id)

    processed_events: list[dict[str, Any]] = []
    interim_events: list[dict[str, Any]] = []
    stage_durations: dict[str, list[float]] = {
        "gw_overhead": [],
        "asr_recv_overhead": [],
        "asr_service_overhead": [],
        "classify_overhead": [],
        "retrieval_overhead": [],
        "card_overhead": [],
        "t0_to_interim": [],
        "t0_to_final": [],
        "total_e2e": [],
    }

    target_finals = target_utterances
    pending_tasks: set[asyncio.Task] = set()

    async def _handle_final_downstream(env: dict[str, Any]):
        # Stage 6: Intent Classification (actual minus requested sleep)
        t0 = time.perf_counter()
        await asyncio.sleep(sim.classifier_delay_ms / 1000.0)
        cls_overhead = max(0.0, (time.perf_counter() - t0) * 1000.0 - sim.classifier_delay_ms)
        stage_durations["classify_overhead"].append(cls_overhead)
        stamp(env, "classify_pub")

        # Stage 7: Vector / Graph Retrieval (actual minus requested sleep)
        t0 = time.perf_counter()
        await asyncio.sleep(sim.retrieval_delay_ms / 1000.0)
        ret_overhead = max(0.0, (time.perf_counter() - t0) * 1000.0 - sim.retrieval_delay_ms)
        stage_durations["retrieval_overhead"].append(ret_overhead)
        stamp(env, "retrieval_done")

        # Stage 8: Card Publication (actual minus requested sleep)
        t0 = time.perf_counter()
        await asyncio.sleep(sim.card_publisher_overhead_ms / 1000.0)
        card_overhead = max(
            0.0, (time.perf_counter() - t0) * 1000.0 - sim.card_publisher_overhead_ms
        )
        stage_durations["card_overhead"].append(card_overhead)
        stamp(env, "card_pub")

        # Client Render
        stamp(env, "ui_render")
        processed_events.append(env)

    async def _event_consumer():
        last_id = "0-0"
        while len(processed_events) < target_finals:
            res = await redis_client.xread({events_key: last_id}, count=50, block=100)
            if not res:
                continue
            for _stream, msgs in res:
                for eid, fields in msgs:
                    last_id = eid.decode() if isinstance(eid, bytes) else str(eid)
                    payload_raw = fields.get(b"payload") or fields.get("payload")
                    if payload_raw:
                        env = json.loads(
                            payload_raw.decode("utf-8")
                            if isinstance(payload_raw, bytes)
                            else payload_raw
                        )
                        if env.get("event_type") == "transcript_final":
                            t = asyncio.create_task(_handle_final_downstream(env))
                            pending_tasks.add(t)
                            t.add_done_callback(pending_tasks.discard)

    async def _interim_consumer():
        last_id = "0-0"
        while len(processed_events) < target_finals:
            res = await redis_client.xread({out_key: last_id}, count=50, block=100)
            if not res:
                continue
            for _stream, msgs in res:
                for eid, fields in msgs:
                    last_id = eid.decode() if isinstance(eid, bytes) else str(eid)
                    payload_raw = fields.get(b"payload") or fields.get("payload")
                    if payload_raw:
                        env = json.loads(
                            payload_raw.decode("utf-8")
                            if isinstance(payload_raw, bytes)
                            else payload_raw
                        )
                        if env.get("event_type") == "transcript_interim":
                            interim_events.append(env)
                            timing = env.get("timing", {})
                            t0 = timing.get("t0_capture")
                            pub = timing.get("asr_pub")
                            if t0 and pub:
                                stage_durations["t0_to_interim"].append(max(0.0, float(pub - t0)))

    consumer_task = asyncio.create_task(_event_consumer())
    interim_task = asyncio.create_task(_interim_consumer())

    try:
        async with websockets.connect(ws_uri) as ws:
            # 1. meeting_start
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

            # 2. clock_sync handshake (3 probes per clocks.md §3.1 step 4, simulated +2.5s)
            probes: list[dict[str, int]] = []
            for _ in range(3):
                client_t1 = int(time.time() * 1000) + client_clock_offset_ms
                await ws.send(
                    json.dumps({"type": "control", "action": "clock_sync", "t1": client_t1})
                )
                sync_reply = json.loads(await ws.recv())
                client_t4 = int(time.time() * 1000) + client_clock_offset_ms
                probes.append(
                    ClockEstimator.compute_probe(
                        client_t1,
                        sync_reply["t2"],
                        sync_reply["t3"],
                        client_t4,
                    )
                )
            best_probe = ClockEstimator.select_best_probe(probes)

            # 3. clock_report
            await ws.send(
                json.dumps(
                    {
                        "type": "control",
                        "action": "clock_report",
                        "offset_ms": best_probe["offset_ms"],
                        "rtt_ms": best_probe["rtt_ms"],
                        "samples": len(probes),
                    }
                )
            )
            await ws.recv()

            # 4. consent_confirmed
            await ws.send(
                json.dumps(
                    {
                        "type": "control",
                        "action": "consent_confirmed",
                        "consent_type": "audio_capture",
                        "external_participants": False,
                        "consent_text_version": "v1.0",
                        "client_version": "1.0.0",
                    }
                )
            )
            await ws.recv()

            # 5. Stream audio chunks (send enough chunks to trigger target_finals)
            chunks_to_send = max(audio_chunks, target_finals * 5)

            async def _send_chunks():
                for i in range(chunks_to_send):
                    t_capture = int(time.time() * 1000) + client_clock_offset_ms
                    frame = encode_audio_frame(
                        client_seq=i + 1,
                        capture_ts_ms=t_capture,
                        payload=b"\x00" * 3200,
                    )
                    await ws.send(frame)
                    await asyncio.sleep(0.08)

            async def _recv_acks():
                acks_received = 0
                while acks_received < chunks_to_send:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if data.get("frame_type") == "audio_chunk" and data.get("status") == "acked":
                        acks_received += 1

            await asyncio.gather(_send_chunks(), _recv_acks())

            # Wait for consumer to finish processing finals
            try:
                await asyncio.wait_for(consumer_task, timeout=60.0)
                if pending_tasks:
                    await asyncio.gather(*pending_tasks, return_exceptions=True)
            except TimeoutError:
                print(
                    f"\n[TIMEOUT] Consumer timed out! Processed {len(processed_events)}/"
                    f"{target_finals} finals"
                )
                print("--- GATEWAY LOGS ---")
                for line_msg in gw_logs[-25:]:
                    print(line_msg)
                print("--- ASR LOGS ---")
                for line_msg in asr_logs[-25:]:
                    print(line_msg)
                print("--------------------\n")
                raise

            # meeting_end
            await ws.send(json.dumps({"type": "control", "action": "meeting_end"}))
            await ws.recv()

    finally:
        consumer_task.cancel()
        interim_task.cancel()
        for p in (gw_proc, asr_proc):
            p.terminate()
            try:
                p.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        jwks.stop()
        with contextlib.suppress(Exception):
            keys = await redis_client.keys(f"mm:{tenant_id}:*")
            if keys:
                await redis_client.delete(*keys)
            await redis_client.srem(ACTIVE_MEETINGS_SET_KEY, f"{tenant_id}|{meeting_id}")
            await redis_client.srem(ACTIVE_MEETINGS_SET_KEY, f"{tenant_id}:{meeting_id}")
        await redis_client.aclose()

    # Analyze results
    all_monotonic = True
    monotonic_errors = []

    for env in processed_events:
        timing = env.get("timing", {})
        errs = validate_monotonic(timing)
        if errs:
            all_monotonic = False
            monotonic_errors.extend(errs)

        t0 = timing.get("t0_capture")
        gw_r = timing.get("gw_recv")
        asr_r = timing.get("asr_recv")
        prov_m = timing.get("provider_msg")
        asr_p = timing.get("asr_pub")
        ui_r = timing.get("ui_render")

        if t0 and gw_r:
            stage_durations["gw_overhead"].append(max(0.0, float(gw_r - t0)))
        if gw_r and asr_r:
            stage_durations["asr_recv_overhead"].append(max(0.0, float(asr_r - gw_r)))
        if prov_m and asr_p:
            stage_durations["asr_service_overhead"].append(max(0.0, float(asr_p - prov_m)))
        if t0 and asr_p:
            stage_durations["t0_to_final"].append(max(0.0, float(asr_p - t0)))
        if t0 and ui_r:
            stage_durations["total_e2e"].append(max(0.0, float(ui_r - t0)))

    # Compute percentiles
    stats_map = {stage: compute_percentiles(lats) for stage, lats in stage_durations.items()}

    all_overhead_passed = True
    if verbose:
        print("=" * 105)
        print(" MEETMIND AI — END-TO-END LATENCY BUDGET VERIFICATION (Q2)")
        print(" [Clock Mode: T0 True Capture Clock via SNTP Sync (Simulated Client Clock +2.5s)]")
        print("=" * 105)
        print(f"Monotonic Stamps Valid: {all_monotonic}")
        print(f"Total Finals Processed: {len(processed_events)} / Target={target_finals}")
        if monotonic_errors:
            for e in monotonic_errors:
                print(f"  [ERROR] {e}")
        print("-" * 105)
        hdr = (
            f"{'Stage / Interval':<24} | {'N':<4} | {'P50 (ms)':<9} | "
            f"{'P95 (ms)':<9} | {'Max (ms)':<9} | {'Ceiling':<10} | {'Headroom':<11} | {'Met?'}"
        )
        print(hdr)
        print("-" * 105)

    stage_ceilings = {
        "gw_overhead": max_own_overhead,
        "asr_recv_overhead": max_own_overhead,
        "asr_service_overhead": max_own_overhead,
        "classify_overhead": max_own_overhead,
        "retrieval_overhead": max_own_overhead,
        "card_overhead": max_own_overhead,
        "t0_to_interim": 2000.0,
        "t0_to_final": 2000.0,
        "total_e2e": 3500.0,
    }

    for stage, s in stats_map.items():
        if not stage_durations[stage]:
            continue
        ceiling = stage_ceilings.get(stage, max_own_overhead)
        passed = s["p95"] <= ceiling
        headroom = ceiling - s["p95"]
        headroom_str = f"{headroom:+.1f} ms"
        if not passed:
            all_overhead_passed = False

        if verbose:
            print(
                f"{stage:<24} | {len(stage_durations[stage]):<4} | "
                f"{s['p50']:<9.2f} | {s['p95']:<9.2f} | {s['max']:<9.2f} | "
                f"<= {ceiling:<7.1f} | {headroom_str:<11} | {str(passed):<5}"
            )

    if verbose:
        print("=" * 105)
        status_str = "PASS" if (all_monotonic and all_overhead_passed) else "FAIL"
        print(f"OVERALL RESULT: {status_str}")
        print("=" * 105)

    return all_monotonic and all_overhead_passed


def main():
    parser = argparse.ArgumentParser(description="MeetMind AI Latency Budget Checker")
    parser.add_argument("--budget", type=str, default=None, help="Path to budget.yaml")
    parser.add_argument(
        "--chunks",
        type=int,
        default=1000,
        help="Number of 100ms chunks to stream (default 1000)",
    )
    parser.add_argument(
        "--target-utterances",
        type=int,
        default=200,
        help="Target number of final utterances (default 200)",
    )
    parser.add_argument("--redis-url", type=str, default="redis://127.0.0.1:6379/0")
    parser.add_argument("--log", type=str, default=None, help="Path to benchmark log to verify")
    args = parser.parse_args()

    if args.log:
        passed = check_benchmark_log(
            log_path=args.log,
            budget_path=args.budget,
            verbose=True,
        )
    else:
        passed = asyncio.run(
            run_budget_check(
                budget_path=args.budget,
                audio_chunks=args.chunks,
                target_utterances=args.target_utterances,
                redis_url=args.redis_url,
                verbose=True,
            )
        )
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
