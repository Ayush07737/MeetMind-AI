"""Acceptance Test B13: Audit Summary Durability with Artificially Slow DB.

Verifies:
1. When Neon database operations are artificially delayed by 2.0s per statement,
   the `asr_session_summary` row lands synchronously in Neon before the worker exits.
2. When the worker receives a SIGTERM / graceful termination signal while an active
   meeting is mid-stream, the worker flushes the summary synchronously (with 2.0s DB delay)
   before the worker process finishes terminating.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket, get_free_port, make_pcm16_chunk
from meetmind_asr.config import AsrSettings
from meetmind_asr.service import AsrService
from meetmind_security import db

pytestmark = [pytest.mark.integration, pytest.mark.timeout(180)]


@pytest.fixture(autouse=True)
async def reset_db_pool():
    """Ensure database connection pool is clean and bound to each test's event loop."""
    try:
        await db.close_db()
    except Exception:
        db._shared_pool = None
    yield
    try:
        await db.close_db()
    except Exception:
        db._shared_pool = None


@pytest.mark.asyncio
async def test_b13_audit_durability_slow_db(
    gateway_harness: ServerHarness,
    real_redis,
) -> None:
    """Verify asr_session_summary lands before worker exit under 2s slow DB statement delay."""
    schema = gateway_harness.schema_a
    meeting_id = f"mtg_b13_slow_{uuid.uuid4().hex[:8]}"

    # Configure artificial 2.0s delay per audit statement via environment
    os.environ["TEST_AUDIT_STATEMENT_DELAY_SECONDS"] = "2.0"
    try:
        settings = AsrSettings(
            _env_file=None,
            ASR_PROVIDER="stub",
            ASR_HEALTH_PORT=get_free_port(),
            REDIS_URL=str(
                real_redis.connection_pool.connection_kwargs.get("url", "redis://127.0.0.1:6379/0")
            ),
        )
        worker = AsrService(settings, worker_id="w_b13_slow")
        await worker.start()

        admin_token = gateway_harness.jwks.mint_token(
            sub="admin_b13", org_id=gateway_harness.org_a, org_role="org:admin"
        )
        _, ingest_ticket_resp = await exchange_ws_ticket(
            gateway_harness.http_url, admin_token, meeting_id, role="ingest"
        )
        ingest_ticket = ingest_ticket_resp["ticket"]
        ingest_uri = (
            f"{gateway_harness.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ingest_ticket}"
        )

        async with websockets.connect(ingest_uri) as ws_ingest:

            async def recv_ack():
                while True:
                    msg = json.loads(await ws_ingest.recv())
                    if msg.get("type") == "ack":
                        return msg

            await ws_ingest.send(
                json.dumps(
                    {
                        "type": "control",
                        "action": "meeting_start",
                        "adapter_type": "chrome_extension",
                    }
                )
            )
            assert (await recv_ack())["status"] == "accepted"

            await ws_ingest.send(
                json.dumps(
                    {
                        "type": "control",
                        "action": "consent_confirmed",
                        "consent_type": "audio_capture",
                        "external_participants": True,
                        "jurisdiction_hint": "GDPR",
                        "consent_text_version": "v2.0",
                        "client_version": "2.1.0",
                    }
                )
            )
            assert (await recv_ack())["status"] == "accepted"

            # Stream audio chunks
            t_base = 12_000_000
            for seq in range(1, 6):
                frame = make_pcm16_chunk(seq=seq, ts_ms=t_base + seq * 100)
                await ws_ingest.send(frame)
                ack = await recv_ack()
                assert ack.get("status") in ("accepted", "acked")
                await asyncio.sleep(0.04)

            # Ensure worker has activated pipeline before meeting_end
            for _ in range(50):
                if meeting_id in worker._active_pipelines:
                    break
                await asyncio.sleep(0.1)
            assert meeting_id in worker._active_pipelines, "Worker did not activate pipeline"

            # End meeting cleanly
            await ws_ingest.send(json.dumps({"type": "control", "action": "meeting_end"}))
            assert (await recv_ack())["status"] == "accepted"

        # Stop worker. Graceful worker.stop() awaits all in-flight lifecycle stops
        # and flushes summary
        t_stop_start = time.perf_counter()
        await worker.stop()
        t_stop_dur = time.perf_counter() - t_stop_start

        # Verify that synchronous stop waited at least 2.0s for the slow DB statement
        assert t_stop_dur >= 1.9, (
            f"Expected stop duration >= 2.0s due to slow DB, got {t_stop_dur:.2f}s"
        )

        pool = await db.get_pool()
        rows = await pool.fetch(
            f"SELECT event_type, payload FROM {schema}.audit_events "
            f"WHERE payload->>'meeting_id' = $1 AND event_type = 'asr_session_summary'",
            meeting_id,
        )
        assert len(rows) == 1, f"Expected 1 asr_session_summary row in Neon, found {len(rows)}"
        p = (
            json.loads(rows[0]["payload"])
            if isinstance(rows[0]["payload"], str)
            else rows[0]["payload"]
        )
        assert p["meeting_id"] == meeting_id
        assert p["provider"] == "stub"
        assert p["chunks_sent"] >= 5
    finally:
        os.environ.pop("TEST_AUDIT_STATEMENT_DELAY_SECONDS", None)


@pytest.mark.asyncio
async def test_b13_audit_durability_sigterm_flush(
    gateway_harness: ServerHarness,
    real_redis,
) -> None:
    """Verify active meeting summary lands in Neon when worker receives SIGTERM mid-stream."""
    schema = gateway_harness.schema_a
    meeting_id = f"mtg_b13_sigterm_{uuid.uuid4().hex[:8]}"

    # Configure artificial 2.0s delay per audit statement via environment
    os.environ["TEST_AUDIT_STATEMENT_DELAY_SECONDS"] = "2.0"
    try:
        settings = AsrSettings(
            _env_file=None,
            ASR_PROVIDER="stub",
            ASR_HEALTH_PORT=get_free_port(),
            REDIS_URL=str(
                real_redis.connection_pool.connection_kwargs.get("url", "redis://127.0.0.1:6379/0")
            ),
        )
        worker = AsrService(settings, worker_id="w_b13_sigterm")
        await worker.start()

        admin_token = gateway_harness.jwks.mint_token(
            sub="admin_b13s", org_id=gateway_harness.org_a, org_role="org:admin"
        )
        _, ingest_ticket_resp = await exchange_ws_ticket(
            gateway_harness.http_url, admin_token, meeting_id, role="ingest"
        )
        ingest_ticket = ingest_ticket_resp["ticket"]
        ingest_uri = (
            f"{gateway_harness.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ingest_ticket}"
        )

        async with websockets.connect(ingest_uri) as ws_ingest:

            async def recv_ack():
                while True:
                    msg = json.loads(await ws_ingest.recv())
                    if msg.get("type") == "ack":
                        return msg

            await ws_ingest.send(
                json.dumps(
                    {
                        "type": "control",
                        "action": "meeting_start",
                        "adapter_type": "chrome_extension",
                    }
                )
            )
            assert (await recv_ack())["status"] == "accepted"

            await ws_ingest.send(
                json.dumps(
                    {
                        "type": "control",
                        "action": "consent_confirmed",
                        "consent_type": "audio_capture",
                        "external_participants": True,
                        "jurisdiction_hint": "GDPR",
                        "consent_text_version": "v2.0",
                        "client_version": "2.1.0",
                    }
                )
            )
            assert (await recv_ack())["status"] == "accepted"

            # Stream audio chunks without sending meeting_end
            t_base = 13_000_000
            for seq in range(1, 6):
                frame = make_pcm16_chunk(seq=seq, ts_ms=t_base + seq * 100)
                await ws_ingest.send(frame)
                ack = await recv_ack()
                assert ack.get("status") in ("accepted", "acked")
                await asyncio.sleep(0.04)

            # Worker is actively processing meeting; ensure pipeline active
            for _ in range(50):
                if meeting_id in worker._active_pipelines:
                    break
                await asyncio.sleep(0.1)
            assert meeting_id in worker._active_pipelines, "Meeting pipeline should be active"

            # Simulate SIGTERM by triggering graceful worker.stop() mid-stream
            t_term_start = time.perf_counter()
            await worker.stop()
            t_term_dur = time.perf_counter() - t_term_start

        # Verify that worker.stop() waited at least 2.0s for the summary flush during SIGTERM
        assert t_term_dur >= 1.9, (
            f"Expected SIGTERM flush >= 2.0s due to slow DB, got {t_term_dur:.2f}s"
        )

        pool = await db.get_pool()
        rows = await pool.fetch(
            f"SELECT event_type, payload FROM {schema}.audit_events "
            f"WHERE payload->>'meeting_id' = $1 AND event_type = 'asr_session_summary'",
            meeting_id,
        )
        assert len(rows) == 1, (
            f"Expected 1 asr_session_summary row in Neon on SIGTERM, found {len(rows)}"
        )
        p = (
            json.loads(rows[0]["payload"])
            if isinstance(rows[0]["payload"], str)
            else rows[0]["payload"]
        )
        assert p["meeting_id"] == meeting_id
        assert p["provider"] == "stub"
        assert p["chunks_sent"] >= 5
    finally:
        os.environ.pop("TEST_AUDIT_STATEMENT_DELAY_SECONDS", None)
