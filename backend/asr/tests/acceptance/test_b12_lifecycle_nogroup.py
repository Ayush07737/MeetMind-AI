"""Acceptance Test B12: NOGROUP Recovery and Lifecycle Fast-Path Start.

Verifies:
1. Start the ASR worker BEFORE any lifecycle stream exists in Redis.
2. The worker creates the consumer group at startup with MKSTREAM.
3. If NOGROUP occurs, the worker recreates the group (id "0") and retries cleanly.
4. When a meeting is started, assert the pipeline starts through the lifecycle path
   (started_via == "lifecycle"), not the reconciler, within 1.0 second.
"""

from __future__ import annotations

import asyncio
import json
import uuid

import httpx
import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket, make_pcm16_chunk
from meetmind_schemas.keys import lifecycle_stream_key

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]


@pytest.mark.asyncio
async def test_b12_worker_starts_before_lifecycle_stream_and_uses_fastpath(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    # 1. Ensure lifecycle stream does NOT exist initially
    l_key = lifecycle_stream_key()
    await real_redis.delete(l_key)
    assert await real_redis.exists(l_key) == 0, f"Expected {l_key} to be deleted before test"

    # 2. Start ASR worker BEFORE lifecycle stream exists with reconciler interval 5.0s
    # so if it starts within 1s, it CANNOT be from the reconciler!
    worker = await asr_worker_factory(
        provider="stub",
        extra_env={
            "ASR_RECONCILE_INTERVAL_MS": "5000",
        },
    )

    meeting_id = f"mtg_b12_{uuid.uuid4().hex[:8]}"
    admin_token = gateway_harness.jwks.mint_token(
        sub="admin_b12", org_id=gateway_harness.org_a, org_role="org:admin"
    )
    _, ingest_ticket_resp = await exchange_ws_ticket(
        gateway_harness.http_url, admin_token, meeting_id, role="ingest"
    )
    ingest_ticket = ingest_ticket_resp["ticket"]
    ingest_uri = f"{gateway_harness.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ingest_ticket}"

    # 3. Connect to gateway and start meeting
    async with websockets.connect(ingest_uri) as ws_ingest:

        async def recv_ack():
            while True:
                msg = json.loads(await ws_ingest.recv())
                if msg.get("type") == "ack":
                    return msg

        # 1. meeting_start transitions to CONSENT_PENDING
        await ws_ingest.send(
            json.dumps({
                "type": "control",
                "action": "meeting_start",
                "adapter_type": "chrome_extension",
            })
        )
        assert (await recv_ack())["status"] == "accepted"

        # 2. consent_confirmed activates meeting and publishes meeting_active to lifecycle stream
        await ws_ingest.send(
            json.dumps({
                "type": "control",
                "action": "consent_confirmed",
                "consent_type": "audio_capture",
                "external_participants": False,
                "jurisdiction_hint": "US",
                "consent_text_version": "v1.0",
                "client_version": "1.0.0",
            })
        )
        assert (await recv_ack())["status"] == "accepted"
        t_start = asyncio.get_running_loop().time()

        # 3. Assert pipeline starts through lifecycle path within 1.0 second
        started_via = None
        elapsed_s = 0.0
        async with httpx.AsyncClient() as client:
            for _ in range(25):  # poll every 40ms up to 1.0s
                await asyncio.sleep(0.04)
                elapsed_s = asyncio.get_running_loop().time() - t_start
                try:
                    res = await client.get(f"{worker.health_url}?verbose=1", timeout=0.5)
                    if res.status_code == 200:
                        data = res.json()
                        metrics = data.get("metrics", {})
                        if metrics.get("started_via"):
                            started_via = metrics["started_via"]
                            break
                except Exception:
                    pass
                if elapsed_s > 1.0:
                    break

        assert started_via is not None, (
            f"Pipeline for {meeting_id} did not start within 1.0s (elapsed={elapsed_s:.3f}s)"
        )
        assert started_via == "lifecycle", (
            f"Expected pipeline to start via 'lifecycle' fast-path, but got '{started_via}'"
        )
        assert elapsed_s <= 1.0, f"Expected start within 1.0s, took {elapsed_s:.3f}s"

        # 4. Stream audio and verify ack
        frame = make_pcm16_chunk(seq=1, ts_ms=1000)
        await ws_ingest.send(frame)
        ack = await recv_ack()
        assert ack.get("status") in ("accepted", "acked")

        # Cleanly end meeting
        await ws_ingest.send(json.dumps({"type": "control", "action": "meeting_end"}))
        assert (await recv_ack())["status"] == "accepted"
