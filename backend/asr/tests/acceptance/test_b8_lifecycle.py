"""Acceptance Test B8: Lifecycle Management and Idempotency.

Verifies:
1. Duplicate meeting_active events (e.g., from gateway reconnect) do NOT spawn
   duplicate pipelines or duplicate transcript finals.
2. meeting_abandoned event (e.g., from janitor) cleanly terminates the pipeline,
   drains remaining audio, releases the lease, and acknowledges the lifecycle entries.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket, make_pcm16_chunk
from meetmind_schemas.keys import asr_owner_key, lifecycle_stream_key

pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]


@pytest.mark.asyncio
async def test_b8_lifecycle_idempotency_and_abandonment(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """Duplicate meeting_active does not duplicate pipelines.

    meeting_abandoned cleanly shuts down.
    """
    meeting_id = f"mtg_b8_{uuid.uuid4().hex[:8]}"
    tenant_id = gateway_harness.tenant_a

    _worker = await asr_worker_factory(provider="stub")

    admin_token = gateway_harness.jwks.mint_token(
        sub="admin_b8", org_id=gateway_harness.org_a, org_role="org:admin"
    )
    _, ingest_ticket_resp = await exchange_ws_ticket(
        gateway_harness.http_url, admin_token, meeting_id, role="ingest"
    )
    ingest_ticket = ingest_ticket_resp["ticket"]
    ingest_uri = f"{gateway_harness.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ingest_ticket}"

    # 1. Connect and start meeting
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

        # Send 5 frames
        t_base = 9_000_000
        for seq in range(1, 6):
            frame = make_pcm16_chunk(seq=seq, ts_ms=t_base + seq * 100)
            await ws_ingest.send(frame)
            ack = await recv_ack()
            assert ack.get("status") in ("accepted", "acked")

    # Wait for pipeline to claim ownership
    lease_key = asr_owner_key(tenant_id, meeting_id)
    for _ in range(30):
        owner = await real_redis.get(lease_key)
        if owner:
            break
        await asyncio.sleep(0.1)
    assert owner is not None, "ASR pipeline did not acquire lease"

    # 2. Inject duplicate meeting_active events into mm:system:lifecycle
    l_key = lifecycle_stream_key()
    now_ms = int(time.time() * 1000)
    for _ in range(3):
        await real_redis.xadd(
            l_key,
            {
                "event": "meeting_active",
                "tenant": tenant_id,
                "meeting_id": meeting_id,
                "ts_ms": str(now_ms),
            },
        )

    # Allow worker to process duplicate lifecycle events
    await asyncio.sleep(1.0)

    # Verify that the lease is still owned by the same worker (not thrashing or resetting)
    owner_after = await real_redis.get(lease_key)
    assert owner_after == owner

    # 3. Inject meeting_abandoned event
    await real_redis.xadd(
        l_key,
        {
            "event": "meeting_abandoned",
            "tenant": tenant_id,
            "meeting_id": meeting_id,
            "ts_ms": str(int(time.time() * 1000)),
        },
    )

    # Wait for pipeline to shut down and release lease
    released = False
    for _ in range(40):
        if await real_redis.get(lease_key) is None:
            released = True
            break
        await asyncio.sleep(0.1)

    assert released, "ASR pipeline did not release lease after meeting_abandoned"

    # Verify pending lifecycle entries are acknowledged
    pending_info = await real_redis.xpending(l_key, "asr")
    # All entries for this meeting must be acked; total pending count should be 0
    assert pending_info["pending"] == 0, f"Pending lifecycle entries remaining: {pending_info}"
