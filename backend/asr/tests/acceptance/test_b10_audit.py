"""Acceptance Test B10: Audit Event Durability and DB Resilience.

Verifies:
1. After a meeting, real asr_session_started and asr_session_summary rows exist in
   the Neon audit_events table with documented fields.
2. No transcript text or audio bytes exist anywhere in audit payloads.
3. When Neon is unreachable, ASR transcription continues normally without failing.
"""

from __future__ import annotations

import asyncio
import json
import uuid

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket, make_pcm16_chunk
from meetmind_schemas.keys import events_stream_key
from meetmind_security import db

pytestmark = [pytest.mark.integration, pytest.mark.timeout(180)]


@pytest.mark.asyncio
async def test_b10_audit_events_and_resilience(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """Audit events written to Neon with exact fields.

    Pipeline transcribes even if Neon is down.
    """
    tenant_id = gateway_harness.tenant_a
    meeting_id = f"mtg_b10_{uuid.uuid4().hex[:8]}"

    # Start ASR worker with real Neon DB URL
    _worker = await asr_worker_factory(provider="stub")

    admin_token = gateway_harness.jwks.mint_token(
        sub="admin_b10", org_id=gateway_harness.org_a, org_role="org:admin"
    )
    _, ingest_ticket_resp = await exchange_ws_ticket(
        gateway_harness.http_url, admin_token, meeting_id, role="ingest"
    )
    ingest_ticket = ingest_ticket_resp["ticket"]
    ingest_uri = f"{gateway_harness.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ingest_ticket}"

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

        # Send 8 audio frames
        t_base = 11_000_000
        for seq in range(1, 9):
            frame = make_pcm16_chunk(seq=seq, ts_ms=t_base + seq * 100)
            await ws_ingest.send(frame)
            ack = await recv_ack()
            assert ack.get("status") in ("accepted", "acked")
            await asyncio.sleep(0.04)

        # End meeting cleanly
        await ws_ingest.send(json.dumps({"type": "control", "action": "meeting_end"}))
        assert (await recv_ack())["status"] == "accepted"

    # Wait for ASR worker to flush and auditor to write to Neon
    await asyncio.sleep(3.0)

    # 1. Query Neon audit_events
    schema = gateway_harness.schema_a
    pool = await db.get_pool()

    rows = []
    events_by_type = {}
    for _ in range(30):
        rows = await pool.fetch(
            f"SELECT event_type, payload FROM {schema}.audit_events "
            f"WHERE payload->>'meeting_id' = $1",
            meeting_id,
        )
        for r in rows:
            p = json.loads(r["payload"]) if isinstance(r["payload"], str) else r["payload"]
            events_by_type[r["event_type"]] = p

        if "asr_session_started" in events_by_type and "asr_session_summary" in events_by_type:
            break
        await asyncio.sleep(0.5)

    assert "asr_session_started" in events_by_type
    assert "asr_session_summary" in events_by_type

    start_payload = events_by_type["asr_session_started"]
    assert start_payload["meeting_id"] == meeting_id
    assert start_payload["provider"] == "stub"
    assert "endpoint_host" in start_payload
    assert "language" in start_payload
    assert "mip_opt_out" in start_payload

    # Assert no text or audio bytes in start payload
    for forbidden in ("text", "transcript", "audio", "data", "raw"):
        assert forbidden not in start_payload

    summary_payload = events_by_type["asr_session_summary"]
    assert summary_payload["meeting_id"] == meeting_id
    assert summary_payload["provider"] == "stub"
    assert "chunks_sent" in summary_payload
    assert summary_payload["chunks_sent"] >= 5
    assert "bytes_sent" in summary_payload
    assert "reconnects" in summary_payload
    assert "dropped_chunks" in summary_payload
    assert "degraded_seconds" in summary_payload

    # Assert no text or audio bytes in summary payload
    for forbidden in ("text", "transcript", "audio", "data", "raw"):
        assert forbidden not in summary_payload

    # 2. Test DB resilience: with Neon unreachable, ASR still transcribes
    _unreachable_worker = await asr_worker_factory(
        provider="stub",
        extra_env={"NEON_DATABASE_URL": "postgresql://invalid:invalid@127.0.0.1:54329/invalid"},
    )

    mtg_resilient = f"mtg_b10_resilient_{uuid.uuid4().hex[:8]}"
    _, ticket_resp_res = await exchange_ws_ticket(
        gateway_harness.http_url, admin_token, mtg_resilient, role="ingest"
    )
    res_ticket = ticket_resp_res["ticket"]
    uri_res = f"{gateway_harness.ws_url}/v1/meetings/{mtg_resilient}/stream?ticket={res_ticket}"

    async with websockets.connect(uri_res) as ws_ingest:
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

        for seq in range(1, 8):
            frame = make_pcm16_chunk(seq=seq, ts_ms=t_base + seq * 100)
            await ws_ingest.send(frame)
            ack = await recv_ack()
            assert ack.get("status") in ("accepted", "acked")
            await asyncio.sleep(0.04)

        await ws_ingest.send(json.dumps({"type": "control", "action": "meeting_end"}))
        assert (await recv_ack())["status"] == "accepted"

    # Wait for processing
    await asyncio.sleep(2.0)

    # Verify finals were still generated despite Neon being down
    ev_key_res = events_stream_key(tenant_id, mtg_resilient)
    raw_res = await real_redis.xrange(ev_key_res)
    res_finals = []
    for _, fields in raw_res:
        p_raw = fields.get(b"payload") or fields.get("payload")
        if p_raw:
            parsed = json.loads(p_raw.decode() if isinstance(p_raw, bytes) else p_raw)
            if parsed.get("event_type") == "transcript_final":
                res_finals.append(parsed)

    assert len(res_finals) >= 1, "ASR pipeline failed to transcribe when DB was unreachable"
