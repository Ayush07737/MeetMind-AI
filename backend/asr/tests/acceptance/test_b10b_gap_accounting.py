"""Acceptance Test B10b: Audit Gap Accounting Verification (S0-12).

Verifies three real-Redis compliance invariants:
1. Mid-stream provider reconnect with no client skip produces asr_session_summary gaps == [].
2. A genuine client skip of N chunks produces exactly one gap with correct bounds and
   gap_chunks == N.
3. Two separate client skips produce two non-overlapping, strictly ordered gap ranges.
"""

from __future__ import annotations

import asyncio
import json
import uuid

import asyncpg
import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket, make_pcm16_chunk
from meetmind_security import db

pytestmark = [pytest.mark.integration, pytest.mark.timeout(180)]


async def _drain_acks(ws):
    while True:
        msg = json.loads(await ws.recv())
        if msg.get("type") == "ack":
            return msg


async def _fetch_summary_payload(meeting_id: str, schema: str) -> dict | None:
    dsn = db.get_owner_dsn()
    for _ in range(30):
        try:
            conn = await asyncpg.connect(dsn, timeout=10.0)
            try:
                rows = await conn.fetch(
                    f"SELECT payload FROM {schema}.audit_events "
                    "WHERE event_type = 'asr_session_summary' AND payload->>'meeting_id' = $1",
                    meeting_id,
                )
                if rows:
                    p = rows[0]["payload"]
                    return json.loads(p) if isinstance(p, str) else p
            finally:
                await conn.close()
        except Exception:
            pass
        await asyncio.sleep(0.5)
    return None


@pytest.mark.asyncio
async def test_b10b_gap_accounting_reconnect_zero_loss(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """Invariant 1: Provider reconnect with no client skips must produce gaps == []."""
    meeting_id = f"mtg_gap1_{uuid.uuid4().hex[:8]}"

    # Start ASR worker with test hooks enabled so we can trigger provider reconnect
    await asr_worker_factory(
        provider="stub",
        extra_env={"ASR_TEST_HOOKS": "true", "APP_ENV": "test"},
    )

    token = gateway_harness.jwks.mint_token(
        sub="admin_gap1", org_id=gateway_harness.org_a, org_role="org:admin"
    )
    _, ticket_resp = await exchange_ws_ticket(
        gateway_harness.http_url, token, meeting_id, role="ingest"
    )
    ws_uri = (
        f"{gateway_harness.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ticket_resp['ticket']}"
    )

    async with websockets.connect(ws_uri) as ws:
        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": "chrome_extension",
                }
            )
        )
        assert (await _drain_acks(ws))["status"] == "accepted"

        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "consent_confirmed",
                    "consent_type": "audio_capture",
                    "external_participants": True,
                    "jurisdiction_hint": "US",
                    "consent_text_version": "v1.0",
                    "client_version": "1.0.0",
                }
            )
        )
        assert (await _drain_acks(ws))["status"] == "accepted"

        # Stream chunks 1..10 continuously without any skip
        t0 = 10_000_000
        for seq in range(1, 11):
            await ws.send(make_pcm16_chunk(seq=seq, ts_ms=t0 + seq * 100))
            await _drain_acks(ws)
            await asyncio.sleep(0.02)

        # Trigger mid-stream provider reconnect via control loop hook
        await real_redis.xadd(
            "mm:system:asr_control", {"action": "drop_provider", "meeting_id": meeting_id}
        )
        await asyncio.sleep(0.5)

        # Stream chunks 11..20 continuously
        for seq in range(11, 21):
            await ws.send(make_pcm16_chunk(seq=seq, ts_ms=t0 + seq * 100))
            await _drain_acks(ws)
            await asyncio.sleep(0.02)

        # Allow pipeline to finish processing all chunks
        await asyncio.sleep(1.0)

        await ws.send(json.dumps({"type": "control", "action": "meeting_end"}))
        assert (await _drain_acks(ws))["status"] == "accepted"

    summary_payload = await _fetch_summary_payload(meeting_id, gateway_harness.schema_a)
    assert summary_payload is not None, "asr_session_summary must be written to Neon"
    assert summary_payload["chunks_sent"] >= 20
    assert summary_payload["reconnects"] >= 1
    # INVARIANT: zero client skips means gaps must be strictly empty!
    assert summary_payload["gaps"] == [], (
        f"Expected zero gaps on provider reconnect, got: {summary_payload['gaps']}"
    )


@pytest.mark.asyncio
async def test_b10b_gap_accounting_single_client_skip(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """Invariant 2: A genuine client skip of N chunks produces exactly one gap."""
    meeting_id = f"mtg_gap2_{uuid.uuid4().hex[:8]}"

    await asr_worker_factory(provider="stub")

    token = gateway_harness.jwks.mint_token(
        sub="admin_gap2", org_id=gateway_harness.org_a, org_role="org:admin"
    )
    _, ticket_resp = await exchange_ws_ticket(
        gateway_harness.http_url, token, meeting_id, role="ingest"
    )
    ws_uri = (
        f"{gateway_harness.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ticket_resp['ticket']}"
    )

    async with websockets.connect(ws_uri) as ws:
        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": "chrome_extension",
                }
            )
        )
        assert (await _drain_acks(ws))["status"] == "accepted"

        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "consent_confirmed",
                    "consent_type": "audio_capture",
                    "external_participants": True,
                    "jurisdiction_hint": "US",
                    "consent_text_version": "v1.0",
                    "client_version": "1.0.0",
                }
            )
        )
        assert (await _drain_acks(ws))["status"] == "accepted"

        t0 = 20_000_000
        # Chunks 1..5
        for seq in range(1, 6):
            await ws.send(make_pcm16_chunk(seq=seq, ts_ms=t0 + seq * 100))
            await _drain_acks(ws)
            await asyncio.sleep(0.02)

        # Skip chunks 6..10 (5 chunks missing), resume at 11
        for seq in range(11, 16):
            await ws.send(make_pcm16_chunk(seq=seq, ts_ms=t0 + seq * 100))
            await _drain_acks(ws)
            await asyncio.sleep(0.02)

        await ws.send(json.dumps({"type": "control", "action": "meeting_end"}))
        assert (await _drain_acks(ws))["status"] == "accepted"

    summary_payload = await _fetch_summary_payload(meeting_id, gateway_harness.schema_a)
    assert summary_payload is not None
    gaps = summary_payload["gaps"]
    assert len(gaps) == 1, f"Expected exactly 1 gap, got: {gaps}"
    assert gaps[0]["from_seq"] == 6
    assert gaps[0]["to_seq"] == 10
    assert gaps[0]["gap_chunks"] == 5


@pytest.mark.asyncio
async def test_b10b_gap_accounting_two_non_overlapping_skips(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """Invariant 3: Two separate client skips produce two non-overlapping, ordered ranges."""
    meeting_id = f"mtg_gap3_{uuid.uuid4().hex[:8]}"

    await asr_worker_factory(provider="stub")

    token = gateway_harness.jwks.mint_token(
        sub="admin_gap3", org_id=gateway_harness.org_a, org_role="org:admin"
    )
    _, ticket_resp = await exchange_ws_ticket(
        gateway_harness.http_url, token, meeting_id, role="ingest"
    )
    ws_uri = (
        f"{gateway_harness.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ticket_resp['ticket']}"
    )

    async with websockets.connect(ws_uri) as ws:
        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": "chrome_extension",
                }
            )
        )
        assert (await _drain_acks(ws))["status"] == "accepted"

        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "consent_confirmed",
                    "consent_type": "audio_capture",
                    "external_participants": True,
                    "jurisdiction_hint": "US",
                    "consent_text_version": "v1.0",
                    "client_version": "1.0.0",
                }
            )
        )
        assert (await _drain_acks(ws))["status"] == "accepted"

        t0 = 30_000_000
        # 1. Chunks 1..3
        for seq in range(1, 4):
            await ws.send(make_pcm16_chunk(seq=seq, ts_ms=t0 + seq * 100))
            await _drain_acks(ws)
            await asyncio.sleep(0.02)

        # Skip 4..6 (3 chunks missing), send 7..9
        for seq in range(7, 10):
            await ws.send(make_pcm16_chunk(seq=seq, ts_ms=t0 + seq * 100))
            await _drain_acks(ws)
            await asyncio.sleep(0.02)

        # Skip 10..14 (5 chunks missing), send 15..17
        for seq in range(15, 18):
            await ws.send(make_pcm16_chunk(seq=seq, ts_ms=t0 + seq * 100))
            await _drain_acks(ws)
            await asyncio.sleep(0.02)

        await ws.send(json.dumps({"type": "control", "action": "meeting_end"}))
        assert (await _drain_acks(ws))["status"] == "accepted"

    summary_payload = await _fetch_summary_payload(meeting_id, gateway_harness.schema_a)
    assert summary_payload is not None
    gaps = summary_payload["gaps"]
    assert len(gaps) == 2, f"Expected exactly 2 gaps, got: {gaps}"

    # Verify strictly non-overlapping and correct bounds
    assert gaps[0]["from_seq"] == 4
    assert gaps[0]["to_seq"] == 6
    assert gaps[0]["gap_chunks"] == 3

    assert gaps[1]["from_seq"] == 10
    assert gaps[1]["to_seq"] == 14
    assert gaps[1]["gap_chunks"] == 5

    assert gaps[0]["to_seq"] < gaps[1]["from_seq"], (
        "Gaps must be strictly non-overlapping and sorted"
    )
