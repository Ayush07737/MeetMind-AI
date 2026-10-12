"""Acceptance Test B9: Speaker Name Resolution and Multi-Channel Mapping.

Verifies:
1. Two-channel audio with local_user_name set on POST /v1/meetings:
   - Channel 0 events carry speaker_name = local_user_name.
   - Channel 1 events with a single remote attendee carry speaker_name = attendee name.
2. Meeting without metadata:
   - speaker_name is null.
"""

from __future__ import annotations

import asyncio
import json
import uuid

import httpx
import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket, make_pcm16_chunk
from meetmind_schemas.keys import events_stream_key

pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]


@pytest.mark.asyncio
async def test_b9_speaker_names_two_channel_mapping(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """Two-channel audio with local_user_name maps channel 0 to local user.

    Null without metadata.
    """
    _worker = await asr_worker_factory(provider="stub")

    admin_token = gateway_harness.jwks.mint_token(
        sub="admin_b9", org_id=gateway_harness.org_a, org_role="org:admin"
    )

    # 1. Create meeting WITH local_user_name and single attendee
    async with httpx.AsyncClient() as client:
        create_resp = await client.post(
            f"{gateway_harness.http_url}/v1/meetings",
            headers={"Authorization": f"Bearer {admin_token}"},
            json={
                "title": "Strategy Session",
                "local_user_name": "Alice Smith",
                "attendees": ["Bob Jones"],
            },
        )
        assert create_resp.status_code == 201, f"Failed to create meeting: {create_resp.text}"
        mtg_with_meta = create_resp.json()["meeting_id"]

    _, ticket_resp_1 = await exchange_ws_ticket(
        gateway_harness.http_url, admin_token, mtg_with_meta, role="ingest"
    )
    t1 = ticket_resp_1["ticket"]
    uri_1 = f"{gateway_harness.ws_url}/v1/meetings/{mtg_with_meta}/stream?ticket={t1}"

    async with websockets.connect(uri_1) as ws:

        async def recv_ack():
            while True:
                msg = json.loads(await ws.recv())
                if msg.get("type") == "ack":
                    return msg

        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": "chrome_extension",
                }
            )
        )
        assert (await recv_ack())["status"] == "accepted"

        await ws.send(
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

        # Send 12 frames of 2-channel audio
        t_base = 10_000_000
        for seq in range(1, 13):
            frame = make_pcm16_chunk(seq=seq, ts_ms=t_base + seq * 100, channels=2)
            await ws.send(frame)
            ack = await recv_ack()
            assert ack.get("status") in ("accepted", "acked")
            await asyncio.sleep(0.03)

        await ws.send(json.dumps({"type": "control", "action": "meeting_end"}))
        assert (await recv_ack())["status"] == "accepted"

    # Wait for processing
    await asyncio.sleep(2.0)

    # Verify finals in meeting 1
    ev_key_1 = events_stream_key(gateway_harness.tenant_a, mtg_with_meta)
    raw_1 = await real_redis.xrange(ev_key_1)
    finals_1 = []
    for _, fields in raw_1:
        p_raw = fields.get(b"payload") or fields.get("payload")
        if p_raw:
            parsed = json.loads(p_raw.decode() if isinstance(p_raw, bytes) else p_raw)
            if parsed.get("event_type") == "transcript_final":
                finals_1.append(parsed["payload"])

    assert len(finals_1) >= 2, f"Expected at least 2 finals, got {len(finals_1)}"
    # Check channel 0 has Alice Smith
    ch0_finals = [f for f in finals_1 if f.get("channel") == 0]
    assert len(ch0_finals) > 0
    for f in ch0_finals:
        assert f["speaker_name"] == "Alice Smith"

    # Check channel 1 has Bob Jones
    ch1_finals = [f for f in finals_1 if f.get("channel") == 1]
    assert len(ch1_finals) > 0
    for f in ch1_finals:
        assert f["speaker_name"] == "Bob Jones"

    # 2. Meeting WITHOUT metadata
    mtg_no_meta = f"mtg_b9_nometa_{uuid.uuid4().hex[:8]}"
    _, ticket_resp_2 = await exchange_ws_ticket(
        gateway_harness.http_url, admin_token, mtg_no_meta, role="ingest"
    )
    t2 = ticket_resp_2["ticket"]
    uri_2 = f"{gateway_harness.ws_url}/v1/meetings/{mtg_no_meta}/stream?ticket={t2}"

    async with websockets.connect(uri_2) as ws:

        async def recv_ack_2():
            while True:
                msg = json.loads(await ws.recv())
                if msg.get("type") == "ack":
                    return msg

        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": "chrome_extension",
                }
            )
        )
        assert (await recv_ack_2())["status"] == "accepted"

        await ws.send(
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
        assert (await recv_ack_2())["status"] == "accepted"

        for seq in range(1, 8):
            frame = make_pcm16_chunk(seq=seq, ts_ms=t_base + seq * 100, channels=2)
            await ws.send(frame)
            ack = await recv_ack_2()
            assert ack.get("status") in ("accepted", "acked")
            await asyncio.sleep(0.03)

        await ws.send(json.dumps({"type": "control", "action": "meeting_end"}))
        assert (await recv_ack_2())["status"] == "accepted"

    await asyncio.sleep(2.0)

    ev_key_2 = events_stream_key(gateway_harness.tenant_a, mtg_no_meta)
    raw_2 = await real_redis.xrange(ev_key_2)
    finals_2 = []
    for _, fields in raw_2:
        p_raw = fields.get(b"payload") or fields.get("payload")
        if p_raw:
            parsed = json.loads(p_raw.decode() if isinstance(p_raw, bytes) else p_raw)
            if parsed.get("event_type") == "transcript_final":
                finals_2.append(parsed["payload"])

    assert len(finals_2) >= 1
    for f in finals_2:
        assert f["speaker_name"] is None, f"Expected None speaker_name, got {f['speaker_name']}"
