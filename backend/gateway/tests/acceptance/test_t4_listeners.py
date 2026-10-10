"""Acceptance Test T4: Multiple Listeners & Single-Writer Exclusivity (Real Uvicorn, Redis, Neon).

Verifies:
1. 1 ingest writer connects, starts meeting, and confirms consent.
2. 2 simultaneous read-only listeners connect via /listen.
3. Allow-listed outbound events fan out to all listeners.
4. Second ingest connection rejected with 4409 (WRITER_CONFLICT).
5. Listener attempting to send audio/text rejected with 4400 (PROTOCOL_VIOLATION).
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket
from meetmind_gateway.keys import audio_stream_key, events_stream_key, out_stream_key
from meetmind_schemas.events import WSCloseCode, encode_audio_frame

pytestmark = [pytest.mark.integration]


@pytest.mark.asyncio
async def test_t4_multiple_listeners_and_single_writer(
    gateway_harness: ServerHarness,
    real_redis,
) -> None:
    h = gateway_harness
    meeting_id = f"mtg_t4_{uuid.uuid4().hex[:6]}"
    tenant_id = h.tenant_a

    # 1. Start Ingest Writer
    writer_token = h.jwks.mint_token(sub="user_writer_1", org_id=h.org_a, org_role="org:admin")
    status, writer_ticket_resp = await exchange_ws_ticket(
        h.http_url, writer_token, meeting_id, role="ingest"
    )
    assert status == 200, f"Writer ticket exchange failed: {writer_ticket_resp}"
    writer_ticket = writer_ticket_resp["ticket"]

    writer_ws_uri = f"{h.ws_url}/v1/meetings/{meeting_id}/stream?ticket={writer_ticket}"

    async with websockets.connect(writer_ws_uri) as writer_ws:
        # Start meeting & confirm consent
        await writer_ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": "chrome_extension",
                }
            )
        )
        ack_start = json.loads(await writer_ws.recv())
        assert ack_start["status"] == "accepted"

        await writer_ws.send(
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
        ack_consent = json.loads(await writer_ws.recv())
        assert ack_consent["status"] == "accepted"

        # Assert audio stream entry count == frames sent
        audio_key = audio_stream_key(tenant_id, meeting_id)
        for seq in (1, 2):
            ts = int(datetime.now(UTC).timestamp() * 1000)
            framed = encode_audio_frame(
                client_seq=seq,
                capture_ts_ms=ts,
                payload=b"audio_pcm_frame",
            )
            await writer_ws.send(framed)
            ack_audio = json.loads(await writer_ws.recv())
            assert ack_audio["type"] == "ack"
            assert ack_audio["seq"] == seq

        stream_count = await real_redis.xlen(audio_key)
        assert stream_count == 2, f"Expected 2 audio frames, found {stream_count}"

        # 2. Connect 2 Listeners via /v1/meetings/{meeting_id}/listen
        token_l1 = h.jwks.mint_token(sub="user_listener_1", org_id=h.org_a, org_role="org:admin")
        status_l1, ticket_l1_resp = await exchange_ws_ticket(
            h.http_url, token_l1, meeting_id, role="listen"
        )
        assert status_l1 == 200, f"Listener 1 ticket failed: {ticket_l1_resp}"
        ticket_l1 = ticket_l1_resp["ticket"]

        token_l2 = h.jwks.mint_token(sub="user_listener_2", org_id=h.org_a, org_role="org:admin")
        status_l2, ticket_l2_resp = await exchange_ws_ticket(
            h.http_url, token_l2, meeting_id, role="listen"
        )
        assert status_l2 == 200, f"Listener 2 ticket failed: {ticket_l2_resp}"
        ticket_l2 = ticket_l2_resp["ticket"]

        l1_uri = f"{h.ws_url}/v1/meetings/{meeting_id}/listen?ticket={ticket_l1}&last_event_id=0-0"
        l2_uri = f"{h.ws_url}/v1/meetings/{meeting_id}/listen?ticket={ticket_l2}&last_event_id=0-0"

        async with websockets.connect(l1_uri) as l1_ws, websockets.connect(l2_uri) as l2_ws:
            # 3. Assert an internal-bus event reaches NEITHER listener
            internal_key = events_stream_key(tenant_id, meeting_id)
            await real_redis.xadd(
                internal_key,
                {"payload": json.dumps({"internal_secret": "internal_bus_only"})},
            )

            # Neither listener should receive the internal bus event
            for l_ws in (l1_ws, l2_ws):
                with pytest.raises(TimeoutError):
                    await asyncio.wait_for(l_ws.recv(), timeout=0.4)

            # 4. Outbound event fanning out exactly once to both listeners
            out_key = out_stream_key(tenant_id, meeting_id)
            event_payload = {
                "type": "intelligence_event",
                "event_type": "intelligence_event",
                "insight": "Crucial product roadmap decision",
            }
            entry_id_1 = await real_redis.xadd(
                out_key,
                {
                    "event_type": "intelligence_event",
                    "payload": json.dumps(event_payload),
                    "meeting_id": meeting_id,
                },
            )
            entry_id_1_str = (
                entry_id_1.decode("utf-8") if isinstance(entry_id_1, bytes) else str(entry_id_1)
            )

            # Both listeners should receive the fanned out event exactly once
            msg_l1_raw = await asyncio.wait_for(l1_ws.recv(), timeout=5.0)
            msg_l2_raw = await asyncio.wait_for(l2_ws.recv(), timeout=5.0)

            msg_l1 = json.loads(msg_l1_raw)
            msg_l2 = json.loads(msg_l2_raw)

            assert msg_l1["event_type"] == "intelligence_event"
            assert msg_l1["payload"]["insight"] == "Crucial product roadmap decision"
            assert msg_l2["event_type"] == "intelligence_event"
            assert msg_l2["payload"]["insight"] == "Crucial product roadmap decision"

            # Verify exactly once: no duplicate delivery
            for l_ws in (l1_ws, l2_ws):
                with pytest.raises(TimeoutError):
                    await asyncio.wait_for(l_ws.recv(), timeout=0.3)

            # 5. Listener 1 attempts to write -> rejected with 4400 (PROTOCOL_VIOLATION)
            await l1_ws.send(b"\x00\x01\x02\x03\x04")
            with pytest.raises(websockets.exceptions.ConnectionClosed) as exc_info:
                while True:
                    await l1_ws.recv()
            assert exc_info.value.rcvd is not None
            assert exc_info.value.rcvd.code == WSCloseCode.PROTOCOL_VIOLATION

        # 6. Verify last_event_id resume works for reconnecting listeners
        # Publish event 2 while listener 2 is disconnected
        event_payload_2 = {
            "type": "intelligence_event",
            "event_type": "intelligence_event",
            "insight": "Follow-up action item from resumed session",
        }
        await real_redis.xadd(
            out_key,
            {
                "event_type": "intelligence_event",
                "payload": json.dumps(event_payload_2),
                "meeting_id": meeting_id,
            },
        )

        # Exchange new ticket for listener 2 and reconnect with last_event_id=entry_id_1_str
        status_l2_resume, ticket_l2_resume_resp = await exchange_ws_ticket(
            h.http_url, token_l2, meeting_id, role="listen"
        )
        assert status_l2_resume == 200
        ticket_l2_resume = ticket_l2_resume_resp["ticket"]
        l2_resume_uri = (
            f"{h.ws_url}/v1/meetings/{meeting_id}/listen?"
            f"ticket={ticket_l2_resume}&last_event_id={entry_id_1_str}"
        )

        async with websockets.connect(l2_resume_uri) as l2_resume_ws:
            msg_resumed_raw = await asyncio.wait_for(l2_resume_ws.recv(), timeout=5.0)
            msg_resumed = json.loads(msg_resumed_raw)
            assert msg_resumed["event_type"] == "intelligence_event"
            assert msg_resumed["payload"]["insight"] == "Follow-up action item from resumed session"

        # 7. Second ingest writer attempts to connect -> rejected with 4409 (WRITER_CONFLICT)
        token_writer2 = h.jwks.mint_token(sub="user_writer_2", org_id=h.org_a, org_role="org:admin")
        status_w2, ticket_w2_resp = await exchange_ws_ticket(
            h.http_url, token_writer2, meeting_id, role="ingest"
        )
        assert status_w2 == 200
        ticket_w2 = ticket_w2_resp["ticket"]

        w2_uri = f"{h.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ticket_w2}"
        with pytest.raises(websockets.exceptions.ConnectionClosed) as exc_conflict:
            async with websockets.connect(w2_uri) as conflict_ws:
                await conflict_ws.recv()

        assert exc_conflict.value.rcvd is not None
        assert exc_conflict.value.rcvd.code == WSCloseCode.WRITER_CONFLICT
