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

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket
from meetmind_gateway.keys import out_stream_key
from meetmind_schemas.events import WSCloseCode

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
            # 3. Publish outbound event and verify fan-out to both listeners
            out_key = out_stream_key(tenant_id, meeting_id)
            event_payload = {
                "type": "intelligence_event",
                "event_type": "intelligence_event",
                "insight": "Crucial product roadmap decision",
            }
            await real_redis.xadd(
                out_key,
                {
                    "event_type": "intelligence_event",
                    "payload": json.dumps(event_payload),
                    "meeting_id": meeting_id,
                },
            )

            # Both listeners should receive the fanned out event
            msg_l1_raw = await asyncio.wait_for(l1_ws.recv(), timeout=5.0)
            msg_l2_raw = await asyncio.wait_for(l2_ws.recv(), timeout=5.0)

            msg_l1 = json.loads(msg_l1_raw)
            msg_l2 = json.loads(msg_l2_raw)

            assert msg_l1["event_type"] == "intelligence_event"
            assert msg_l1["payload"]["insight"] == "Crucial product roadmap decision"
            assert msg_l2["event_type"] == "intelligence_event"
            assert msg_l2["payload"]["insight"] == "Crucial product roadmap decision"

            # 4. Listener 1 attempts to write -> rejected with 4400 (PROTOCOL_VIOLATION)
            await l1_ws.send(b"\x00\x01\x02\x03\x04")
            with pytest.raises(websockets.exceptions.ConnectionClosed) as exc_info:
                # Discard any buffered frames until connection closed
                while True:
                    await l1_ws.recv()
            assert exc_info.value.rcvd is not None
            assert exc_info.value.rcvd.code == WSCloseCode.PROTOCOL_VIOLATION

        # 5. Second ingest writer attempts to connect -> rejected with 4409 (WRITER_CONFLICT)
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
