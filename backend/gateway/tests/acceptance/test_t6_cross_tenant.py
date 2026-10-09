"""Acceptance Test T6: Strict Cross-Tenant Isolation (Real Uvicorn, Redis, Neon).

Verifies:
1. Meeting created under Tenant A binds meeting to Tenant A.
2. Tenant B attempting ticket exchange for Tenant A's meeting gets HTTP 403.
3. cross_tenant_access_denied audit row written to Neon in Tenant B schema.
4. Tenant B WebSocket connection attempts rejected with 4403 (FORBIDDEN).
5. Redis stream keys strictly isolated by tenant prefix with no leakage.
"""

from __future__ import annotations

import json
import uuid

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket
from meetmind_gateway.keys import audio_stream_key
from meetmind_schemas.events import WSCloseCode, encode_audio_frame
from websockets.exceptions import ConnectionClosed

pytestmark = [pytest.mark.integration]


@pytest.mark.asyncio
async def test_t6_cross_tenant_isolation_and_audit(
    gateway_harness: ServerHarness,
    real_redis,
) -> None:
    h = gateway_harness
    meeting_id = f"mtg_t6_iso_{uuid.uuid4().hex[:6]}"
    tenant_a = h.tenant_a
    tenant_b = h.tenant_b

    # 1. Establish meeting under Tenant A
    token_a = h.jwks.mint_token(sub="user_tenant_a", org_id=h.org_a, org_role="org:admin")
    status_a, resp_a = await exchange_ws_ticket(h.http_url, token_a, meeting_id, role="ingest")
    assert status_a == 200
    ticket_a = resp_a["ticket"]

    uri_a = f"{h.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ticket_a}"
    async with websockets.connect(uri_a) as ws_a:
        # Start meeting & confirm consent
        await ws_a.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": "chrome_extension",
                }
            )
        )
        ack_start = json.loads(await ws_a.recv())
        assert ack_start["status"] == "accepted"

        await ws_a.send(
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
        ack_consent = json.loads(await ws_a.recv())
        assert ack_consent["status"] == "accepted"

        # Send 1 audio chunk in Tenant A
        pcm = b"TENANT_A_AUDIO_CHUNK"
        framed = encode_audio_frame(client_seq=1, capture_ts_ms=1000, payload=pcm)
        await ws_a.send(framed)
        ack_audio = json.loads(await ws_a.recv())
        assert ack_audio["seq"] == 1

        # 2. Tenant B attempts ticket exchange for Tenant A's meeting -> 403 Forbidden
        token_b = h.jwks.mint_token(sub="user_tenant_b", org_id=h.org_b, org_role="org:admin")
        status_b, resp_b = await exchange_ws_ticket(h.http_url, token_b, meeting_id, role="ingest")
        assert status_b == 403
        assert resp_b.get("detail") == "cross-tenant access denied"

        # 3. Verify cross_tenant_access_denied audit event written to Tenant B schema in Neon
        async with h.tenant_conn(tenant_b) as conn:
            audit_row = await conn.fetchrow(
                """
                SELECT event_type, payload
                FROM audit_events
                WHERE tenant_id = $1 AND event_type = 'cross_tenant_access_denied'
                ORDER BY created_at DESC LIMIT 1
                """,
                tenant_b,
            )
            assert audit_row is not None
            payload = json.loads(audit_row["payload"])
            assert payload["meeting_id"] == meeting_id
            assert payload["target_tenant"] == tenant_a
            assert payload["attempted_tenant"] == tenant_b

        # 4. Tenant B attempts connection with a ticket issued for another meeting
        other_meeting = f"mtg_t6_other_{uuid.uuid4().hex[:6]}"
        status_other, resp_other = await exchange_ws_ticket(
            h.http_url, token_b, other_meeting, role="listen"
        )
        assert status_other == 200
        ticket_b_other = resp_other["ticket"]

        # Attempt to use ticket_b on Tenant A's meeting
        attack_uri = f"{h.ws_url}/v1/meetings/{meeting_id}/listen?ticket={ticket_b_other}"
        with pytest.raises(ConnectionClosed) as exc_attack:
            async with websockets.connect(attack_uri) as ws_attack:
                await ws_attack.recv()
        # Rejected because ticket was issued for different meeting / cross tenant
        assert exc_attack.value.rcvd is not None
        assert exc_attack.value.rcvd.code == WSCloseCode.FORBIDDEN

    # 5. Verify Redis key isolation: data in Tenant A stream, none in Tenant B stream
    key_a = audio_stream_key(tenant_a, meeting_id)
    key_b = audio_stream_key(tenant_b, meeting_id)

    len_a = await real_redis.xlen(key_a)
    len_b = await real_redis.xlen(key_b)

    assert len_a == 1
    assert len_b == 0
