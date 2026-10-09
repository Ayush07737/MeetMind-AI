"""Acceptance Test T5: Comprehensive Authentication Matrix (Real Uvicorn, Local JWKS, Redis).

Verifies:
1. Expired token -> 401
2. Future nbf -> 401
3. Wrong issuer -> 401
4. Wrong azp -> 401
5. Unknown kid -> 401
6. Key rotation -> dynamic refresh succeeds (200)
7. HS256 algorithm confusion attack -> 401
8. Algorithm 'none' attack -> 401
9. Replayed WS ticket -> 4401
10. Invalid / nonexistent WS ticket -> 4401
11. Ticket issued for Meeting A used on Meeting B -> 4403
"""

from __future__ import annotations

import uuid

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket
from meetmind_schemas.events import WSCloseCode
from websockets.exceptions import ConnectionClosed

pytestmark = [pytest.mark.integration]


@pytest.mark.asyncio
async def test_t5_auth_token_failures_matrix(
    gateway_harness: ServerHarness,
) -> None:
    h = gateway_harness
    meeting_id = f"mtg_t5_auth_{uuid.uuid4().hex[:6]}"

    # 1. Expired token
    expired_token = h.jwks.mint_token(
        sub="user_expired",
        org_id=h.org_a,
        org_role="org:admin",
        exp_offset=-3600,
    )
    status, resp = await exchange_ws_ticket(h.http_url, expired_token, meeting_id)
    assert status == 401

    # 2. Future nbf
    future_nbf_token = h.jwks.mint_token(
        sub="user_nbf",
        org_id=h.org_a,
        org_role="org:admin",
        nbf_offset=3600,
    )
    status, resp = await exchange_ws_ticket(h.http_url, future_nbf_token, meeting_id)
    assert status == 401

    # 3. Wrong issuer
    wrong_iss_token = h.jwks.mint_token(
        sub="user_iss",
        org_id=h.org_a,
        org_role="org:admin",
        iss="https://attacker.example.com",
    )
    status, resp = await exchange_ws_ticket(h.http_url, wrong_iss_token, meeting_id)
    assert status == 401

    # 4. Wrong azp
    wrong_azp_token = h.jwks.mint_token(
        sub="user_azp",
        org_id=h.org_a,
        org_role="org:admin",
        azp="https://malicious-client.com",
    )
    status, resp = await exchange_ws_ticket(h.http_url, wrong_azp_token, meeting_id)
    assert status == 401

    # 5. Unknown kid
    unknown_kid_token = h.jwks.mint_token(
        sub="user_unknown_kid",
        org_id=h.org_a,
        org_role="org:admin",
        kid="non-existent-kid-12345",
    )
    status, resp = await exchange_ws_ticket(h.http_url, unknown_kid_token, meeting_id)
    assert status == 401

    # 6. Algorithm 'none' attack
    none_token = h.jwks.mint_token(
        sub="user_none",
        org_id=h.org_a,
        org_role="org:admin",
        alg="none",
    )
    status, resp = await exchange_ws_ticket(h.http_url, none_token, meeting_id)
    assert status == 401

    # 7. HS256 algorithm confusion attack
    hs256_token = h.jwks.mint_token(
        sub="user_hs256",
        org_id=h.org_a,
        org_role="org:admin",
        alg="HS256",
        sign_key=b"secret_key_confusion",
    )
    status, resp = await exchange_ws_ticket(h.http_url, hs256_token, meeting_id)
    assert status == 401


@pytest.mark.asyncio
async def test_t5_key_rotation_dynamic_success(
    gateway_harness: ServerHarness,
) -> None:
    h = gateway_harness
    meeting_id = f"mtg_t5_rot_{uuid.uuid4().hex[:6]}"

    # Rotate key on JWKS server
    rotated_kid = h.jwks.rotate_key(new_kid=f"rotated-{uuid.uuid4().hex[:6]}")

    # Mint token with rotated kid
    rotated_token = h.jwks.mint_token(
        sub="user_rotated",
        org_id=h.org_a,
        org_role="org:admin",
        kid=rotated_kid,
    )

    # Server should dynamically fetch updated JWKS and verify without failure
    status, resp = await exchange_ws_ticket(h.http_url, rotated_token, meeting_id)
    assert status == 200
    assert "ticket" in resp


@pytest.mark.asyncio
async def test_t5_ws_ticket_security_invariants(
    gateway_harness: ServerHarness,
) -> None:
    h = gateway_harness
    meeting_a = f"mtg_t5_a_{uuid.uuid4().hex[:6]}"
    meeting_b = f"mtg_t5_b_{uuid.uuid4().hex[:6]}"

    token = h.jwks.mint_token(sub="user_ticket_test", org_id=h.org_a, org_role="org:admin")

    # 1. Invalid / nonexistent ticket -> 4401 (UNAUTHORIZED)
    fake_uri = f"{h.ws_url}/v1/meetings/{meeting_a}/stream?ticket=nonexistent_ticket_12345"
    with pytest.raises(ConnectionClosed) as exc_fake:
        async with websockets.connect(fake_uri) as ws:
            await ws.recv()
    assert exc_fake.value.rcvd is not None
    assert exc_fake.value.rcvd.code == WSCloseCode.UNAUTHORIZED

    # 2. Ticket issued for meeting A used on meeting B -> 4403 (FORBIDDEN)
    status, resp_a = await exchange_ws_ticket(h.http_url, token, meeting_a, role="ingest")
    assert status == 200
    ticket_a = resp_a["ticket"]

    wrong_mtg_uri = f"{h.ws_url}/v1/meetings/{meeting_b}/stream?ticket={ticket_a}"
    with pytest.raises(ConnectionClosed) as exc_wrong_mtg:
        async with websockets.connect(wrong_mtg_uri) as ws:
            await ws.recv()
    assert exc_wrong_mtg.value.rcvd is not None
    assert exc_wrong_mtg.value.rcvd.code == WSCloseCode.FORBIDDEN

    # 3. Ticket replay -> second connection gets 4401 (UNAUTHORIZED)
    status2, resp2 = await exchange_ws_ticket(h.http_url, token, meeting_a, role="ingest")
    assert status2 == 200
    ticket_valid = resp2["ticket"]
    valid_uri = f"{h.ws_url}/v1/meetings/{meeting_a}/stream?ticket={ticket_valid}"

    # First connection consumes ticket
    async with websockets.connect(valid_uri):
        # Connection established successfully
        pass

    # Second connection tries to replay the consumed ticket
    with pytest.raises(ConnectionClosed) as exc_replay:
        async with websockets.connect(valid_uri) as ws2:
            await ws2.recv()
    assert exc_replay.value.rcvd is not None
    assert exc_replay.value.rcvd.code == WSCloseCode.UNAUTHORIZED
