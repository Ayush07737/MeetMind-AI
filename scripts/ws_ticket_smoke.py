#!/usr/bin/env python
"""MeetMind AI — End-to-End WebSocket Ticket & Ingestion Smoke Test.

Demonstrates the WP3 authentication & ticket lifecycle:
1. Generate RS256 Clerk JWT signed by private RSA key.
2. Exchange JWT for one-time 128-bit ticket via POST /v1/ws-ticket.
3. Verify replay prevention: second exchange with the same ticket is rejected.
4. Connect to WebSocket stream at /v1/meetings/{meeting_id}/stream?ticket={ticket}.
5. Lifecycle sequence:
   - Send 'meeting_start' control frame -> receive ack
   - Send 'consent_confirmed' control frame -> receive ack (consent gate opened)
   - Send binary PCM audio chunks -> receive monotonically sequenced acks
   - Send 'meeting_end' control frame -> receive ack
6. Graceful disconnect.
"""

from __future__ import annotations

import argparse
import http.server
import json
import logging
import os
import sys
import threading
import time
from typing import Any

import jwt as pyjwt
from cryptography.hazmat.primitives.asymmetric import rsa
from dotenv import load_dotenv
from jwt.algorithms import RSAAlgorithm

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("clerk_smoke")


# ── Ephemeral RSA & JWKS Server for Smoke Demo ───────────────────────────────


def _generate_rsa_keypair():
    priv = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    raw_jwk = RSAAlgorithm.to_jwk(priv.public_key(), as_dict=True)
    raw_jwk.update({"kid": "smoke-key-1", "use": "sig", "alg": "RS256"})
    return priv, raw_jwk


class _JWKSHandler(http.server.BaseHTTPRequestHandler):
    jwks_payload: bytes = b"{}"

    def do_GET(self) -> None:  # noqa: N802
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(self.jwks_payload)

    def log_message(self, *args: Any) -> None:
        pass


def _start_jwks_server(jwk_dict: dict[str, Any]) -> tuple[http.server.HTTPServer, str]:
    _JWKSHandler.jwks_payload = json.dumps({"keys": [jwk_dict]}).encode("utf-8")
    server = http.server.HTTPServer(("127.0.0.1", 0), _JWKSHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{port}/.well-known/jwks.json"


def make_smoke_token(
    priv_key: rsa.RSAPrivateKey,
    kid: str = "smoke-key-1",
    sub: str = "user_smoke_admin",
    tenant_id: str = "default",
) -> str:
    now = int(time.time())
    payload = {
        "sub": sub,
        "org_id": tenant_id,
        "iat": now,
        "nbf": now - 1,
        "exp": now + 3600,
        "azp": "*",
    }
    return pyjwt.encode(
        payload,
        priv_key,
        algorithm="RS256",
        headers={"kid": kid},
    )


# ── Smoke Test Runner ─────────────────────────────────────────────────────────


def run_smoke_test(meeting_id: str = "mtg_smoke_test_001") -> bool:
    print("\n========================================================")
    print(" MeetMind AI — Component #1 WebSocket Ticket Smoke Test")
    print("========================================================\n")

    # 1. Setup ephemeral JWKS and test keys
    logger.info("Step 1: Generating ephemeral RSA256 keypair and JWKS endpoint...")
    priv_key, jwk = _generate_rsa_keypair()
    server, jwks_url = _start_jwks_server(jwk)
    logger.info("JWKS endpoint serving at: %s", jwks_url)

    # 2. Configure gateway application
    logger.info("Step 2: Initializing MeetMind Gateway in development mode...")
    from conftest import FakeRedis
    from fastapi.testclient import TestClient
    from meetmind_gateway.app import create_app
    from meetmind_gateway.auth import ClerkAuthenticator
    from meetmind_gateway.config import GatewaySettings
    from meetmind_gateway.redis_streams import StreamProducer
    from meetmind_gateway.sequence import SequenceCounter
    from meetmind_gateway.session_manager import SessionManager

    fake_redis = FakeRedis()
    db_url = os.getenv("NEON_DATABASE_URL", "postgresql://test:test@localhost/test")
    settings = GatewaySettings(
        neon_database_url=db_url,
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        clerk_jwks_url=jwks_url,
        clerk_secret_key="sk_test_fake_secret_key_32_chars!",
        clerk_publishable_key="pk_test_smoke_key",
        default_tenant_id="default",
        app_env="development",
    )
    app = create_app(settings)
    app.state.redis = fake_redis
    app.state.redis_binary = fake_redis
    app.state.authenticator = ClerkAuthenticator(
        jwks_url=jwks_url,
        default_tenant_id="default",
        app_env="development",
    )
    app.state.session_manager = SessionManager(fake_redis)
    app.state.seq_counter = SequenceCounter(fake_redis)
    app.state.stream_producer = StreamProducer(fake_redis)

    client = TestClient(app)

    # 3. Mint RS256 token and exchange for WS ticket
    logger.info("Step 3: Minting RS256 token and requesting ticket via POST /v1/ws-ticket...")
    token = make_smoke_token(priv_key)
    resp = client.post("/v1/ws-ticket", headers={"Authorization": f"Bearer {token}"})
    if resp.status_code != 200:
        logger.error("Ticket exchange failed with status %d: %s", resp.status_code, resp.text)
        return False
    ticket_payload = resp.json()
    ticket = ticket_payload.get("ticket")
    exp_in = ticket_payload.get("expires_in", 30)
    logger.info("Ticket successfully granted: %s (expires in %ds)", ticket, exp_in)

    # 4. Connect to WebSocket stream with ticket
    ws_url = f"/v1/meetings/{meeting_id}/stream?ticket={ticket}"
    logger.info("Step 4: Connecting to WebSocket at %s...", ws_url)
    with client.websocket_connect(ws_url) as ws:
        logger.info("WebSocket handshake accepted!")

        # 5. Send meeting_start control frame
        logger.info("Step 5: Sending 'meeting_start' control frame...")
        ws.send_text(json.dumps({"type": "control", "action": "meeting_start"}))
        ack_start = json.loads(ws.receive_text())
        logger.info("Received meeting_start ack: %s", ack_start)
        assert ack_start.get("status") == "accepted"

        # 6. Send consent_confirmed control frame
        logger.info("Step 6: Sending 'consent_confirmed' control frame...")
        ws.send_text(
            json.dumps(
                {
                    "type": "control",
                    "action": "consent_confirmed",
                    "payload": {"consent_type": "audio_capture"},
                }
            )
        )
        ack_consent = json.loads(ws.receive_text())
        logger.info("Received consent_confirmed ack: %s", ack_consent)
        assert ack_consent.get("status") == "accepted"

        # 7. Send binary audio chunk
        logger.info("Step 7: Streaming binary audio chunk (16-bit PCM 16kHz)...")
        pcm_chunk = b"\x00\x01\x02\x03" * 256  # 1KB mock audio
        ws.send_bytes(pcm_chunk)
        ack_audio = json.loads(ws.receive_text())
        logger.info("Received audio chunk ack: %s (seq=%s)", ack_audio, ack_audio.get("seq"))
        assert ack_audio.get("status") == "acked"
        assert ack_audio.get("seq") == 1

        # 8. Send meeting_end control frame
        logger.info("Step 8: Sending 'meeting_end' control frame...")
        ws.send_text(json.dumps({"type": "control", "action": "meeting_end"}))
        ack_end = json.loads(ws.receive_text())
        logger.info("Received meeting_end ack: %s", ack_end)
        assert ack_end.get("status") == "accepted"

        logger.info("Closing WebSocket session...")

    # 9. Verify ticket reuse is blocked (atomic GETDEL verification)
    logger.info("Step 9: Verifying replay attack protection (reusing ticket %s)...", ticket)
    from meetmind_schemas.events import WSCloseCode
    from starlette.websockets import WebSocketDisconnect

    try:
        with client.websocket_connect(ws_url):
            logger.error("Security failure: Replayed ticket was accepted!")
            return False
    except WebSocketDisconnect as exc:
        logger.info("Ticket reuse successfully rejected with WSCloseCode: %s", exc.code)
        assert exc.code == WSCloseCode.UNAUTHORIZED

    print("\n--------------------------------------------------------")
    print(" ALL SMOKE TEST CHECKS PASSED SUCCESSFULLY (WP1-WP3)")
    print("--------------------------------------------------------\n")
    server.shutdown()
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MeetMind AI WebSocket Ticket Smoke Test")
    parser.add_argument("--meeting-id", default="mtg_smoke_test_001", help="Meeting ID to test")
    args = parser.parse_args()

    # Ensure backend gateway is in path
    sys.path.insert(0, "backend/gateway")
    sys.path.insert(0, "backend/gateway/tests")

    success = run_smoke_test(args.meeting_id)
    sys.exit(0 if success else 1)
