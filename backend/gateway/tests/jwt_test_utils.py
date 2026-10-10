"""Shared test RSA keys and ephemeral JWKS server for MeetMind Gateway unit tests."""

from __future__ import annotations

import http.server
import json
import threading
import time
from typing import Any

import jwt as pyjwt
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

# ── Global Test RSA Keys & JWKS Server ────────────────────────────────────────

TEST_RSA_PRIV_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_raw_jwk = RSAAlgorithm.to_jwk(TEST_RSA_PRIV_KEY.public_key(), as_dict=True)
_raw_jwk.update({"kid": "test-key-1", "use": "sig", "alg": "RS256"})
TEST_RSA_JWK = _raw_jwk


class _JWKSHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"keys": [TEST_RSA_JWK]}).encode("utf-8"))

    def log_message(self, format: str, *args: Any) -> None:
        pass


_jwks_httpd = http.server.HTTPServer(("127.0.0.1", 0), _JWKSHandler)
_jwks_port = _jwks_httpd.server_address[1]
_jwks_thread = threading.Thread(target=_jwks_httpd.serve_forever, daemon=True)
_jwks_thread.start()
TEST_JWKS_URL = f"http://127.0.0.1:{_jwks_port}/.well-known/jwks.json"


def make_test_jwt(
    sub: str = "user_test",
    org_id: str = "test_tenant",
    exp_offset: int = 3600,
    **extra_claims: Any,
) -> str:
    """Create a valid RS256 JWT signed with the test private key."""
    payload = {
        "sub": sub,
        "org_id": org_id,
        "exp": int(time.time()) + exp_offset,
        "iat": int(time.time()),
        **extra_claims,
    }
    return pyjwt.encode(
        payload,
        TEST_RSA_PRIV_KEY,
        algorithm="RS256",
        headers={"kid": "test-key-1"},
    )
