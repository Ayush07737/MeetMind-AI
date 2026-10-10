"""Acceptance Test Suite - Real Service Fixtures and Harness.

NO MOCKS.
Spawns:
- Real local JWKS HTTP server serving RS256 keys
- Real Neon database throwaway tenant schemas with full migrations applied
- Real Redis (localhost:6379)
- Real Uvicorn server running MeetMind Gateway
"""

from __future__ import annotations

import asyncio
import http.server
import json
import socket
import threading
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import jwt as pyjwt
import pytest
import redis.asyncio as aioredis
from cryptography.hazmat.primitives.asymmetric import rsa
from dotenv import load_dotenv

# Load repository .env
load_dotenv(Path(__file__).resolve().parents[4] / ".env")

from meetmind_security import db  # noqa: E402
from meetmind_security.tenant import TenantConfig  # noqa: E402

# Mark all acceptance tests as integration tests
pytestmark = [pytest.mark.integration]


def get_free_port() -> int:
    """Find an available TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class LocalJWKSServer:
    """Real HTTP server that serves JWKS on localhost for RS256 signature verification."""

    def __init__(self, port: int) -> None:
        self.port = port
        self.issuer = f"http://127.0.0.1:{port}"
        self.jwks_url = f"{self.issuer}/.well-known/jwks.json"

        # Generate primary RS256 keypair
        self.primary_kid = "test-kid-primary"
        self.primary_private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )
        self.keys_by_kid: dict[str, rsa.RSAPrivateKey] = {
            self.primary_kid: self.primary_private_key
        }

        self._server: http.server.HTTPServer | None = None
        self._thread: threading.Thread | None = None

    def _get_jwks_payload(self) -> dict[str, Any]:
        jwk_list = []
        for kid, priv_key in self.keys_by_kid.items():
            pub_key = priv_key.public_key()
            pub_numbers = pub_key.public_numbers()

            def _to_base64url(val: int) -> str:
                byte_len = (val.bit_length() + 7) // 8
                raw_bytes = val.to_bytes(byte_len, "big")
                import base64

                return base64.urlsafe_b64encode(raw_bytes).rstrip(b"=").decode("ascii")

            jwk_list.append(
                {
                    "kty": "RSA",
                    "use": "sig",
                    "alg": "RS256",
                    "kid": kid,
                    "n": _to_base64url(pub_numbers.n),
                    "e": _to_base64url(pub_numbers.e),
                }
            )
        return {"keys": jwk_list}

    def start(self) -> None:
        server_instance = self

        class JWKSHandler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path == "/.well-known/jwks.json":
                    payload = json.dumps(server_instance._get_jwks_payload()).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, format: str, *args: Any) -> None:
                pass  # suppress HTTP server logs in tests

        self._server = http.server.HTTPServer(("127.0.0.1", self.port), JWKSHandler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server.server_close()
        if self._thread:
            self._thread.join(timeout=2.0)

    def rotate_key(self, new_kid: str = "test-kid-rotated") -> str:
        """Add a new keypair to JWKS simulating key rotation."""
        new_priv = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.keys_by_kid[new_kid] = new_priv
        return new_kid

    def mint_token(
        self,
        sub: str = "user_acceptance",
        org_id: str = "org_acceptance_a",
        org_role: str = "org:admin",
        kid: str | None = None,
        exp_offset: int = 3600,
        nbf_offset: int = -10,
        iss: str | None = None,
        azp: str = "https://app.meetmind.ai",
        extra_claims: dict[str, Any] | None = None,
        sign_key: Any = None,
        alg: str = "RS256",
        headers: dict[str, Any] | None = None,
        include_org: bool = True,
    ) -> str:
        """Mint a signed JWT."""
        now = int(datetime.now(UTC).timestamp())
        payload = {
            "sub": sub,
            "iss": iss or self.issuer,
            "azp": azp,
            "exp": now + exp_offset,
            "nbf": now + nbf_offset,
            "iat": now,
        }
        if include_org:
            payload["o"] = {
                "id": org_id,
                "rol": org_role,
                "slg": "acceptance-slug",
            }
        if extra_claims:
            payload.update(extra_claims)

        chosen_kid = kid or self.primary_kid
        token_headers = {"kid": chosen_kid}
        if headers:
            token_headers.update(headers)

        if alg == "none":
            return pyjwt.encode(payload, key="", algorithm="none")

        key_to_use = sign_key or self.keys_by_kid.get(chosen_kid, self.primary_private_key)
        return pyjwt.encode(payload, key_to_use, algorithm=alg, headers=token_headers)


async def _connect_db_with_retry(dsn: str, max_retries: int = 5) -> Any:
    """Connect to Neon with retry for transient network hiccups or cold starts."""
    import asyncpg

    ssl_ctx = db._create_ssl_context(dsn)
    for attempt in range(1, max_retries + 1):
        try:
            return await asyncpg.connect(dsn, ssl=ssl_ctx, timeout=15.0)
        except Exception:
            if attempt == max_retries:
                raise
            await asyncio.sleep(1.0 * attempt)


@dataclass
class ServerHarness:
    http_url: str
    ws_url: str
    port: int
    jwks: LocalJWKSServer
    tenant_a: str
    tenant_b: str
    org_a: str
    org_b: str
    schema_a: str
    schema_b: str

    @asynccontextmanager
    async def tenant_conn(self, tenant_id: str) -> AsyncGenerator[Any, None]:
        """Acquire a direct connection scoped to tenant's schema within current event loop."""
        schema = self.schema_a if tenant_id == self.tenant_a else self.schema_b
        dsn = db.get_owner_dsn()
        conn = await _connect_db_with_retry(dsn)
        try:
            async with conn.transaction():
                await conn.execute(f'SET LOCAL search_path = "{schema}"')
                yield conn
        finally:
            await conn.close()


@pytest.fixture(scope="module")
def jwks_server() -> Any:
    port = get_free_port()
    server = LocalJWKSServer(port)
    server.start()
    yield server
    server.stop()


@pytest.fixture
async def real_redis() -> AsyncGenerator[aioredis.Redis, None]:
    client = aioredis.from_url("redis://localhost:6379/0", decode_responses=False)
    await client.ping()
    yield client
    # Clean up test keys
    keys = await client.keys("mm:*")
    if keys:
        await client.delete(*keys)
    await client.aclose()


@pytest.fixture(scope="module")
async def real_db_tenants() -> AsyncGenerator[tuple[TenantConfig, TenantConfig], None]:
    """Create two isolated test tenant schemas on Neon and apply migrations."""
    from meetmind_security.migrations import apply_migrations_conn

    dsn = db.get_owner_dsn()

    id_a = f"test_a_{uuid.uuid4().hex[:6]}"
    id_b = f"test_b_{uuid.uuid4().hex[:6]}"

    cfg_a = TenantConfig(
        tenant_id=id_a,
        pg_schema=f"tenant_{id_a}",
        neo4j_namespace=id_a,
        qdrant_prefix=id_a,
        redis_prefix=id_a,
    )
    cfg_b = TenantConfig(
        tenant_id=id_b,
        pg_schema=f"tenant_{id_b}",
        neo4j_namespace=id_b,
        qdrant_prefix=id_b,
        redis_prefix=id_b,
    )

    db._router.register(cfg_a)
    db._router.register(cfg_b)

    admin_conn = await _connect_db_with_retry(dsn)
    try:
        await admin_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{cfg_a.pg_schema}"')
        await admin_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{cfg_b.pg_schema}"')
        await apply_migrations_conn(admin_conn, cfg_a.pg_schema)
        await apply_migrations_conn(admin_conn, cfg_b.pg_schema)
    finally:
        await admin_conn.close()

    yield (cfg_a, cfg_b)

    # Teardown
    teardown_conn = await _connect_db_with_retry(dsn)
    try:
        await teardown_conn.execute(f'DROP SCHEMA IF EXISTS "{cfg_a.pg_schema}" CASCADE')
        await teardown_conn.execute(f'DROP SCHEMA IF EXISTS "{cfg_b.pg_schema}" CASCADE')
    finally:
        await teardown_conn.close()


@pytest.fixture(scope="module")
async def gateway_harness(
    jwks_server: LocalJWKSServer,
    real_db_tenants: tuple[TenantConfig, TenantConfig],
) -> AsyncGenerator[ServerHarness, None]:
    """Start real Uvicorn server in an isolated subprocess."""
    import os
    import subprocess
    import sys

    import httpx

    cfg_a, cfg_b = real_db_tenants
    org_a = f"org_{cfg_a.tenant_id}"
    org_b = f"org_{cfg_b.tenant_id}"

    port = get_free_port()
    repo_root = Path(__file__).resolve().parents[4]

    env = os.environ.copy()
    env["CLERK_JWKS_URL"] = jwks_server.jwks_url
    env["CLERK_ISSUER"] = jwks_server.issuer
    env["ALLOWED_ORIGINS"] = json.dumps(["https://app.meetmind.ai"])
    env["TENANT_ORG_MAP"] = json.dumps({org_a: cfg_a.tenant_id, org_b: cfg_b.tenant_id})
    env["ELEVATED_LISTEN_ROLES"] = json.dumps(["org:admin"])
    env["APP_ENV"] = "test"
    env["NEON_DATABASE_URL"] = db.get_owner_dsn()
    env["NEON_APP_DATABASE_URL"] = db.get_owner_dsn()
    env["REDIS_URL"] = "redis://localhost:6379/0"
    env["IDLE_TIMEOUT_SECONDS"] = "30.0"
    env["ALLOW_TOKEN_IN_DEV"] = "false"
    env["LOG_LEVEL"] = "WARNING"
    env["PYTHONPATH"] = (
        str(repo_root / "backend" / "gateway")
        + os.pathsep
        + str(repo_root / "backend" / "security")
        + os.pathsep
        + str(repo_root / "packages" / "shared-schemas" / "schemas_py")
    )

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "meetmind_gateway.app:create_app",
            "--factory",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for /health endpoint to be available
    healthy = False
    for _ in range(150):
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(f"http://127.0.0.1:{port}/health", timeout=1.0)
                if res.status_code == 200:
                    healthy = True
                    break
        except Exception:
            pass

        if proc.poll() is not None:
            _, stderr = proc.communicate()
            raise RuntimeError(
                f"Uvicorn subprocess exited early with code {proc.returncode}:\n"
                f"{stderr.decode('utf-8')}"
            )
        await asyncio.sleep(0.1)

    assert healthy, "Uvicorn subprocess failed to become healthy within timeout"

    harness = ServerHarness(
        http_url=f"http://127.0.0.1:{port}",
        ws_url=f"ws://127.0.0.1:{port}",
        port=port,
        jwks=jwks_server,
        tenant_a=cfg_a.tenant_id,
        tenant_b=cfg_b.tenant_id,
        org_a=org_a,
        org_b=org_b,
        schema_a=cfg_a.pg_schema,
        schema_b=cfg_b.pg_schema,
    )

    yield harness

    proc.terminate()
    try:
        proc.wait(timeout=3.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


async def exchange_ws_ticket(
    http_url: str,
    token: str,
    meeting_id: str,
    role: str = "ingest",
) -> tuple[int, dict[str, Any]]:
    """Exchange token for one-time WebSocket ticket via POST /v1/ws-ticket."""
    import httpx

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{http_url}/v1/ws-ticket",
            headers={"Authorization": f"Bearer {token}"},
            json={"meeting_id": meeting_id, "role": role},
        )
        try:
            return resp.status_code, resp.json()
        except Exception:
            return resp.status_code, {}
