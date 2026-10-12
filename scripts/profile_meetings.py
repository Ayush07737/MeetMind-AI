"""Profile POST /v1/meetings at 25 and 100 concurrent requests against a 4-worker uvicorn server.

Measures:
1. Client-side end-to-end RTT (Min, P50, P95, P99, Max)
2. Per-stage breakdown from Server-Timing headers:
   - JWT verify (signature + JWKS lookup)
   - Tenant resolve
   - Redis ops (SessionManager.create_meeting)
   - Server total
"""

import asyncio
import base64
import contextlib
import http.server
import json
import os
import socket
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
import jwt as pyjwt
from cryptography.hazmat.primitives.asymmetric import rsa

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "backend" / "gateway"))
sys.path.insert(0, str(REPO_ROOT / "backend" / "security"))
sys.path.insert(0, str(REPO_ROOT / "packages" / "shared-schemas" / "schemas_py"))


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class ThreadedJWKSServer:
    def __init__(self, port: int) -> None:
        self.port = port
        self.issuer = f"http://127.0.0.1:{port}"
        self.jwks_url = f"{self.issuer}/.well-known/jwks.json"
        self.kid = "profile-kid"
        self.priv_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self._server: http.server.ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def _get_payload(self) -> dict[str, Any]:
        pub_numbers = self.priv_key.public_key().public_numbers()

        def b64url(val: int) -> str:
            raw = val.to_bytes((val.bit_length() + 7) // 8, "big")
            return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

        return {
            "keys": [
                {
                    "kty": "RSA",
                    "use": "sig",
                    "alg": "RS256",
                    "kid": self.kid,
                    "n": b64url(pub_numbers.n),
                    "e": b64url(pub_numbers.e),
                }
            ]
        }

    def start(self) -> None:
        srv = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path == "/.well-known/jwks.json":
                    data = json.dumps(srv._get_payload()).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, format: str, *args: Any) -> None:
                pass

        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", self.port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server.server_close()
        if self._thread:
            self._thread.join(timeout=2.0)

    def mint_token(self, org_id: str = "org_prof") -> str:
        now = int(time.time())
        return pyjwt.encode(
            {
                "sub": "user_profile",
                "iss": self.issuer,
                "azp": "https://app.meetmind.ai",
                "iat": now,
                "nbf": now,
                "exp": now + 3600,
                "o": {"id": org_id, "rol": "org:admin"},
            },
            self.priv_key,
            algorithm="RS256",
            headers={"kid": self.kid},
        )


def parse_server_timing(header: str) -> dict[str, float]:
    timings = {}
    for part in header.split(","):
        part = part.strip()
        if ";" in part:
            name, dur_part = part.split(";", 1)
            if dur_part.startswith("dur="):
                with contextlib.suppress(ValueError):
                    timings[name.strip()] = float(dur_part[4:])
    return timings


async def run_stage_benchmark(
    base_url: str,
    token: str,
    concurrency: int,
) -> dict[str, Any]:
    limits = httpx.Limits(max_connections=300, max_keepalive_connections=200)
    async with httpx.AsyncClient(limits=limits, timeout=30.0) as client:
        # Pre-warm connection
        async def make_one(idx: int) -> dict[str, Any]:
            mid = f"mtg_prof_{concurrency}_{idx}_{uuid.uuid4().hex[:6]}"
            t0 = time.perf_counter()
            resp = await client.post(
                f"{base_url}/v1/meetings",
                headers={"Authorization": f"Bearer {token}"},
                json={"meeting_id": mid, "local_user_name": f"User {idx}"},
            )
            t_client = (time.perf_counter() - t0) * 1000
            st_header = resp.headers.get("server-timing", "")
            stages = parse_server_timing(st_header)
            return {
                "status": resp.status_code,
                "client_ms": t_client,
                "jwt_ms": stages.get("jwt", 0.0),
                "tenant_ms": stages.get("tenant", 0.0),
                "redis_ms": stages.get("redis", 0.0),
                "server_total_ms": stages.get("total", 0.0),
            }

        tasks = [make_one(i) for i in range(concurrency)]
        results = await asyncio.gather(*tasks)

    # Compute percentiles
    def p(vals: list[float], pct: float) -> float:
        s = sorted(vals)
        idx = int(len(s) * (pct / 100.0))
        return s[min(idx, len(s) - 1)]

    client_times = [r["client_ms"] for r in results]
    jwt_times = [r["jwt_ms"] for r in results]
    tenant_times = [r["tenant_ms"] for r in results]
    redis_times = [r["redis_ms"] for r in results]
    server_times = [r["server_total_ms"] for r in results]

    return {
        "concurrency": concurrency,
        "success_count": sum(1 for r in results if r["status"] == 201),
        "client_ms": {
            "min": round(min(client_times), 2),
            "p50": round(p(client_times, 50), 2),
            "p95": round(p(client_times, 95), 2),
            "p99": round(p(client_times, 99), 2),
            "max": round(max(client_times), 2),
        },
        "jwt_ms": {
            "p50": round(p(jwt_times, 50), 2),
            "p95": round(p(jwt_times, 95), 2),
        },
        "tenant_ms": {
            "p50": round(p(tenant_times, 50), 2),
            "p95": round(p(tenant_times, 95), 2),
        },
        "redis_ms": {
            "p50": round(p(redis_times, 50), 2),
            "p95": round(p(redis_times, 95), 2),
        },
        "server_total_ms": {
            "p50": round(p(server_times, 50), 2),
            "p95": round(p(server_times, 95), 2),
        },
    }


async def main() -> None:
    print("=" * 70)
    print(" MeetMind AI - P0-6 Auth & Meeting Creation Profiling Under Load")
    print("=" * 70)

    jwks_port = get_free_port()
    jwks_server = ThreadedJWKSServer(jwks_port)
    jwks_server.start()
    print(f"JWKS Server started on port {jwks_port}")

    org_id = "org_prof_123"
    tenant_id = "tenant_prof_123"
    token = jwks_server.mint_token(org_id)

    gw_port = get_free_port()
    env = os.environ.copy()
    env["CLERK_JWKS_URL"] = jwks_server.jwks_url
    env["CLERK_ISSUER"] = jwks_server.issuer
    env["ALLOWED_ORIGINS"] = json.dumps(["https://app.meetmind.ai"])
    env["TENANT_ORG_MAP"] = json.dumps({org_id: tenant_id})
    env["APP_ENV"] = "test"
    env["AUDIO_REDIS_URL"] = "redis://127.0.0.1:6379/0"
    env["REDIS_URL"] = "redis://127.0.0.1:6379/0"
    env["AUDIO_REDIS_PERSISTENCE_ACKNOWLEDGED"] = "true"
    env["PYTHONPATH"] = (
        str(REPO_ROOT / "backend" / "gateway")
        + os.pathsep
        + str(REPO_ROOT / "backend" / "security")
        + os.pathsep
        + str(REPO_ROOT / "packages" / "shared-schemas" / "schemas_py")
    )

    print(f"Starting 4-worker uvicorn server on port {gw_port}...")
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "meetmind_gateway.app:create_app",
            "--factory",
            "--workers",
            "4",
            "--host",
            "127.0.0.1",
            "--port",
            str(gw_port),
            "--log-level",
            "warning",
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        # Wait for server readiness
        base_url = f"http://127.0.0.1:{gw_port}"
        async with httpx.AsyncClient(timeout=10.0) as client:
            ready = False
            for _ in range(50):
                try:
                    r = await client.get(f"{base_url}/health")
                    if r.status_code == 200:
                        ready = True
                        break
                except Exception:
                    await asyncio.sleep(0.1)
            if not ready:
                raise RuntimeError("Uvicorn 4-worker server failed to start")
            print("4-worker server is ready.\n")

            # Warmup single request
            warm_resp = await client.post(
                f"{base_url}/v1/meetings",
                headers={"Authorization": f"Bearer {token}"},
                json={"meeting_id": "mtg_warmup"},
            )
            assert warm_resp.status_code == 201

            # Run 25 concurrency benchmark
            print("Running benchmark at 25 concurrent requests...")
            res25 = await run_stage_benchmark(base_url, token, 25)

            # Cool down
            await asyncio.sleep(1.0)

            # Run 100 concurrency benchmark
            print("Running benchmark at 100 concurrent requests...")
            res100 = await run_stage_benchmark(base_url, token, 100)

            # Print results table
            print("\n" + "=" * 70)
            print(" PROFILING RESULTS SUMMARY (POST /v1/meetings across 4 workers)")
            print("=" * 70)
            print(f"{'Metric':<25} | {'25 Concurrency':<20} | {'100 Concurrency':<20}")

            def row(label: str, v25: str, v100: str) -> None:
                print(f"{label:<25} | {v25:<20} | {v100:<20}")

            row("Success Count", f"{res25['success_count']}/25", f"{res100['success_count']}/100")
            row(
                "Client RTT P50",
                f"{res25['client_ms']['p50']} ms",
                f"{res100['client_ms']['p50']} ms",
            )
            row(
                "Client RTT P95",
                f"{res25['client_ms']['p95']} ms",
                f"{res100['client_ms']['p95']} ms",
            )
            row(
                "Client RTT P99",
                f"{res25['client_ms']['p99']} ms",
                f"{res100['client_ms']['p99']} ms",
            )
            row(
                "Client RTT Max",
                f"{res25['client_ms']['max']} ms",
                f"{res100['client_ms']['max']} ms",
            )
            print("-" * 70)
            row(
                "Server Total P50",
                f"{res25['server_total_ms']['p50']} ms",
                f"{res100['server_total_ms']['p50']} ms",
            )
            row(
                "Server Total P95",
                f"{res25['server_total_ms']['p95']} ms",
                f"{res100['server_total_ms']['p95']} ms",
            )
            print("-" * 70)
            row("JWT Verify P50", f"{res25['jwt_ms']['p50']} ms", f"{res100['jwt_ms']['p50']} ms")
            row("JWT Verify P95", f"{res25['jwt_ms']['p95']} ms", f"{res100['jwt_ms']['p95']} ms")
            row(
                "Tenant Resolve P50",
                f"{res25['tenant_ms']['p50']} ms",
                f"{res100['tenant_ms']['p50']} ms",
            )
            row(
                "Tenant Resolve P95",
                f"{res25['tenant_ms']['p95']} ms",
                f"{res100['tenant_ms']['p95']} ms",
            )
            row(
                "Redis Ops P50",
                f"{res25['redis_ms']['p50']} ms",
                f"{res100['redis_ms']['p50']} ms",
            )
            row(
                "Redis Ops P95",
                f"{res25['redis_ms']['p95']} ms",
                f"{res100['redis_ms']['p95']} ms",
            )
            print("=" * 70)

    finally:
        proc.kill()
        proc.wait()
        jwks_server.stop()


if __name__ == "__main__":
    asyncio.run(main())
