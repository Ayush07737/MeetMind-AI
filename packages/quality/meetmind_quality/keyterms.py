"""MeetMind AI — KeytermProvider Interface & Domain Pack Vocabulary (Q5).

Enforces dynamic keyterm injection for ASR models with a strict 100-keyterm limit
per Deepgram API specification.
"""

from __future__ import annotations

import abc

MAX_DEEPGRAM_KEYTERMS: int = 100


class KeytermProvider(abc.ABC):
    """Abstract interface for supplying ASR keyterms."""

    @abc.abstractmethod
    async def get_keyterms(self, tenant_id: str, domain: str | None = None) -> list[str]:
        """Return list of keyterms capped at MAX_DEEPGRAM_KEYTERMS (100)."""


class StaticKeytermProvider(KeytermProvider):
    """Static keyterm provider with deterministic sorting and 100-term capping."""

    def __init__(self, keyterms: list[str]) -> None:
        # Deduplicate while preserving order, then cap at 100
        seen = set()
        deduped = []
        for kt in keyterms:
            s = kt.strip()
            if s and s not in seen:
                seen.add(s)
                deduped.append(s)
        self._keyterms = deduped[:MAX_DEEPGRAM_KEYTERMS]

    async def get_keyterms(self, tenant_id: str, domain: str | None = None) -> list[str]:
        return list(self._keyterms)


ENGINEERING_KEYTERMS: list[str] = [
    # Platforms & Infra
    "Kubernetes",
    "Kafka",
    "PostgreSQL",
    "Redis",
    "Redis Streams",
    "CRDT",
    "gRPC",
    "Docker",
    "AWS",
    "GCP",
    "Terraform",
    "OpenSearch",
    "Elasticsearch",
    "Envoy",
    "Nginx",
    "Prometheus",
    "Grafana",
    "Datadog",
    "Jaeger",
    "OpenTelemetry",
    # Architecture & Design
    "ADR",
    "EventEnvelope",
    "Monorepo",
    "Microservice",
    "WebSocket",
    "REST",
    "GraphQL",
    "OAuth",
    "JWT",
    "SAML",
    "mTLS",
    "CircuitBreaker",
    "Idempotency",
    "Sharding",
    "Replication",
    "Failover",
    "RPO",
    "RTO",
    "SLA",
    "SLO",
    "SLI",
    # Engineering metrics & units
    "p50",
    "p95",
    "p99",
    "rps",
    "ms",
    "throughput",
    "latency",
    "headroom",
    "jitter",
    "backpressure",
    # Ticket & PR prefixes
    "PLAT",
    "ENG",
    "FRONT",
    "INFRA",
    "DATA",
    "CORE",
    "TECH",
    "SEC",
    "INC",
    "PR",
    # Tools & Languages
    "Python",
    "TypeScript",
    "Go",
    "Rust",
    "Pydantic",
    "Zod",
    "FastAPI",
    "Uvicorn",
    "Node.js",
    "Vite",
    "Vitest",
    "Pytest",
    "Ruff",
    "Prettier",
    "ESLint",
    "Git",
    "GitHub",
    "CI/CD",
]


class EngineeringKeytermProvider(KeytermProvider):
    """Provides Engineering & Development domain pack keyterms."""

    def __init__(self, tenant_overrides: dict[str, list[str]] | None = None) -> None:
        self._overrides = tenant_overrides or {}

    async def get_keyterms(self, tenant_id: str, domain: str | None = None) -> list[str]:
        custom = self._overrides.get(tenant_id, [])
        combined = list(custom) + list(ENGINEERING_KEYTERMS)

        seen = set()
        deduped = []
        for kt in combined:
            s = kt.strip()
            if s and s not in seen:
                seen.add(s)
                deduped.append(s)

        # Strict 100-keyterm cap enforcement
        return deduped[:MAX_DEEPGRAM_KEYTERMS]
