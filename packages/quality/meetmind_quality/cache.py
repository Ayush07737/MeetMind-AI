"""MeetMind AI — Quality Context Cache & Prefetch Interface (Q5).

Defines interfaces and concrete implementations (in-memory and Redis-backed)
for speculative prefetching and warm context caching across the intelligence pipeline.
"""

from __future__ import annotations

import abc
import json
import time
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class PrefetchHint(BaseModel):
    """Speculative entity-keyed warm context prefetch hint (Q5)."""

    model_config = ConfigDict(extra="forbid")

    entity_type: str  # ticket | pull_request | commit | incident | adr | service
    entity_id: str  # e.g. PLAT-101, PR-482, INC-892
    tenant_id: str
    meeting_id: str
    confidence: float = Field(ge=0.0, le=1.0)
    triggered_at_ms: int
    warm_keys: list[str] = Field(default_factory=list)


class ContextCache(abc.ABC):
    """Abstract interface for tiered context caching."""

    @abc.abstractmethod
    async def get(self, tenant_id: str, key: str) -> dict[str, Any] | None:
        """Retrieve cached context item by tenant and key."""

    @abc.abstractmethod
    async def set(
        self, tenant_id: str, key: str, value: dict[str, Any], ttl_seconds: int = 300
    ) -> None:
        """Store context item with tenant isolation and TTL."""

    @abc.abstractmethod
    async def prefetch(self, hint: PrefetchHint) -> bool:
        """Eagerly warm cache entries indicated by prefetch hint."""

    @abc.abstractmethod
    async def delete(self, tenant_id: str, key: str) -> None:
        """Evict cached context item."""


class InMemoryContextCache(ContextCache):
    """In-memory dictionary cache with TTL expiry for testing and local operation."""

    def __init__(self) -> None:
        self._store: dict[str, tuple[dict[str, Any], float]] = {}

    def _format_key(self, tenant_id: str, key: str) -> str:
        return f"{tenant_id}:{key}"

    async def get(self, tenant_id: str, key: str) -> dict[str, Any] | None:
        composite = self._format_key(tenant_id, key)
        if composite not in self._store:
            return None
        val, expiry = self._store[composite]
        if time.time() > expiry:
            del self._store[composite]
            return None
        return val

    async def set(
        self, tenant_id: str, key: str, value: dict[str, Any], ttl_seconds: int = 300
    ) -> None:
        composite = self._format_key(tenant_id, key)
        expiry = time.time() + ttl_seconds
        self._store[composite] = (value, expiry)

    async def prefetch(self, hint: PrefetchHint) -> bool:
        # Pre-seed warm keys with placeholder metadata if not already present
        now = time.time()
        for k in hint.warm_keys:
            composite = self._format_key(hint.tenant_id, k)
            if composite not in self._store:
                self._store[composite] = (
                    {
                        "entity_type": hint.entity_type,
                        "entity_id": hint.entity_id,
                        "prefetched": True,
                        "timestamp_ms": hint.triggered_at_ms,
                    },
                    now + 120.0,
                )
        return True

    async def delete(self, tenant_id: str, key: str) -> None:
        composite = self._format_key(tenant_id, key)
        self._store.pop(composite, None)


class RedisContextCache(ContextCache):
    """Redis-backed context cache with strict tenant prefixing."""

    def __init__(self, redis_client: Any) -> None:
        self._redis = redis_client

    def _format_key(self, tenant_id: str, key: str) -> str:
        return f"{tenant_id}:context:{key}"

    async def get(self, tenant_id: str, key: str) -> dict[str, Any] | None:
        redis_key = self._format_key(tenant_id, key)
        raw = await self._redis.get(redis_key)
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json.loads(raw)

    async def set(
        self, tenant_id: str, key: str, value: dict[str, Any], ttl_seconds: int = 300
    ) -> None:
        redis_key = self._format_key(tenant_id, key)
        payload = json.dumps(value)
        await self._redis.set(redis_key, payload, ex=ttl_seconds)

    async def prefetch(self, hint: PrefetchHint) -> bool:
        pipe = self._redis.pipeline()
        for k in hint.warm_keys:
            redis_key = self._format_key(hint.tenant_id, k)
            payload = json.dumps(
                {
                    "entity_type": hint.entity_type,
                    "entity_id": hint.entity_id,
                    "prefetched": True,
                    "timestamp_ms": hint.triggered_at_ms,
                }
            )
            pipe.set(redis_key, payload, ex=120, nx=True)
        await pipe.execute()
        return True

    async def delete(self, tenant_id: str, key: str) -> None:
        redis_key = self._format_key(tenant_id, key)
        await self._redis.delete(redis_key)
