"""Shared test fixtures for MeetMind AI Gateway (§1)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from meetmind_gateway.auth import AuthenticatedUser, ClerkAuthenticator
from meetmind_gateway.config import GatewaySettings
from meetmind_gateway.redis_streams import StreamProducer
from meetmind_gateway.sequence import SequenceCounter
from meetmind_gateway.session_manager import SessionManager
from tests.jwt_test_utils import (
    TEST_JWKS_URL,
)

# ── Fake Redis ────────────────────────────────────────────────────────────────


class FakeRedis:
    """Minimal in-memory Redis mock for unit tests."""

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}
        self._streams: dict[str, list[tuple[str, dict]]] = {}
        self._stream_counter: dict[str, int] = {}
        self._dollar_cursors: dict[str, str] = {}
        self._pubsub_channels: dict[str, list] = {}

    async def get(self, key: str) -> str | None:
        return self._data.get(key)

    async def getdel(self, key: str) -> str | None:
        val = self._data.get(key)
        if key in self._data:
            del self._data[key]
        return val

    async def set(
        self,
        key: str,
        value: Any,
        ex: int | None = None,
        px: int | None = None,
        nx: bool = False,
    ) -> bool:
        if nx and key in self._data:
            return False
        self._data[key] = str(value)
        return True

    async def expire(self, key: str, time: int) -> bool:
        return key in self._data

    async def pexpire(self, key: str, time: int) -> bool:
        return key in self._data

    async def execute_command(self, cmd: str, *args: Any) -> Any:
        if cmd.upper() == "GETDEL":
            return await self.getdel(args[0])
        raise NotImplementedError(f"FakeRedis doesn't support command {cmd}")

    async def incr(self, key: str) -> int:
        val = int(self._data.get(key, 0)) + 1
        self._data[key] = str(val)
        return val

    async def delete(self, *keys: str) -> int:
        count = 0
        for k in keys:
            if k in self._data:
                del self._data[k]
                count += 1
            if k in self._streams:
                del self._streams[k]
                count += 1
        return count

    async def exists(self, key: str) -> int:
        return 1 if key in self._data else 0

    async def hset(
        self,
        key: str,
        field: str | None = None,
        value: Any = None,
        mapping: dict | None = None,
    ) -> int:
        if key not in self._data:
            self._data[key] = {}
        if mapping:
            self._data[key].update(mapping)
            return len(mapping)
        if field is not None:
            self._data[key][field] = value
            return 1
        return 0

    async def hget(self, key: str, field: str) -> str | None:
        h = self._data.get(key, {})
        return h.get(field)

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self._data.get(key, {}))

    async def sadd(self, key: str, *values: str) -> int:
        if key not in self._data:
            self._data[key] = set()
        before = len(self._data[key])
        self._data[key].update(values)
        return len(self._data[key]) - before

    async def srem(self, key: str, *values: str) -> int:
        s = self._data.get(key, set())
        count = 0
        for v in values:
            if v in s:
                s.discard(v)
                count += 1
        return count

    async def scard(self, key: str) -> int:
        return len(self._data.get(key, set()))

    async def xadd(self, stream: str, fields: dict, maxlen: int | None = None) -> str:
        if stream not in self._streams:
            self._streams[stream] = []
            self._stream_counter[stream] = 0
        self._stream_counter[stream] += 1
        entry_id = f"1-{self._stream_counter[stream]}"
        self._streams[stream].append((entry_id, fields))
        if maxlen and len(self._streams[stream]) > maxlen:
            self._streams[stream] = self._streams[stream][-maxlen:]
        return entry_id

    async def xrevrange(
        self, stream: str, max: str = "+", min: str = "-", count: int | None = None
    ) -> list[tuple[str, dict]]:
        entries = self._streams.get(stream, [])
        rev = list(reversed(entries))
        if count:
            rev = rev[:count]
        return rev

    async def xgroup_create(
        self, stream: str, group: str, id: str = "0", mkstream: bool = False
    ) -> None:
        if stream not in self._streams and mkstream:
            self._streams[stream] = []

    async def xread(
        self,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict]]]]:
        results = []
        for stream, last_id in streams.items():
            entries = self._streams.get(stream, [])
            if last_id == "$":
                if stream not in self._dollar_cursors:
                    self._dollar_cursors[stream] = entries[-1][0] if entries else "0-0"
                    selected = []
                else:
                    base_id = self._dollar_cursors[stream]
                    selected = [e for e in entries if e[0] > base_id]
                    if selected:
                        self._dollar_cursors[stream] = selected[-1][0]
            elif last_id in ("0", "0-0"):
                selected = entries
            else:
                selected = [e for e in entries if e[0] > last_id]
            if count and len(selected) > count:
                selected = selected[:count]
            if selected:
                results.append((stream, selected))
        if not results and block:
            await asyncio.sleep(min(block / 1000.0, 0.05))
        return results

    def pubsub(self) -> FakePubSub:
        return FakePubSub(self)

    async def publish(self, channel: str, message: str) -> int:
        listeners = self._pubsub_channels.get(channel, [])
        for q in listeners:
            await q.put({"type": "message", "channel": channel, "data": message})
        return len(listeners)

    async def aclose(self) -> None:
        pass


class FakePubSub:
    """Fake pub-sub for testing."""

    def __init__(self, redis: FakeRedis) -> None:
        self._redis = redis
        self._queue: asyncio.Queue = asyncio.Queue()
        self._channels: list[str] = []

    async def subscribe(self, *channels: str) -> None:
        for ch in channels:
            self._channels.append(ch)
            if ch not in self._redis._pubsub_channels:
                self._redis._pubsub_channels[ch] = []
            self._redis._pubsub_channels[ch].append(self._queue)

    async def unsubscribe(self, *channels: str) -> None:
        for ch in channels or self._channels:
            if (
                ch in self._redis._pubsub_channels
                and self._queue in self._redis._pubsub_channels[ch]
            ):
                self._redis._pubsub_channels[ch].remove(self._queue)

    async def listen(self):
        while True:
            msg = await self._queue.get()
            yield msg

    async def aclose(self) -> None:
        pass


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def fake_redis() -> FakeRedis:
    """Return a fresh FakeRedis instance."""
    return FakeRedis()


@pytest.fixture
def settings() -> GatewaySettings:
    """Return test settings configured with local test JWKS."""
    return GatewaySettings(
        neon_database_url="postgresql://test:test@localhost/test",
        redis_url="redis://localhost:6379/0",
        clerk_jwks_url=TEST_JWKS_URL,
        clerk_secret_key="sk_test_fake_key_for_testing",
        clerk_publishable_key="pk_test_fake_key_for_testing",
        default_tenant_id="test_tenant",
        app_env="development",
    )


@pytest.fixture
def authenticator(settings: GatewaySettings) -> ClerkAuthenticator:
    """Return a ClerkAuthenticator pointing to the local test JWKS server."""
    return ClerkAuthenticator(
        jwks_url=settings.clerk_jwks_url,
        default_tenant_id=settings.default_tenant_id,
        app_env="development",
    )


@pytest.fixture
def seq_counter(fake_redis: FakeRedis) -> SequenceCounter:
    return SequenceCounter(fake_redis)


@pytest.fixture
def stream_producer(fake_redis: FakeRedis) -> StreamProducer:
    return StreamProducer(fake_redis, maxlen=1000)


@pytest.fixture
def session_manager(fake_redis: FakeRedis) -> SessionManager:
    return SessionManager(fake_redis)


@pytest.fixture
def authenticated_user() -> AuthenticatedUser:
    """A pre-built authenticated user for tests."""
    return AuthenticatedUser(
        user_id="user_test_123",
        tenant_id="test_tenant",
        session_id="sess_test_456",
        claims={"sub": "user_test_123", "org_id": "test_tenant"},
    )
