"""Unit tests for meetmind_quality.cache."""

import pytest
from meetmind_quality.cache import InMemoryContextCache, PrefetchHint


@pytest.mark.asyncio
async def test_in_memory_cache_get_set():
    cache = InMemoryContextCache()
    await cache.set("tenant_eng", "ticket:PLAT-101", {"title": "Slow query", "assignee": "Vikram"})

    val = await cache.get("tenant_eng", "ticket:PLAT-101")
    assert val is not None
    assert val["title"] == "Slow query"

    # Different tenant isolation
    val_other = await cache.get("tenant_sales", "ticket:PLAT-101")
    assert val_other is None


@pytest.mark.asyncio
async def test_in_memory_cache_prefetch():
    cache = InMemoryContextCache()
    hint = PrefetchHint(
        entity_type="ticket",
        entity_id="PLAT-101",
        tenant_id="tenant_eng",
        meeting_id="mtg_01",
        confidence=0.9,
        triggered_at_ms=1000,
        warm_keys=["ticket:PLAT-101", "pr:PR-482"],
    )

    ok = await cache.prefetch(hint)
    assert ok is True

    # Check warm keys were seeded
    cached_ticket = await cache.get("tenant_eng", "ticket:PLAT-101")
    assert cached_ticket is not None
    assert cached_ticket["prefetched"] is True
    assert cached_ticket["entity_id"] == "PLAT-101"

    # Delete
    await cache.delete("tenant_eng", "ticket:PLAT-101")
    assert await cache.get("tenant_eng", "ticket:PLAT-101") is None
