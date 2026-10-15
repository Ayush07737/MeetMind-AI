"""Unit tests for ASR reconciler loop (P0-4).

No network: uses mocked Redis and Service components.
Verifies:
1. Reconciler claims only unleased meetings (via SSCAN and pipelined lease GETs).
2. Reconciler respects ASR_MAX_MEETINGS_PER_WORKER.
3. Reconciler handles claim races (Lua CAS loser backs off).
4. Reconciler handles multi-batch SSCAN pagination.
"""

from __future__ import annotations

import asyncio
import contextlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from meetmind_asr.config import AsrSettings
from meetmind_asr.service import AsrService


@pytest.fixture
def mock_asr_service() -> AsrService:
    settings = AsrSettings(
        asr_provider="stub",
        asr_worker_id="worker_test_1",
        asr_max_meetings_per_worker=2,
        asr_reconcile_interval_ms=10,
        asr_owner_lease_ttl_ms=3000,
        asr_health_port=0,
    )
    with patch("meetmind_asr.service.aioredis"):
        svc = AsrService(settings)
        svc.redis = AsyncMock()
        return svc


@pytest.mark.asyncio
async def test_reconciler_claims_only_unleased_meetings(mock_asr_service: AsrService) -> None:
    svc = mock_asr_service
    svc._is_running = True

    # SSCAN returns 2 meetings: m1 (unleased) and m2 (already owned by worker_2)
    svc.redis.sscan = AsyncMock(return_value=(0, [b"t1|m1", b"t1|m2"]))

    # Pipelined GETs return: None for m1, b"worker_2" for m2
    pipe_mock = MagicMock()
    pipe_mock.get = MagicMock()
    pipe_mock.execute = AsyncMock(return_value=[None, b"worker_2"])
    svc.redis.pipeline = MagicMock(return_value=pipe_mock)

    def _acquire_side_effect(t, m):
        svc._is_running = False
        return True

    svc._acquire_lease = AsyncMock(side_effect=_acquire_side_effect)
    svc._start_pipeline = AsyncMock()

    # Run reconciler for one iteration
    task = asyncio.create_task(svc._reconcile_loop())
    await asyncio.sleep(0.05)
    svc._is_running = False
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    # Assertions
    pipe_mock.get.assert_any_call("mm:t1:asr_owner:m1")
    pipe_mock.get.assert_any_call("mm:t1:asr_owner:m2")
    # Only m1 was unleased -> _acquire_lease called once for m1
    svc._acquire_lease.assert_called_once_with("t1", "m1")
    svc._start_pipeline.assert_called_once_with("t1", "m1", started_via="reconciler")


@pytest.mark.asyncio
async def test_reconciler_respects_max_meetings_per_worker(mock_asr_service: AsrService) -> None:
    svc = mock_asr_service
    svc._is_running = True
    svc.settings.asr_max_meetings_per_worker = 1

    # Worker already has 1 active meeting -> at capacity
    svc._active_pipelines["m_existing"] = MagicMock()

    svc.redis.sscan = AsyncMock(return_value=(0, [b"t1|m_new"]))
    svc._acquire_lease = AsyncMock()
    svc._start_pipeline = AsyncMock()

    task = asyncio.create_task(svc._reconcile_loop())
    await asyncio.sleep(0.05)
    svc._is_running = False
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    # Should not even attempt SSCAN or acquire
    assert svc._acquire_lease.call_count == 0
    assert svc._start_pipeline.call_count == 0


@pytest.mark.asyncio
async def test_reconciler_handles_claim_race_loser_backs_off(mock_asr_service: AsrService) -> None:
    svc = mock_asr_service
    svc._is_running = True

    svc.redis.sscan = AsyncMock(return_value=(0, [b"t1|m1"]))
    pipe_mock = MagicMock()
    pipe_mock.execute = AsyncMock(return_value=[None])  # unleased at inspection
    svc.redis.pipeline = MagicMock(return_value=pipe_mock)

    # Another worker secured the lease between GET and CAS -> _acquire_lease returns False
    def _race_side_effect(t, m):
        svc._is_running = False
        return False

    svc._acquire_lease = AsyncMock(side_effect=_race_side_effect)
    svc._start_pipeline = AsyncMock()

    task = asyncio.create_task(svc._reconcile_loop())
    await asyncio.sleep(0.05)
    svc._is_running = False
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    svc._acquire_lease.assert_called_once_with("t1", "m1")
    # Did not secure lease -> do NOT start pipeline
    assert svc._start_pipeline.call_count == 0


@pytest.mark.asyncio
async def test_reconciler_sscan_cursor_iteration(mock_asr_service: AsrService) -> None:
    svc = mock_asr_service
    svc._is_running = True

    # 2 pages: first returns cursor "42", second returns cursor "0"
    svc.redis.sscan = AsyncMock(
        side_effect=[
            (b"42", [b"t1|m1"]),
            (b"0", [b"t1|m2"]),
            (b"0", []),
        ]
    )
    pipe_mock = MagicMock()
    pipe_mock.execute = AsyncMock(return_value=[None])
    svc.redis.pipeline = MagicMock(return_value=pipe_mock)

    svc._acquire_lease = AsyncMock(return_value=True)
    svc._start_pipeline = AsyncMock()

    task = asyncio.create_task(svc._reconcile_loop())
    await asyncio.sleep(0.05)
    svc._is_running = False
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert svc.redis.sscan.call_count >= 2
