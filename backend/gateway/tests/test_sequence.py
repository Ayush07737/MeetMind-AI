"""Tests for MeetMind AI Gateway — Sequence counter."""

import pytest
from meetmind_gateway.sequence import SequenceCounter


class TestSequenceCounter:
    @pytest.fixture
    def counter(self, fake_redis):
        return SequenceCounter(fake_redis)

    async def test_first_increment_returns_one(self, counter):
        seq = await counter.next("mtg_001")
        assert seq == 1

    async def test_increments_monotonically(self, counter):
        seq1 = await counter.next("mtg_001")
        seq2 = await counter.next("mtg_001")
        seq3 = await counter.next("mtg_001")
        assert seq1 == 1
        assert seq2 == 2
        assert seq3 == 3

    async def test_different_meetings_isolated(self, counter):
        await counter.next("mtg_A")
        await counter.next("mtg_A")
        seq_b = await counter.next("mtg_B")
        assert seq_b == 1  # independent counter

    async def test_current_returns_zero_initially(self, counter):
        assert await counter.current("mtg_new") == 0

    async def test_current_returns_last_value(self, counter):
        await counter.next("mtg_001")
        await counter.next("mtg_001")
        assert await counter.current("mtg_001") == 2

    async def test_reset_clears_counter(self, counter):
        await counter.next("mtg_001")
        await counter.next("mtg_001")
        await counter.reset("mtg_001")
        assert await counter.current("mtg_001") == 0
        # After reset, next starts from 1 again
        assert await counter.next("mtg_001") == 1
