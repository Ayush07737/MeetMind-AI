"""Unit tests for Transcript Deduplication (WP-D).

Verifies:
- LRU utterance_id cache deduplication
- Time-overlap with equal normalized text deduplication on the same channel
- Multi-channel distinction (same text overlapping on different channels is NOT a duplicate)
- Fault injection simulation (simulated replay duplicates)
"""

from __future__ import annotations

from meetmind_asr.dedupe import Deduplicator, normalize_text
from meetmind_schemas.events import compute_utterance_id


def test_normalize_text():
    assert normalize_text("  Hello,  World!  ") == "hello world"
    assert normalize_text("Action item: follow-up with team.") == "action item followup with team"
    assert normalize_text("") == ""


def test_dedupe_by_utterance_id():
    dedup = Deduplicator(lru_capacity=10)
    uid = compute_utterance_id("meet-1", 0, 0, 100)

    # First time: not a duplicate
    assert not dedup.is_duplicate(uid, channel=0, text="hello world", start_ms=100, end_ms=200)

    # Record published
    dedup.record_published(uid, channel=0, text="hello world", start_ms=100, end_ms=200)

    # Second time with same utterance_id: duplicate!
    assert dedup.is_duplicate(uid, channel=0, text="hello world", start_ms=100, end_ms=200)


def test_dedupe_lru_eviction():
    dedup = Deduplicator(lru_capacity=2)
    uid1 = "u1"
    uid2 = "u2"
    uid3 = "u3"

    dedup.record_published(uid1, 0, "text 1", 0, 100)
    dedup.record_published(uid2, 0, "text 2", 100, 200)
    # uid1 is in cache
    assert dedup.is_duplicate(uid1, 0, "different text", 500, 600)

    # Adding uid3 should evict uid1
    dedup.record_published(uid3, 0, "text 3", 200, 300)
    assert not dedup.is_duplicate(uid1, 0, "different text", 500, 600)
    assert dedup.is_duplicate(uid2, 0, "different text", 500, 600)
    assert dedup.is_duplicate(uid3, 0, "different text", 500, 600)


def test_dedupe_time_overlap_equal_text():
    dedup = Deduplicator()
    # Published final from 1000ms to 2500ms: "welcome to the meeting"
    uid1 = "uid_orig"
    dedup.record_published(
        uid1, channel=0, text="Welcome to the meeting!", start_ms=1000, end_ms=2500
    )

    # Replay creates a slightly different hash/start_ms (e.g. 1100ms to 2400ms) with the same text
    uid2 = "uid_replay"
    # Overlapping interval [1100, 2400] overlaps with [1000, 2500]
    assert dedup.is_duplicate(
        uid2, channel=0, text="welcome to the meeting", start_ms=1100, end_ms=2400
    )

    # Same text but completely non-overlapping time [3000, 4000] is NOT a duplicate
    uid3 = "uid_later"
    assert not dedup.is_duplicate(
        uid3, channel=0, text="welcome to the meeting", start_ms=3000, end_ms=4000
    )

    # Different channel with overlap is NOT a duplicate
    assert not dedup.is_duplicate(
        uid2, channel=1, text="welcome to the meeting", start_ms=1100, end_ms=2400
    )


def test_fault_injection_replay_scenario():
    """Simulates provider crash replay where chunks produce overlapping finals."""
    dedup = Deduplicator()

    # Step 1: Normal utterance produced
    uid_a = compute_utterance_id("m1", 0, 0, 1000)
    assert not dedup.is_duplicate(uid_a, 0, "Let us begin the discussion.", 1000, 2500)
    dedup.record_published(uid_a, 0, "Let us begin the discussion.", 1000, 2500)

    # Step 2: Fault injection / reconnect replays audio. Provider generates exact duplicate:
    assert dedup.is_duplicate(uid_a, 0, "Let us begin the discussion.", 1000, 2500)

    # Step 3: Provider generates partial overlap with identical words:
    uid_b = compute_utterance_id("m1", 0, 0, 1050)
    assert dedup.is_duplicate(uid_b, 0, "Let us begin the discussion", 1050, 2520)

    # Step 4: Next actual sentence is processed normally:
    uid_c = compute_utterance_id("m1", 0, 0, 2600)
    assert not dedup.is_duplicate(uid_c, 0, "First topic on the agenda.", 2600, 4000)
    dedup.record_published(uid_c, 0, "First topic on the agenda.", 2600, 4000)
