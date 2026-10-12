"""Unit tests for TimelineMapper (WP-D).

Verifies:
- Linear mapping without gaps
- Client-seq gaps do not drift timestamps
- Provider reconnect resets session offset but preserves meeting timeline
- Replay overlap mapping
- Multiple reconnects
"""

from __future__ import annotations

from meetmind_asr.timeline import TimelineMapper


def test_timeline_linear_progression():
    tm = TimelineMapper()
    # Meeting starts at epoch 1_000_000 ms
    tm.register_chunk(capture_ts_ms=1_000_000, duration_ms=100)
    tm.register_chunk(capture_ts_ms=1_000_100, duration_ms=100)
    tm.register_chunk(capture_ts_ms=1_000_200, duration_ms=100)

    # Provider reports transcript at offset 50ms..150ms in session
    start_ms, end_ms, epoch_ms = tm.map_range(50, 150)
    assert start_ms == 50
    assert end_ms == 150
    assert epoch_ms == 1_000_050


def test_timeline_client_seq_gap():
    """Client drops audio or network packet lost: capture_ts jumps by 500ms."""
    tm = TimelineMapper()
    # Chunk 0 at 1000ms
    tm.register_chunk(capture_ts_ms=1000, duration_ms=100)
    # Gap: next chunk is at 1600ms (500ms gap)
    tm.register_chunk(capture_ts_ms=1600, duration_ms=100)

    # Provider offset 0..100 maps to chunk 0
    s0, e0, epoch0 = tm.map_range(0, 100)
    assert s0 == 0
    assert e0 == 100
    assert epoch0 == 1000

    # Provider offset 100..200 maps to chunk 1 (starts at capture_ts 1600, delta from 1000 is 600ms)
    s1, e1, epoch1 = tm.map_range(100, 200)
    assert s1 == 600
    assert e1 == 700
    assert epoch1 == 1600


def test_timeline_reconnect_preserves_origin():
    """Provider disconnects and reconnects: session offset resets to 0."""
    tm = TimelineMapper()
    # Session 1: 500ms of audio
    for i in range(5):
        tm.register_chunk(capture_ts_ms=1000 + i * 100, duration_ms=100)

    # Provider session 1 reports at offset 200..300
    s_pre, e_pre, epoch_pre = tm.map_range(200, 300)
    assert s_pre == 200
    assert e_pre == 300
    assert epoch_pre == 1200

    # Reconnect occurs! Session resets
    tm.reset_session()

    # Session 2 begins at capture_ts 1600 (100ms after session 1 + some downtime)
    tm.register_chunk(capture_ts_ms=1600, duration_ms=100)
    tm.register_chunk(capture_ts_ms=1700, duration_ms=100)

    # Provider session 2 reports at its offset 0..100
    s_post, e_post, epoch_post = tm.map_range(0, 100)
    assert s_post == 600  # 1600 - 1000
    assert e_post == 700
    assert epoch_post == 1600


def test_timeline_replay_overlap():
    """Replaying 200ms of audio after reconnect."""
    tm = TimelineMapper()
    # Session 1
    tm.register_chunk(capture_ts_ms=2000, duration_ms=100)
    tm.register_chunk(capture_ts_ms=2100, duration_ms=100)

    tm.reset_session()

    # Replay chunk from 2100ms
    tm.register_chunk(capture_ts_ms=2100, duration_ms=100)
    # New chunk at 2200ms
    tm.register_chunk(capture_ts_ms=2200, duration_ms=100)

    # In session 2, provider offset 0..100 corresponds to replayed chunk 2100ms
    s_rep, e_rep, epoch_rep = tm.map_range(0, 100)
    assert s_rep == 100  # 2100 - 2000
    assert epoch_rep == 2100

    # Provider offset 100..200 corresponds to new chunk 2200ms
    s_new, e_new, epoch_new = tm.map_range(100, 200)
    assert s_new == 200  # 2200 - 2000
    assert epoch_new == 2200


def test_timeline_multiple_reconnects():
    """Two reconnects in a single meeting."""
    tm = TimelineMapper()
    # Session 1
    tm.register_chunk(capture_ts_ms=5000, duration_ms=100)
    assert tm.map_range(0, 100) == (0, 100, 5000)

    # Reconnect 1
    tm.reset_session()
    tm.register_chunk(capture_ts_ms=5300, duration_ms=100)
    assert tm.map_range(0, 100) == (300, 400, 5300)

    # Reconnect 2
    tm.reset_session()
    tm.register_chunk(capture_ts_ms=5800, duration_ms=100)
    assert tm.map_range(0, 100) == (800, 900, 5800)
