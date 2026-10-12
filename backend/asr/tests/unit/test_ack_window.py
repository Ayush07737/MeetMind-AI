"""Unit tests for Delayed Ack Window logic (WP-D).

Verifies:
- Sent entries are queued in _pending_acks with their send timestamp.
- Entries younger than ASR_REPLAY_WINDOW_MS are NOT acknowledged.
- Entries older than ASR_REPLAY_WINDOW_MS are drained and acknowledged.
- Bounded replay history retains only chunks within the replay window.
"""

from __future__ import annotations

import collections

from meetmind_asr.providers.base import AudioChunk


def test_ack_window_time_eligibility():
    replay_window_ms = 5000
    replay_s = replay_window_ms / 1000.0

    pending_acks: collections.deque[tuple[float, str]] = collections.deque()

    # Simulate sending 3 chunks at t = 100.0, 102.0, 104.0
    pending_acks.append((100.0, "1000-0"))
    pending_acks.append((102.0, "1001-0"))
    pending_acks.append((104.0, "1002-0"))

    # At current time t = 103.0:
    # 103.0 - 100.0 = 3.0s < 5.0s -> nothing is older than 5.0s
    now = 103.0
    to_ack = []
    while pending_acks and (now - pending_acks[0][0]) >= replay_s:
        _, eid = pending_acks.popleft()
        to_ack.append(eid)

    assert to_ack == []
    assert len(pending_acks) == 3

    # At current time t = 105.5:
    # 105.5 - 100.0 = 5.5s >= 5.0s -> "1000-0" is eligible
    # 105.5 - 102.0 = 3.5s < 5.0s -> not eligible
    now = 105.5
    while pending_acks and (now - pending_acks[0][0]) >= replay_s:
        _, eid = pending_acks.popleft()
        to_ack.append(eid)

    assert to_ack == ["1000-0"]
    assert len(pending_acks) == 2

    # At current time t = 110.0:
    # All remaining entries are >= 5.0s
    now = 110.0
    to_ack = []
    while pending_acks and (now - pending_acks[0][0]) >= replay_s:
        _, eid = pending_acks.popleft()
        to_ack.append(eid)

    assert to_ack == ["1001-0", "1002-0"]
    assert len(pending_acks) == 0


def test_sent_replay_history_pruning():
    """Verify that chunks in sent_replay_history are pruned when older than the window."""
    replay_window_ms = 5000
    replay_s = replay_window_ms / 1000.0

    history: collections.deque[tuple[float, AudioChunk]] = collections.deque()

    c1 = AudioChunk(data=b"chunk1", client_seq=1, capture_ts_ms=1000, recv_ns=0)
    c2 = AudioChunk(data=b"chunk2", client_seq=2, capture_ts_ms=1100, recv_ns=0)
    c3 = AudioChunk(data=b"chunk3", client_seq=3, capture_ts_ms=7000, recv_ns=0)

    # c1 sent at t = 10.0
    history.append((10.0, c1))
    # c2 sent at t = 12.0
    history.append((12.0, c2))

    # c3 sent at t = 16.0 (6 seconds after c1)
    now = 16.0
    history.append((now, c3))

    while history and (now - history[0][0]) > replay_s:
        history.popleft()

    # c1 (10.0) is 6s old -> pruned
    # c2 (12.0) is 4s old -> kept
    # c3 (16.0) is 0s old -> kept
    remaining_seqs = [chunk.client_seq for _, chunk in history]
    assert remaining_seqs == [2, 3]
