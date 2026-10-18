"""MeetMind AI — Timeline Mapper (WP-D).

Maintains a per-meeting mapping from provider session offsets to the original capture timeline.
Guarantees that:
1. Reconnects do not shift timestamps (provider session offset resets to 0,
   but capture timeline continues).
2. Client sequence gaps do not drift timestamps.
3. Replayed audio chunks retain their original capture timestamps.
4. start_ms and end_ms are non-negative offsets from the meeting's first observed capture timestamp.
5. start_epoch_ms is the absolute epoch millisecond timestamp.
"""

from __future__ import annotations

import bisect
import collections
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ChunkAnchor:
    """Anchors a chunk's provider session offset to its capture timestamp."""

    session_start_ms: int
    session_end_ms: int
    capture_ts_ms: int


class TimelineMapper:
    """Maps provider session offsets to the absolute and relative capture timeline.

    Uses a sliding window (default 120 seconds) to prevent unbounded memory growth
    during long-running meetings, while retaining per-session origin offsets.
    """

    def __init__(self, window_seconds: int = 120) -> None:
        self.meeting_origin_capture_ts: int | None = None
        self._window_ms = window_seconds * 1000
        self._anchors: collections.deque[ChunkAnchor] = collections.deque()
        self._current_session_audio_ms: int = 0
        self._session_origin_capture_ts: int | None = None
        self._session_origin_offset_ms: int = 0
        self._last_pruned_anchor: ChunkAnchor | None = None

    def register_chunk(
        self,
        capture_ts_ms: int,
        duration_ms: int = 100,
    ) -> None:
        """Register an audio chunk sent to the current provider session."""
        if self.meeting_origin_capture_ts is None:
            self.meeting_origin_capture_ts = capture_ts_ms
        if self._session_origin_capture_ts is None:
            self._session_origin_capture_ts = capture_ts_ms
            self._session_origin_offset_ms = self._current_session_audio_ms

        start_offset = self._current_session_audio_ms
        end_offset = start_offset + duration_ms
        self._anchors.append(
            ChunkAnchor(
                session_start_ms=start_offset,
                session_end_ms=end_offset,
                capture_ts_ms=capture_ts_ms,
            )
        )
        self._current_session_audio_ms = end_offset

        # Sliding window pruning: prune anchors older than 120s
        cutoff_ms = self._current_session_audio_ms - self._window_ms
        while len(self._anchors) > 10 and self._anchors[0].session_end_ms < cutoff_ms:
            self._last_pruned_anchor = self._anchors.popleft()

    def reset_session(self) -> None:
        """Reset provider session cumulative audio on reconnect (retains meeting origin)."""
        self._anchors.clear()
        self._current_session_audio_ms = 0
        self._session_origin_capture_ts = None
        self._session_origin_offset_ms = 0
        self._last_pruned_anchor = None

    def map_offset(self, provider_offset_ms: int) -> tuple[int, int]:
        """Convert provider session offset into (relative_start_ms, absolute_epoch_ms).

        Returns:
            (start_ms relative to meeting origin, start_epoch_ms)
        """
        if self.meeting_origin_capture_ts is None:
            return 0, 0

        if not self._anchors:
            base_capture = self._session_origin_capture_ts or self.meeting_origin_capture_ts
            epoch_ts = base_capture + provider_offset_ms
            return max(0, epoch_ts - self.meeting_origin_capture_ts), epoch_ts

        # If offset is older than the oldest anchor in sliding window,
        # extrapolate from pruned anchor
        if provider_offset_ms < self._anchors[0].session_start_ms:
            ref_anchor = self._last_pruned_anchor or self._anchors[0]
            delta = provider_offset_ms - ref_anchor.session_start_ms
            epoch_ts = ref_anchor.capture_ts_ms + delta
            start_ms = max(0, epoch_ts - self.meeting_origin_capture_ts)
            return start_ms, epoch_ts

        # Binary search for matching anchor
        start_offsets = [a.session_start_ms for a in self._anchors]
        idx = bisect.bisect_right(start_offsets, provider_offset_ms) - 1
        idx = max(0, min(idx, len(self._anchors) - 1))
        anchor = self._anchors[idx]

        delta = max(0, provider_offset_ms - anchor.session_start_ms)
        epoch_ts = anchor.capture_ts_ms + delta
        start_ms = max(0, epoch_ts - self.meeting_origin_capture_ts)
        return start_ms, epoch_ts

    def map_range(
        self,
        provider_start_ms: int,
        provider_end_ms: int,
    ) -> tuple[int, int, int]:
        """Map provider session (start_ms, end_ms) range.

        Returns:
            (meeting_start_ms, meeting_end_ms, start_epoch_ms)
        """
        start_ms, start_epoch_ms = self.map_offset(provider_start_ms)
        duration = max(0, provider_end_ms - provider_start_ms)
        end_ms = start_ms + duration
        return start_ms, end_ms, start_epoch_ms
