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
from dataclasses import dataclass


@dataclass(frozen=True)
class ChunkAnchor:
    """Anchors a chunk's provider session offset to its capture timestamp."""

    session_start_ms: int
    session_end_ms: int
    capture_ts_ms: int


class TimelineMapper:
    """Maps provider session offsets to the absolute and relative capture timeline."""

    def __init__(self) -> None:
        self.meeting_origin_capture_ts: int | None = None
        self._anchors: list[ChunkAnchor] = []
        self._current_session_audio_ms: int = 0

    def register_chunk(
        self,
        capture_ts_ms: int,
        duration_ms: int = 100,
    ) -> None:
        """Register an audio chunk sent to the current provider session."""
        if self.meeting_origin_capture_ts is None:
            self.meeting_origin_capture_ts = capture_ts_ms

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

    def reset_session(self) -> None:
        """Reset provider session cumulative audio on reconnect (retains meeting origin)."""
        self._anchors.clear()
        self._current_session_audio_ms = 0

    def map_offset(self, provider_offset_ms: int) -> tuple[int, int]:
        """Convert provider session offset into (relative_start_ms, absolute_epoch_ms).

        Returns:
            (start_ms relative to meeting origin, start_epoch_ms)
        """
        if self.meeting_origin_capture_ts is None:
            return 0, 0

        if not self._anchors:
            # Fallback if no anchors registered yet
            epoch_ts = self.meeting_origin_capture_ts + provider_offset_ms
            return provider_offset_ms, epoch_ts

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
