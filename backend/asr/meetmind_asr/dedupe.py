"""MeetMind AI — Transcript Deduplication (WP-D).

Replayed audio produced during worker crashes or provider reconnects can
generate duplicate finals.
Deduplicates:
1. By utterance_id (deterministic hash with per-meeting LRU cache).
2. By time-overlap on the same channel with equal normalized text.
"""

from __future__ import annotations

import collections
import re
from dataclasses import dataclass


def normalize_text(text: str) -> str:
    """Normalize text by stripping whitespace, punctuation, and lowercasing."""
    cleaned = re.sub(r"[^\w\s]", "", text).strip().lower()
    return " ".join(cleaned.split())


@dataclass(frozen=True)
class PublishedFinal:
    """Record of a published transcript_final."""

    utterance_id: str
    channel: int
    normalized_text: str
    start_ms: int
    end_ms: int


class Deduplicator:
    """Per-meeting deduplicator for transcript_final events."""

    def __init__(self, lru_capacity: int = 1000) -> None:
        self._lru_capacity = lru_capacity
        self._seen_utterance_ids: collections.OrderedDict[str, bool] = collections.OrderedDict()
        self._recent_finals_by_channel: dict[int, list[PublishedFinal]] = collections.defaultdict(
            list
        )

    def is_duplicate(
        self,
        utterance_id: str,
        channel: int | None,
        text: str,
        start_ms: int,
        end_ms: int,
    ) -> bool:
        """Check if final is a duplicate by utterance_id or time-overlap with equal text."""
        # 1. Check utterance_id LRU
        if utterance_id in self._seen_utterance_ids:
            return True

        # 2. Check time-overlap with equal text on the same channel
        ch = channel if channel is not None else 0
        norm_candidate = normalize_text(text)
        if not norm_candidate:
            return False

        for existing in self._recent_finals_by_channel[ch]:
            if existing.normalized_text == norm_candidate:
                # Check for time interval overlap:
                # [start_ms, end_ms] intersects [existing.start_ms, existing.end_ms]
                overlap_start = max(start_ms, existing.start_ms)
                overlap_end = min(end_ms, existing.end_ms)
                if overlap_start < overlap_end:
                    return True

        return False

    def record_published(
        self,
        utterance_id: str,
        channel: int | None,
        text: str,
        start_ms: int,
        end_ms: int,
    ) -> None:
        """Mark final as published in LRU cache and recent channel history."""
        # Update LRU
        self._seen_utterance_ids[utterance_id] = True
        if len(self._seen_utterance_ids) > self._lru_capacity:
            self._seen_utterance_ids.popitem(last=False)

        ch = channel if channel is not None else 0
        norm_text = normalize_text(text)
        history = self._recent_finals_by_channel[ch]
        history.append(
            PublishedFinal(
                utterance_id=utterance_id,
                channel=ch,
                normalized_text=norm_text,
                start_ms=start_ms,
                end_ms=end_ms,
            )
        )
        # Keep recent history bounded (last 50 finals per channel)
        if len(history) > 50:
            history.pop(0)
