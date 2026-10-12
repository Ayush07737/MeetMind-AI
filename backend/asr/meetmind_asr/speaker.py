"""MeetMind AI — Speaker Name Resolver (WP-D).

Maps audio channel and diarization indices to calendar/participant names.
Invariants & Limitations:
- 2-channel stereo audio: channel 0 is the local user (local_user_name).
- If exactly one remote attendee is listed and exactly one remote speaker index
  is observed on channel 1, map channel 1 to that attendee.
- Single-channel mixed/mono audio or multiple remote attendees: speaker_name is null
  (diarization speaker indices cannot be mapped to participant names without voice enrolment).
- Never blocks on metadata queries.
"""

from __future__ import annotations


class SpeakerNameResolver:
    """Resolves human speaker names from channel index and participant metadata."""

    def __init__(
        self,
        local_user_name: str | None = None,
        attendees: list[str] | None = None,
    ) -> None:
        self.local_user_name = (
            local_user_name.strip() if (local_user_name and local_user_name.strip()) else None
        )
        self.attendees = [a.strip() for a in (attendees or []) if a and a.strip()]
        self._remote_speakers_seen: set[int] = set()

    def resolve(
        self,
        channels: int,
        channel: int | None,
        speaker: int = 0,
    ) -> str | None:
        """Map channel and speaker index to participant name."""
        if channels == 2:
            if channel == 0:
                return self.local_user_name
            elif channel == 1:
                self._remote_speakers_seen.add(speaker)
                if len(self.attendees) == 1 and len(self._remote_speakers_seen) == 1:
                    return self.attendees[0]
                return None

        # 1-channel mono/mixed audio cannot be reliably mapped without voice biometrics
        return None
