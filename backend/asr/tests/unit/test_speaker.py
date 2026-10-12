"""Unit tests for SpeakerNameResolver (WP-D).

Verifies:
- 2 channels: channel 0 maps to local_user_name
- 2 channels: channel 1 maps to the single remote attendee if exactly 1 remote speaker seen
- 2 channels: channel 1 is null if multiple remote attendees or multiple remote speakers seen
- 1 channel (mono/mixed): speaker_name is always None
- Graceful handling of missing/null metadata
"""

from __future__ import annotations

from meetmind_asr.speaker import SpeakerNameResolver


def test_stereo_local_user_mapping():
    resolver = SpeakerNameResolver(
        local_user_name="Alice Smith",
        attendees=["Bob Jones"],
    )
    # Channel 0 always maps to local_user_name in stereo mode
    assert resolver.resolve(channels=2, channel=0, speaker=0) == "Alice Smith"
    assert resolver.resolve(channels=2, channel=0, speaker=1) == "Alice Smith"


def test_stereo_single_remote_attendee():
    resolver = SpeakerNameResolver(
        local_user_name="Alice Smith",
        attendees=["Bob Jones"],
    )
    # Channel 1 with exactly one speaker seen maps to Bob Jones
    assert resolver.resolve(channels=2, channel=1, speaker=0) == "Bob Jones"
    # Same speaker again still maps
    assert resolver.resolve(channels=2, channel=1, speaker=0) == "Bob Jones"


def test_stereo_multiple_remote_speakers():
    resolver = SpeakerNameResolver(
        local_user_name="Alice Smith",
        attendees=["Bob Jones"],
    )
    # First remote speaker seen
    assert resolver.resolve(channels=2, channel=1, speaker=0) == "Bob Jones"
    # Second remote speaker seen on channel 1 -> becomes ambiguous!
    assert resolver.resolve(channels=2, channel=1, speaker=1) is None


def test_stereo_multiple_remote_attendees():
    resolver = SpeakerNameResolver(
        local_user_name="Alice Smith",
        attendees=["Bob Jones", "Charlie Brown"],
    )
    # Multiple remote attendees listed -> cannot disambiguate remote channel without enrolment
    assert resolver.resolve(channels=2, channel=1, speaker=0) is None


def test_mono_always_null():
    resolver = SpeakerNameResolver(
        local_user_name="Alice Smith",
        attendees=["Bob Jones"],
    )
    # 1 channel mono/mixed audio cannot map to names
    assert resolver.resolve(channels=1, channel=0, speaker=0) is None
    assert resolver.resolve(channels=1, channel=None, speaker=0) is None


def test_null_or_empty_metadata():
    resolver = SpeakerNameResolver(local_user_name=None, attendees=None)
    assert resolver.resolve(channels=2, channel=0, speaker=0) is None
    assert resolver.resolve(channels=2, channel=1, speaker=0) is None

    resolver_empty = SpeakerNameResolver(local_user_name="", attendees=["  "])
    assert resolver_empty.resolve(channels=2, channel=0, speaker=0) is None
    assert resolver_empty.resolve(channels=2, channel=1, speaker=0) is None
