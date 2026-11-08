"""Unit tests verifying Q1 Timing and Clock Control Frames against shared JSON fixtures."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from meetmind_schemas.events import (
    ClockReportControlFrame,
    ClockSyncControlFrame,
    ClockSyncReplyFrame,
    EventTiming,
)
from meetmind_schemas.timing import validate_monotonic
from pydantic import ValidationError

FIXTURES_PATH = Path(__file__).resolve().parents[2] / "fixtures" / "timing_fixtures.json"


@pytest.fixture(scope="module")
def timing_fixtures() -> dict:
    with open(FIXTURES_PATH, encoding="utf-8") as f:
        return json.load(f)


def test_python_valid_clock_sync(timing_fixtures: dict) -> None:
    for frame in timing_fixtures["valid_clock_sync"]:
        parsed = ClockSyncControlFrame.model_validate(frame)
        assert parsed.action == "clock_sync"
        assert parsed.t1 > 0


def test_python_invalid_clock_sync(timing_fixtures: dict) -> None:
    for item in timing_fixtures["invalid_clock_sync"]:
        with pytest.raises(ValidationError):
            ClockSyncControlFrame.model_validate(item["frame"])


def test_python_valid_clock_sync_reply(timing_fixtures: dict) -> None:
    for frame in timing_fixtures["valid_clock_sync_reply"]:
        parsed = ClockSyncReplyFrame.model_validate(frame)
        assert parsed.action == "clock_sync_reply"
        assert parsed.t2 >= parsed.t1


def test_python_invalid_clock_sync_reply(timing_fixtures: dict) -> None:
    for item in timing_fixtures["invalid_clock_sync_reply"]:
        with pytest.raises(ValidationError):
            ClockSyncReplyFrame.model_validate(item["frame"])


def test_python_valid_clock_report(timing_fixtures: dict) -> None:
    for frame in timing_fixtures["valid_clock_report"]:
        parsed = ClockReportControlFrame.model_validate(frame)
        assert parsed.action == "clock_report"
        assert parsed.rtt_ms >= 0


def test_python_invalid_clock_report(timing_fixtures: dict) -> None:
    for item in timing_fixtures["invalid_clock_report"]:
        with pytest.raises(ValidationError):
            ClockReportControlFrame.model_validate(item["frame"])


def test_python_valid_event_timing_and_monotonic(timing_fixtures: dict) -> None:
    for timing in timing_fixtures["valid_event_timing"]:
        parsed = EventTiming.model_validate(timing)
        violations = validate_monotonic(parsed)
        assert len(violations) == 0


def test_python_invalid_event_timing_monotonic(timing_fixtures: dict) -> None:
    for item in timing_fixtures["invalid_event_timing_monotonic"]:
        parsed = EventTiming.model_validate(item["timing"])
        violations = validate_monotonic(parsed)
        assert len(violations) > 0
        assert item["violating_stage"] in violations[0]


def test_python_invalid_event_timing_extra_field(timing_fixtures: dict) -> None:
    for item in timing_fixtures["invalid_event_timing_extra_field"]:
        with pytest.raises(ValidationError):
            EventTiming.model_validate(item["timing"])
