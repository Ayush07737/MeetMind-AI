"""Pytest configuration for backend/asr/tests.

Handles conditional skipping of live Deepgram tests based on RUN_DEEPGRAM_TESTS=1.
"""

from __future__ import annotations

import os

import pytest


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if os.environ.get("RUN_DEEPGRAM_TESTS") != "1":
        skip_deepgram = pytest.mark.skip(
            reason="Live Deepgram tests skipped unless RUN_DEEPGRAM_TESTS=1"
        )
        for item in items:
            if "deepgram" in item.keywords:
                item.add_marker(skip_deepgram)
