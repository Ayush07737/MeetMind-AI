"""Tests for centralized Redis key schemes (WP4) and guard against hardcoded prefixes."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
from meetmind_gateway.keys import (
    audio_stream_key,
    events_stream_key,
    last_seq_key,
    listeners_key,
    meeting_session_key,
    meeting_tenant_binding_key,
    out_stream_key,
    transcript_stream_key,
    user_connections_key,
    validate_key_segment,
    writer_lease_key,
    ws_ticket_key,
)


class TestKeysValidation:
    def test_valid_key_generation(self):
        tenant = "tenant_123"
        meeting = "mtg_456"

        assert audio_stream_key(tenant, meeting) == "mm:tenant_123:audio:mtg_456"
        assert transcript_stream_key(tenant, meeting) == "mm:tenant_123:transcript:mtg_456"
        assert events_stream_key(tenant, meeting) == "mm:tenant_123:events:mtg_456"
        assert out_stream_key(tenant, meeting) == "mm:tenant_123:out:mtg_456"
        assert meeting_session_key(tenant, meeting) == "mm:tenant_123:meeting:mtg_456"
        assert writer_lease_key(tenant, meeting) == "mm:tenant_123:writer:mtg_456"
        assert last_seq_key(tenant, meeting) == "mm:tenant_123:lastseq:mtg_456"
        assert listeners_key(tenant, meeting) == "mm:tenant_123:listeners:mtg_456"
        assert meeting_tenant_binding_key(meeting) == "mm:binding:mtg_456"
        assert ws_ticket_key("tick_abc") == "mm:ticket:tick_abc"
        assert user_connections_key(tenant, "u1") == "mm:tenant_123:userconns:u1"

    @pytest.mark.parametrize(
        "invalid_segment",
        [
            "",
            "tenant:injection",
            "tenant with space",
            "a" * 65,  # exceeds 64 chars
            "tenant/slash",
            "tenant$bad",
        ],
    )
    def test_invalid_key_segments_rejected(self, invalid_segment):
        with pytest.raises(ValueError, match="Must match"):
            validate_key_segment(invalid_segment)

        with pytest.raises(ValueError):
            audio_stream_key(invalid_segment, "mtg_valid")

        with pytest.raises(ValueError):
            audio_stream_key("tenant_valid", invalid_segment)


class TestKeysPrefixGuard:
    """Guard test: fails if any gateway module outside keys.py contains a key-prefix literal."""

    FORBIDDEN_PREFIXES = ("audio:", "events:", "transcript:", "listeners:", "session:")

    def test_no_forbidden_key_prefixes_outside_keys_py(self):
        gateway_pkg = Path(__file__).resolve().parents[1] / "meetmind_gateway"
        py_files = list(gateway_pkg.glob("*.py"))
        violations: list[str] = []

        for p in py_files:
            if p.name == "keys.py":
                continue

            content = p.read_text(encoding="utf-8")
            tree = ast.parse(content, filename=str(p))

            for node in ast.walk(tree):
                # Check string literals
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    for prefix in self.FORBIDDEN_PREFIXES:
                        if prefix in node.value:
                            violations.append(
                                f"{p.name}:{node.lineno} contains forbidden prefix '{prefix}' "
                                f"in literal '{node.value}'"
                            )

        assert not violations, "Found hardcoded key prefixes outside keys.py:\n" + "\n".join(
            violations
        )
