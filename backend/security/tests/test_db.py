"""Tests for MeetMind AI - Neon Database Connection Pool.

Verifies:
1. SSL context enforcement (TLS required for Neon)
2. Fail-fast error when NEON_DATABASE_URL is missing
3. Correct resolution when NEON_DATABASE_URL is provided
"""

import ssl
from unittest.mock import patch

import pytest
from meetmind_security.db import _create_ssl_context, _require_neon_url


class TestNeonDatabaseConfig:
    def test_create_ssl_context_enforces_tls(self):
        ctx = _create_ssl_context()
        assert isinstance(ctx, ssl.SSLContext)
        assert ctx.check_hostname is True
        assert ctx.verify_mode == ssl.CERT_REQUIRED

    def test_require_neon_url_raises_when_missing(self):
        with (
            patch("meetmind_security.db.NEON_DATABASE_URL", None),
            pytest.raises(RuntimeError, match="NEON_DATABASE_URL is not set"),
        ):
            _require_neon_url()

    def test_require_neon_url_raises_when_empty(self):
        with (
            patch("meetmind_security.db.NEON_DATABASE_URL", ""),
            pytest.raises(RuntimeError, match="NEON_DATABASE_URL is not set"),
        ):
            _require_neon_url()

    def test_require_neon_url_returns_valid_url(self):
        fake = "postgresql://meetmind:pwd@ep-test.neon.tech/meetmind_ai?sslmode=require"
        with patch("meetmind_security.db.NEON_DATABASE_URL", fake):
            assert _require_neon_url() == fake
