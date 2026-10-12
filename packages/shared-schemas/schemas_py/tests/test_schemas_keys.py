"""Unit tests for meetmind_schemas.keys."""

import pytest
from meetmind_schemas.keys import (
    JANITOR_LEASE_KEY,
    LIFECYCLE_STREAM_KEY,
    asr_owner_lease_key,
    audio_stream_key,
    events_stream_key,
    janitor_lock_key,
    last_seq_key,
    lifecycle_stream_key,
    listeners_key,
    meeting_session_key,
    metrics_key,
    out_stream_key,
    tenant_connections_key,
    transcript_stream_key,
    user_connections_key,
    validate_key_segment,
    validate_tenant_segment,
    writer_lease_key,
    ws_ticket_key,
)


def test_keys_formatting():
    tenant = "tenant_test1"
    meeting = "mtg_abc123"

    assert audio_stream_key(tenant, meeting) == "mm:tenant_test1:audio:mtg_abc123"
    assert transcript_stream_key(tenant, meeting) == "mm:tenant_test1:transcript:mtg_abc123"
    assert events_stream_key(tenant, meeting) == "mm:tenant_test1:events:mtg_abc123"
    assert out_stream_key(tenant, meeting) == "mm:tenant_test1:out:mtg_abc123"
    assert meeting_session_key(tenant, meeting) == "mm:tenant_test1:meeting:mtg_abc123"
    assert writer_lease_key(tenant, meeting) == "mm:tenant_test1:writer:mtg_abc123"
    assert asr_owner_lease_key(tenant, meeting) == "mm:tenant_test1:asr_owner:mtg_abc123"
    assert last_seq_key(tenant, meeting) == "mm:tenant_test1:seq:mtg_abc123"
    assert listeners_key(tenant, meeting) == "mm:tenant_test1:listeners:mtg_abc123"
    assert metrics_key(tenant, meeting) == "mm:tenant_test1:metrics:mtg_abc123"
    assert ws_ticket_key("tkt_12345") == "mm:ticket:tkt_12345"
    assert user_connections_key(tenant, "user_123") == "mm:tenant_test1:userconns:user_123"
    assert tenant_connections_key(tenant) == "mm:tenant_test1:conns"
    assert lifecycle_stream_key() == LIFECYCLE_STREAM_KEY == "mm:system:lifecycle"
    assert janitor_lock_key() == JANITOR_LEASE_KEY == "mm:global:janitor:lease"


def test_validate_tenant_segment():
    assert validate_tenant_segment("  TENANT_ABC  ") == "tenant_abc"
    with pytest.raises(ValueError):
        validate_tenant_segment("")
    with pytest.raises(ValueError):
        validate_tenant_segment("invalid tenant with spaces")


def test_validate_key_segment():
    assert validate_key_segment("valid-mid_123") == "valid-mid_123"
    with pytest.raises(ValueError):
        validate_key_segment("")
    with pytest.raises(ValueError):
        validate_key_segment("invalid/slash")
