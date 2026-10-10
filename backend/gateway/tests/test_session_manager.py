"""Tests for MeetMind AI Gateway — Session manager."""

import pytest
from meetmind_gateway.frames import MeetingStatus
from meetmind_gateway.session_manager import SessionManager


class TestSessionManager:
    @pytest.fixture
    def manager(self, fake_redis):
        return SessionManager(fake_redis)

    async def test_create_session(self, manager):
        session = await manager.create_session(
            meeting_id="mtg_001",
            tenant_id="acme",
            user_id="user_1",
            adapter_type="chrome_extension",
        )
        assert session.meeting_id == "mtg_001"
        assert session.tenant_id == "acme"
        assert session.status == MeetingStatus.PENDING_CONSENT

    async def test_update_and_get_status(self, manager):
        await manager.create_session("mtg_001", "acme", "user_1", "chrome_extension")

        await manager.update_status("mtg_001", MeetingStatus.ACTIVE)
        status = await manager.get_status("mtg_001")
        assert status == MeetingStatus.ACTIVE

    async def test_get_status_nonexistent(self, manager):
        status = await manager.get_status("nonexistent")
        assert status is None

    async def test_multiple_listeners(self, manager):
        await manager.create_session("mtg_001", "acme", "user_1", "chrome")
        await manager.create_session("mtg_001", "acme", "user_2", "desktop")

        count = await manager.get_listener_count("mtg_001")
        assert count == 2

    async def test_remove_listener(self, manager):
        s1 = await manager.create_session("mtg_001", "acme", "user_1", "chrome")
        s2 = await manager.create_session("mtg_001", "acme", "user_2", "desktop")

        remaining = await manager.remove_listener("mtg_001", s1.connection_id)
        assert remaining == 1

        remaining = await manager.remove_listener("mtg_001", s2.connection_id)
        assert remaining == 0

    async def test_cleanup_session(self, manager, fake_redis):
        await manager.create_session("mtg_001", "acme", "user_1", "chrome")
        await manager.cleanup_session("mtg_001")

        assert await manager.get_status("mtg_001") is None
        assert await manager.get_listener_count("mtg_001") == 0

    async def test_session_data_retrieval(self, manager):
        await manager.create_session("mtg_001", "acme", "user_1", "chrome_extension")
        data = await manager.get_session_data("mtg_001")
        assert data is not None
        assert data["tenant_id"] == "acme"
        assert data["meeting_id"] == "mtg_001"

    async def test_joining_existing_session_preserves_state(self, manager):
        """A second client joining an active meeting inherits its state."""
        await manager.create_session("mtg_001", "acme", "user_1", "chrome")
        await manager.update_status("mtg_001", MeetingStatus.ACTIVE)

        s2 = await manager.create_session("mtg_001", "acme", "user_2", "desktop")
        assert s2.status == MeetingStatus.ACTIVE

    async def test_refresh_activity_updates_ttls(self, manager, fake_redis):
        """Verify that refresh_activity refreshes TTLs on session and listener keys."""
        await manager.create_session("mtg_001", "acme", "user_1", "chrome")
        # Call refresh_activity
        await manager.refresh_activity("mtg_001", "acme", ttl=3600)

        # Keys exist and are tracked
        from meetmind_gateway.keys import (
            listeners_key,
            meeting_session_key,
            meeting_tenant_binding_key,
        )

        assert await fake_redis.exists(meeting_session_key("acme", "mtg_001")) == 1
        assert await fake_redis.exists(listeners_key("acme", "mtg_001")) == 1
        assert await fake_redis.exists(meeting_tenant_binding_key("mtg_001")) == 1
