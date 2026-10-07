"""Tests for MeetMind AI Gateway — Redis streams."""

import pytest
from meetmind_gateway.redis_streams import StreamProducer


class TestStreamProducer:
    @pytest.fixture
    def producer(self, fake_redis):
        return StreamProducer(fake_redis, maxlen=100)

    async def test_publish_audio(self, producer, fake_redis):
        entry_id = await producer.publish_audio(
            "mtg_001",
            seq=1,
            tenant_id="acme",
            user_id="user_1",
            adapter_type="chrome_extension",
            audio_data=b"\x00\x01\x02\x03",
            timestamp_ms=1696000000000,
        )
        assert entry_id is not None

        # Verify stream has the entry
        assert "audio:mtg_001" in fake_redis._streams
        assert len(fake_redis._streams["audio:mtg_001"]) == 1

        _, fields = fake_redis._streams["audio:mtg_001"][0]
        assert fields["seq"] == "1"
        assert fields["tenant_id"] == "acme"
        assert fields["audio_data"] == b"\x00\x01\x02\x03"

    async def test_publish_transcript(self, producer, fake_redis):
        entry_id = await producer.publish_transcript(
            "mtg_001",
            seq=1,
            tenant_id="acme",
            user_id="user_1",
            adapter_type="desktop_app",
            text="Hello, world",
            timestamp_ms=1696000000000,
        )
        assert entry_id is not None
        assert "transcript:mtg_001" in fake_redis._streams

        _, fields = fake_redis._streams["transcript:mtg_001"][0]
        assert fields["text"] == "Hello, world"

    async def test_multiple_entries_ordered(self, producer, fake_redis):
        for i in range(5):
            await producer.publish_audio(
                "mtg_001",
                seq=i + 1,
                tenant_id="acme",
                user_id="user_1",
                adapter_type="chrome",
                audio_data=bytes([i]),
                timestamp_ms=1696000000000 + i,
            )

        entries = fake_redis._streams["audio:mtg_001"]
        assert len(entries) == 5
        for idx, (_, fields) in enumerate(entries):
            assert fields["seq"] == str(idx + 1)

    async def test_maxlen_enforced(self, fake_redis):
        producer = StreamProducer(fake_redis, maxlen=3)

        for i in range(5):
            await producer.publish_audio(
                "mtg_001",
                seq=i + 1,
                tenant_id="acme",
                user_id="user_1",
                adapter_type="chrome",
                audio_data=b"x",
                timestamp_ms=1696000000000,
            )

        # Only last 3 entries should remain
        assert len(fake_redis._streams["audio:mtg_001"]) == 3

    async def test_ensure_consumer_group(self, producer, fake_redis):
        await producer.ensure_consumer_group("audio:mtg_001", "asr_workers")
        assert "audio:mtg_001" in fake_redis._streams
