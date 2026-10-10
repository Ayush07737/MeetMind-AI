"""Tests for MeetMind AI Gateway — Redis streams (WP4)."""

import pytest
from meetmind_gateway.keys import audio_stream_key, out_stream_key, transcript_stream_key
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

        stream_key = audio_stream_key("acme", "mtg_001")
        assert stream_key in fake_redis._streams
        assert len(fake_redis._streams[stream_key]) == 1

        _, fields = fake_redis._streams[stream_key][0]
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
        stream_key = transcript_stream_key("acme", "mtg_001")
        assert stream_key in fake_redis._streams

        _, fields = fake_redis._streams[stream_key][0]
        assert fields["text"] == "Hello, world"

    async def test_multiple_entries_ordered(self, producer, fake_redis):
        stream_key = audio_stream_key("acme", "mtg_001")
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

        entries = fake_redis._streams[stream_key]
        assert len(entries) == 5
        for idx, (_, fields) in enumerate(entries):
            assert fields["seq"] == str(idx + 1)

    async def test_maxlen_enforced(self, fake_redis):
        producer = StreamProducer(fake_redis, maxlen=3)
        stream_key = audio_stream_key("acme", "mtg_001")

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

        assert len(fake_redis._streams[stream_key]) == 3

    async def test_ensure_consumer_group(self, producer, fake_redis):
        stream_key = audio_stream_key("acme", "mtg_001")
        await producer.ensure_consumer_group(stream_key, "asr_workers")
        assert stream_key in fake_redis._streams

    async def test_publish_outbound_event_allowlisted(self, producer, fake_redis):
        entry_id = await producer.publish_outbound_event(
            "mtg_001",
            "acme",
            {"event_type": "transcript_chunk", "text": "test", "seq": 1},
        )
        assert entry_id is not None
        skey = out_stream_key("acme", "mtg_001")
        assert skey in fake_redis._streams
        assert len(fake_redis._streams[skey]) == 1

    async def test_publish_outbound_event_rejected_if_not_allowlisted(self, producer):
        with pytest.raises(ValueError, match="not allow-listed"):
            await producer.publish_outbound_event(
                "mtg_001",
                "acme",
                {"event_type": "internal_secret_worker_event", "data": 123},
            )
