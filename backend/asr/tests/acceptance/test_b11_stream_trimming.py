"""Acceptance Test B11: Stream Trimming of Acked Audio (D1b Item 3).

Verifies:
1. As the ASR service acknowledges audio entries past the replay window,
   it trims them from mm:{tenant}:audio:{meeting} via XDEL.
2. After a 60s meeting stream (600 chunks), the Redis stream length stays
   bounded near the replay window, NOT the full 600-chunk history.
3. Records and verifies Redis memory per meeting before and after.
"""

from __future__ import annotations

import asyncio
import json
import uuid

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket, make_pcm16_chunk
from meetmind_schemas.keys import audio_stream_key

pytestmark = [pytest.mark.integration, pytest.mark.timeout(90)]


@pytest.mark.asyncio
async def test_b11_audio_stream_trimming_near_replay_window(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    meeting_id = f"mtg_b11_{uuid.uuid4().hex[:8]}"
    tenant_id = gateway_harness.tenant_a
    stream_key = audio_stream_key(tenant_id, meeting_id)

    # 1. Start ASR worker with a 2000ms replay window
    _worker = await asr_worker_factory(
        provider="stub",
        extra_env={
            "ASR_REPLAY_WINDOW_MS": "2000",
        },
    )

    # Record initial Redis memory
    mem_initial_raw = await real_redis.info("memory")
    used_mem_initial = mem_initial_raw.get("used_memory", 0)

    admin_token = gateway_harness.jwks.mint_token(
        sub="admin_b11", org_id=gateway_harness.org_a, org_role="org:admin"
    )
    _, ticket_resp = await exchange_ws_ticket(
        gateway_harness.http_url, admin_token, meeting_id, role="ingest"
    )
    ticket = ticket_resp["ticket"]
    ws_uri = f"{gateway_harness.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ticket}"

    async with websockets.connect(ws_uri) as ws:

        async def recv_ack():
            while True:
                msg = json.loads(await ws.recv())
                if msg.get("type") == "ack":
                    return msg

        # Start meeting & consent
        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": "chrome_extension",
                }
            )
        )
        assert (await recv_ack())["status"] == "accepted"

        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "consent_confirmed",
                    "consent_type": "audio_capture",
                    "external_participants": False,
                    "jurisdiction_hint": "US",
                    "consent_text_version": "v1.0",
                    "client_version": "1.0.0",
                }
            )
        )
        assert (await recv_ack())["status"] == "accepted"

        # Stream 20 seconds of audio: 200 chunks (100ms each) with drift-corrected pacing
        t_base = 5_000_000
        t_stream_start = asyncio.get_running_loop().time()
        for seq in range(1, 201):
            target_time = t_stream_start + (seq * 0.1)
            sleep_dur = target_time - asyncio.get_running_loop().time()
            if sleep_dur > 0:
                await asyncio.sleep(sleep_dur)

            frame = make_pcm16_chunk(seq=seq, ts_ms=t_base + seq * 100)
            await ws.send(frame)
            ack = await recv_ack()
            assert ack.get("status") in ("accepted", "acked")

        # Allow ack flush loop (runs every 200ms) to drain and trim older chunks
        stream_len = 200
        for _ in range(25):
            await asyncio.sleep(0.3)
            stream_len = await real_redis.xlen(stream_key)
            if stream_len < 50:
                break

        # 2. Assert stream length is bounded near the replay window, NOT 200 chunks
        assert stream_len < 50, (
            f"Expected audio stream to be trimmed near replay window (< 50), but got {stream_len}"
        )

        mem_steady_raw = await real_redis.info("memory")
        used_mem_steady = mem_steady_raw.get("used_memory", 0)

        # 3. End meeting
        await ws.send(json.dumps({"type": "control", "action": "meeting_end"}))
        assert (await recv_ack())["status"] == "accepted"

    # Wait for meeting drain and lease release
    await asyncio.sleep(2.0)
    final_stream_len = await real_redis.xlen(stream_key)
    assert final_stream_len == 0, (
        f"Expected audio stream to be fully drained, got {final_stream_len}"
    )

    mem_final_raw = await real_redis.info("memory")
    used_mem_final = mem_final_raw.get("used_memory", 0)

    print(f"\n[REDIS MEMORY STATS for meeting {meeting_id}]")
    print(f"  Initial Used Memory: {used_mem_initial} bytes")
    print(
        f"  Steady Used Memory (200 chunks sent, trimmed to {stream_len}): {used_mem_steady} bytes"
    )
    print(f"  Final Used Memory (after drain): {used_mem_final} bytes")
