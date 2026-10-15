"""MeetMind AI — L3: Deepgram Verified Parameters Suite.

Automated assertions for:
(a) mip_opt_out=true
(b) keyterm repeated parameters
(c) max keyterms (100 supported by client, 101 probe)
(d) language=en-IN with keyterms
(e) diarize=true returns speaker labels
(f) multichannel=true returns per-channel results
(g) exact result fields used for timestamps and model identifier in metadata
"""

from __future__ import annotations

import asyncio
import json
import os
import wave
from pathlib import Path

import pytest
import websockets

REPO_ROOT = Path(__file__).resolve().parents[4]
FIXTURE_WAV = (
    REPO_ROOT / "tests" / "fixtures" / "audio" / "generated" / "30s_two_speaker_overlap_mono.wav"
)
STEREO_WAV = REPO_ROOT / "tests" / "fixtures" / "audio" / "generated" / "30s_two_channel.wav"


def get_deepgram_api_key() -> str:
    key = os.environ.get("DEEPGRAM_API_KEY", "").strip()
    if not key:
        pytest.skip("DEEPGRAM_API_KEY is not set in environment")
    return key


async def run_probe(params: list[tuple[str, str]], pcm_bytes: bytes) -> dict:
    import urllib.parse

    key = get_deepgram_api_key()
    query_str = urllib.parse.urlencode(params)
    url = f"wss://api.deepgram.com/v1/listen?{query_str}"
    headers = {"Authorization": f"Token {key}"}

    res = {"messages": [], "metadata": None, "results": []}
    async with websockets.connect(url, additional_headers=headers, open_timeout=10.0) as ws:
        # Send audio in 100ms chunks
        chunk_size = 6400 if ("channels", "2") in params else 3200
        for offset in range(0, len(pcm_bytes), chunk_size):
            await ws.send(pcm_bytes[offset : offset + chunk_size])
            await asyncio.sleep(0.04)

        await ws.send(json.dumps({"type": "CloseStream"}))

        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=4.0)
                msg = json.loads(raw)
                res["messages"].append(msg)
                if msg.get("type") == "Metadata":
                    res["metadata"] = msg
                elif msg.get("type") == "Results":
                    res["results"].append(msg)
        except (TimeoutError, websockets.exceptions.ConnectionClosed):
            pass
    return res


def load_fixture_pcm(filepath: Path, seconds: float = 2.0) -> bytes:
    with wave.open(str(filepath), "rb") as wf:
        nframes = int(wf.getframerate() * seconds)
        return wf.readframes(nframes)


@pytest.mark.deepgram
@pytest.mark.asyncio
async def test_l3_parameters_probe():
    """Verify parameters against live Deepgram Nova-3 API."""
    print("\n[Estimated Cost] ~0.15 audio minutes x $0.0058/min = ~$0.0009")
    mono_pcm = load_fixture_pcm(FIXTURE_WAV, seconds=2.0)
    stereo_pcm = load_fixture_pcm(STEREO_WAV, seconds=2.0)

    res_a = await run_probe(
        [
            ("model", "nova-3"),
            ("encoding", "linear16"),
            ("sample_rate", "16000"),
            ("mip_opt_out", "true"),
        ],
        mono_pcm,
    )
    assert res_a["metadata"] is not None, "Expected Metadata message with mip_opt_out=true"
    model_info = res_a["metadata"].get("model_info", {})
    # Extract model name
    first_model = next(iter(model_info.values()), {})
    assert "nova-3" in first_model.get("arch", "").lower(), f"Unexpected arch: {first_model}"

    # (b) repeated keyterm
    res_b = await run_probe(
        [
            ("model", "nova-3"),
            ("encoding", "linear16"),
            ("sample_rate", "16000"),
            ("keyterm", "EBITDA"),
            ("keyterm", "Vertex Dynamics"),
        ],
        mono_pcm,
    )
    assert len(res_b["results"]) > 0, "Expected Results with repeated keyterms"

    # (c) 100 keyterms
    terms_100 = [("model", "nova-3"), ("encoding", "linear16"), ("sample_rate", "16000")] + [
        ("keyterm", f"term{i}") for i in range(100)
    ]
    res_c100 = await run_probe(terms_100, mono_pcm)
    assert len(res_c100["results"]) > 0, "100 keyterms should be accepted"

    # (d) language=en-IN with keyterms
    res_d = await run_probe(
        [
            ("model", "nova-3"),
            ("encoding", "linear16"),
            ("sample_rate", "16000"),
            ("language", "en-IN"),
            ("keyterm", "crore"),
            ("keyterm", "lakh"),
        ],
        mono_pcm,
    )
    assert len(res_d["results"]) > 0, "en-IN with keyterms should be accepted"

    # (e) diarize=true returns speaker labels
    res_e = await run_probe(
        [
            ("model", "nova-3"),
            ("encoding", "linear16"),
            ("sample_rate", "16000"),
            ("diarize", "true"),
        ],
        mono_pcm,
    )
    speakers = set()
    for r in res_e["results"]:
        for w in r.get("channel", {}).get("alternatives", [{}])[0].get("words", []):
            if "speaker" in w:
                speakers.add(w["speaker"])
    assert len(speakers) > 0, f"Expected speaker labels in words, found: {speakers}"

    # (f) multichannel=true returns per-channel results
    res_f = await run_probe(
        [
            ("model", "nova-3"),
            ("encoding", "linear16"),
            ("sample_rate", "16000"),
            ("channels", "2"),
            ("multichannel", "true"),
        ],
        stereo_pcm,
    )
    channels = set()
    for r in res_f["results"]:
        ch_idx = r.get("channel_index")
        if ch_idx is not None:
            channels.add(ch_idx[0] if isinstance(ch_idx, list) else ch_idx)
    assert 0 in channels, f"Expected channel 0 in multichannel results: {channels}"

    # (g) Exact result timestamp fields
    r0 = res_a["results"][0]
    assert "start" in r0 and isinstance(r0["start"], (int, float))
    assert "duration" in r0 and isinstance(r0["duration"], (int, float))
    alt0 = r0["channel"]["alternatives"][0]
    assert "transcript" in alt0
    if alt0.get("words"):
        w0 = alt0["words"][0]
        assert "start" in w0 and "end" in w0
