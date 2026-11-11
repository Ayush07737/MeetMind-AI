"""MeetMind AI — ASR Provider Bake-off Evaluation Runner (A1 / B4 / B5).

Evaluates candidate ASR providers against Engineering Corpus v2
(`ml/eval/audio/synthetic_eng/manifest.jsonl`) with 3 interleaved repetitions,
unified harness-clock definitions, shared normalizer metrics (WER, Hinglish WER,
NTER, IEMR, EER), word-level interim stability/retraction metrics, DNS/TCP/TLS/
WS-upgrade/ping network timing from the host, 2-speaker mono diarization & overlap
checks, owner-default weighted scorecard, and hard gates.

OUTPUT RULE: Prints DATA ONLY. No explanatory prose or recommendations.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import json
import os
import random
import re
import socket
import ssl
import sys
import time
import urllib.parse
import wave
from pathlib import Path
from typing import Any

import httpx
import websockets
from dotenv import load_dotenv

# Ensure local modules resolve cleanly regardless of cwd
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[2]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(REPO_ROOT / "packages" / "quality") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "packages" / "quality"))

from meetmind_quality.keyterms import ENGINEERING_KEYTERMS  # noqa: E402
from normalize import (  # noqa: E402
    compute_entity_error_rate,
    compute_identifier_exact_match,
    compute_numeric_token_error_rate,
    extract_canonical_numbers,
    extract_identifiers,
)
from normalize import (  # noqa: E402
    compute_normalized_wer as compute_wer,
)

MANIFEST_PATH = REPO_ROOT / "ml/eval/audio/synthetic_eng/manifest.jsonl"

DEFAULT_SCORECARD_WEIGHTS: dict[str, float] = {
    "accuracy": 0.35,
    "diarization": 0.15,
    "latency_stability": 0.20,
    "vocabulary": 0.10,
    "cost": 0.10,
    "residency_compliance": 0.05,
    "reliability_sdk": 0.05,
}

# Verified official docs & pricing (access date 2026-11-10 unless noted)
VENDOR_METADATA: dict[str, dict[str, Any]] = {
    "deepgram_nova3": {
        "name": "Deepgram Nova-3 (en-IN)",
        "region": "api.in.deepgram.com (India)",
        "india_residency": True,
        "live_diarization": True,
        "custom_vocab": True,
        "zero_retention": True,
        "price_mono_usd_hr": 0.462,  # $0.0077/min PAYG (+$0.0013/min keyterm option)
        "price_2ch_usd_hr": 0.924,  # 2 channels * $0.0077/min (multichannel per-channel billing)
        "pricing_note": "$0.0077/min mono PAYG (+$0.0013/min keyterm); deepgram.com/pricing",
    },
    "deepgram_nova3_multi": {
        "name": "Deepgram Nova-3 (multi)",
        "region": "api.in.deepgram.com (India)",
        "india_residency": True,
        "live_diarization": True,
        "custom_vocab": True,
        "zero_retention": True,
        "price_mono_usd_hr": 0.552,  # $0.0092/min multilingual PAYG; deepgram.com/pricing
        "price_2ch_usd_hr": 1.104,  # 2 channels * $0.0092/min
        "pricing_note": "$0.0092/min multi PAYG (500-tok kt cap <=65); deepgram.com/pricing",
    },
    "deepgram_flux": {
        "name": "Deepgram Flux",
        "region": "api.in.deepgram.com (India)",
        "india_residency": True,
        "live_diarization": False,  # TurnInfo lacks speaker attribution
        "custom_vocab": True,
        "zero_retention": True,
        "price_mono_usd_hr": 0.462,  # $0.0077/min PAYG; deepgram.com/pricing
        "price_2ch_usd_hr": None,  # UNVERIFIED on /v2/listen multichannel
        "pricing_note": "$0.0077/min PAYG; 2-ch UNVERIFIED; deepgram.com/pricing",
    },
    "gladia_solaria1": {
        "name": "Gladia Solaria-1",
        "region": "eu-west / us-west (No India)",
        "india_residency": False,
        "live_diarization": False,  # Verified HTTP 400 on /v2/live when diarization passed
        "custom_vocab": True,
        "zero_retention": False,  # Self-serve has no documented ZDR opt-out flag on /v2/live
        "price_mono_usd_hr": 0.740,  # EUR 0.68/hr live (~$0.74/hr USD equiv) on gladia.io/pricing
        "price_2ch_usd_hr": None,  # 2-channel live multiplier UNVERIFIED on public self-serve page
        "pricing_note": "EUR 0.68/hr (~$0.74/hr) live Solaria-1 mono; 2-ch UNVERIFIED; gladia.io",
    },
    "assemblyai_universal": {
        "name": "AssemblyAI Universal-3.6",
        "region": "Global / US / EU (No India)",
        "india_residency": False,
        "live_diarization": True,
        "custom_vocab": True,
        "zero_retention": True,
        "price_mono_usd_hr": 0.450,  # $0.45/hr Universal-3.6 Pro Realtime (base streaming $0.15/hr)
        "price_2ch_usd_hr": None,  # UNVERIFIED on v3 streaming query schema
        "pricing_note": "$0.45/hr Universal-3.6 Pro ($0.15/hr base); assemblyai.com/pricing",
    },
    "sarvam": {
        "name": "Sarvam Saaras v3",
        "region": "ap-south-1 (India)",
        "india_residency": True,
        "live_diarization": False,
        "custom_vocab": True,
        "zero_retention": False,
        "price_mono_usd_hr": 0.350,
        "price_2ch_usd_hr": None,
        "pricing_note": "INR 0.49/min (~$0.35/hr); docs.sarvam.ai",
    },
    "openai": {
        "name": "OpenAI Realtime",
        "region": "US / EU (No India)",
        "india_residency": False,
        "live_diarization": False,
        "custom_vocab": True,
        "zero_retention": True,
        "price_mono_usd_hr": 3.600,
        "price_2ch_usd_hr": None,
        "pricing_note": "$0.06/min audio input; openai.com/api/pricing",
    },
}

BAKEOFF_KEYTERMS: list[str] = list(
    dict.fromkeys(
        ENGINEERING_KEYTERMS
        + [
            "Vikram",
            "Ananya",
            "Priya",
            "Rohan",
            "PLAT",
            "ENG",
            "FRONT",
            "INFRA",
            "DATA",
            "CORE",
            "TECH",
            "SEC",
            "INC",
            "ADR",
            "PR",
            "Pydantic",
            "Qdrant",
            "Neo4j",
            "FastAPI",
        ]
    )
)[:100]


def bootstrap_metric_ci(
    values: list[float], n_bootstrap: int = 1000, alpha: float = 0.05, seed: int = 42
) -> dict[str, float]:
    """Compute 95% bootstrap confidence interval resampled at segment level."""
    if not values:
        return {"mean": 0.0, "ci_lower": 0.0, "ci_upper": 0.0}
    rng = random.Random(seed)
    n = len(values)
    mean_val = sum(values) / float(n)

    boot_means: list[float] = []
    for _ in range(n_bootstrap):
        sample = [values[rng.randrange(n)] for _ in range(n)]
        boot_means.append(sum(sample) / float(n))

    boot_means.sort()
    lower_idx = int(n_bootstrap * (alpha / 2.0))
    upper_idx = min(n_bootstrap - 1, int(n_bootstrap * (1.0 - alpha / 2.0)))

    return {
        "mean": round(mean_val, 4),
        "ci_lower": round(boot_means[lower_idx], 4),
        "ci_upper": round(boot_means[upper_idx], 4),
    }


def _pct(vals: list[float], q: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    idx = min(len(s) - 1, max(0, int(len(s) * q)))
    return round(float(s[idx]), 2)


def _tokenize_plain(text: str) -> list[str]:
    cleaned = re.sub(r"[^a-z0-9\s]", " ", (text or "").lower())
    return [tok for tok in cleaned.split() if tok]


def compute_interim_stability_metrics(
    partials: list[tuple[float, str]],
    final_text: str,
    first_chunk_perf: float | None,
    last_final_perf: float | None,
    audio_duration_s: float,
) -> dict[str, Any]:
    """Compute word-level interim lag, time-to-stable text, and retraction rate."""
    if not first_chunk_perf or not last_final_perf:
        return {
            "word_interim_lags_ms": [],
            "stable_times_ms": [],
            "retractions": 0,
            "total_interim_words": 0,
        }

    tok_partials: list[tuple[float, list[str]]] = [
        (ts, _tokenize_plain(txt)) for ts, txt in partials if _tokenize_plain(txt)
    ]
    final_toks = _tokenize_plain(final_text)
    if not final_toks:
        return {
            "word_interim_lags_ms": [],
            "stable_times_ms": [],
            "retractions": 0,
            "total_interim_words": 0,
        }

    n_final = len(final_toks)
    word_interim_lags: list[float] = []
    stable_times: list[float] = []

    for k, target_tok in enumerate(final_toks):
        est_spoken_perf = first_chunk_perf + (k / max(1, n_final)) * max(0.1, audio_duration_s)

        # First partial timestamp exposing >= k+1 words
        first_seen_ts: float | None = None
        for ts, p_toks in tok_partials:
            if len(p_toks) > k:
                first_seen_ts = ts
                break
        if first_seen_ts is not None:
            word_interim_lags.append(max(0.0, (first_seen_ts - est_spoken_perf) * 1000.0))

        # Earliest partial index after which position k stays == target_tok through final
        stable_ts: float = last_final_perf
        for p_idx in range(len(tok_partials)):
            ts_p, p_toks = tok_partials[p_idx]
            if k < len(p_toks) and p_toks[k] == target_tok:
                stayed = True
                for j in range(p_idx + 1, len(tok_partials)):
                    _, later_toks = tok_partials[j]
                    if k >= len(later_toks) or later_toks[k] != target_tok:
                        stayed = False
                        break
                if stayed:
                    stable_ts = ts_p
                    break
        stable_times.append(max(0.0, (stable_ts - est_spoken_perf) * 1000.0))

    # Retraction rate across consecutive partial snapshots + final snapshot
    snapshots: list[list[str]] = [p_toks for _, p_toks in tok_partials]
    snapshots.append(final_toks)
    retractions = 0
    total_interim_words = sum(len(p_toks) for _, p_toks in tok_partials)
    for idx in range(len(snapshots) - 1):
        prev_t = snapshots[idx]
        next_t = snapshots[idx + 1]
        for w_idx in range(min(len(prev_t), len(next_t))):
            if prev_t[w_idx] != next_t[w_idx]:
                retractions += 1

    return {
        "word_interim_lags_ms": word_interim_lags,
        "stable_times_ms": stable_times,
        "retractions": retractions,
        "total_interim_words": total_interim_words,
    }


def load_pcm16_mono(wav_path: Path) -> bytes:
    """Read raw 16kHz PCM16 bytes from WAV file."""
    with wave.open(str(wav_path), "rb") as wf:
        return wf.readframes(wf.getnframes())


def select_stratified_corpus(
    all_records: list[dict[str, Any]], sample_size: int = 15, seed: int = 42
) -> list[dict[str, Any]]:
    """Guarantee all 10 scenarios first, then fill Hinglish/IDs across mono variants."""
    mono_variants = ["clean", "noise_15db", "telephony_8khz", "speed_1_15x"]
    mono_records = [r for r in all_records if r.get("variant") in mono_variants]

    scenarios = sorted({r["scenario"] for r in mono_records})
    rng = random.Random(seed)
    selected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    # Pass 1: Exactly 1 stratified turn from each of all 10 scenarios
    for idx, sc in enumerate(scenarios):
        sc_recs = [r for r in mono_records if r["scenario"] == sc]
        variant_choice = mono_variants[idx % len(mono_variants)]
        hinglish_recs = [r for r in sc_recs if r.get("language_tag") == "hinglish"]
        id_recs = [r for r in sc_recs if extract_identifiers(r["spoken_text"])]
        num_recs = [
            r
            for r in sc_recs
            if extract_identifiers(r["spoken_text"]) or extract_canonical_numbers(r["spoken_text"])
        ]

        if idx % 3 == 0 and hinglish_recs:
            var_h = [r for r in hinglish_recs if r.get("variant") == variant_choice]
            cand = rng.choice(var_h or hinglish_recs)
        else:
            var_id = [r for r in id_recs if r.get("variant") == variant_choice]
            var_num = [r for r in num_recs if r.get("variant") == variant_choice]
            cand = rng.choice(var_id or id_recs or var_num or num_recs or sc_recs)

        if cand["segment_id"] not in seen_ids:
            selected.append(cand)
            seen_ids.add(cand["segment_id"])

    # Pass 2: Add extra Hinglish & identifier turns across remaining capacity
    extra_hinglish = [
        r
        for r in mono_records
        if r.get("language_tag") == "hinglish" and r["segment_id"] not in seen_ids
    ]
    rng.shuffle(extra_hinglish)
    for r in extra_hinglish:
        if len(selected) >= min(sample_size, len(scenarios) + max(2, sample_size // 4)):
            break
        selected.append(r)
        seen_ids.add(r["segment_id"])

    if len(selected) < sample_size:
        extra_ids = [
            r
            for r in mono_records
            if r["segment_id"] not in seen_ids and extract_identifiers(r["spoken_text"])
        ]
        rng.shuffle(extra_ids)
        for r in extra_ids:
            if len(selected) >= sample_size:
                break
            selected.append(r)
            seen_ids.add(r["segment_id"])

    if len(selected) < sample_size:
        extra_nums = [
            r
            for r in mono_records
            if r["segment_id"] not in seen_ids and extract_canonical_numbers(r["spoken_text"])
        ]
        rng.shuffle(extra_nums)
        for r in extra_nums:
            if len(selected) >= sample_size:
                break
            selected.append(r)
            seen_ids.add(r["segment_id"])

    if len(selected) < sample_size:
        remaining = [r for r in mono_records if r["segment_id"] not in seen_ids]
        rng.shuffle(remaining)
        for r in remaining:
            if len(selected) >= sample_size:
                break
            selected.append(r)
            seen_ids.add(r["segment_id"])

    return selected[:sample_size]


async def measure_endpoint_network(
    name: str,
    host: str,
    ws_url: str,
    headers: dict[str, str] | None = None,
    post_init_url: str | None = None,
    post_init_headers: dict[str, str] | None = None,
    post_init_body: dict[str, Any] | None = None,
    samples: int = 5,
) -> dict[str, Any]:
    """Measure DNS, TCP, TLS, HTTP/WS-upgrade, and WS Ping RTT (ms) over N samples."""
    dns_ms: list[float] = []
    tcp_ms: list[float] = []
    tls_ms: list[float] = []
    ws_upg_ms: list[float] = []
    ping_rtt_ms: list[float] = []
    err_msg: str | None = None

    ctx = ssl.create_default_context()
    for _ in range(samples):
        try:
            t0 = time.perf_counter()
            addr = socket.getaddrinfo(host, 443, socket.AF_INET, socket.SOCK_STREAM)
            ip = addr[0][4][0]
            dns_ms.append((time.perf_counter() - t0) * 1000.0)

            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5.0)
            t_tcp = time.perf_counter()
            s.connect((ip, 443))
            tcp_ms.append((time.perf_counter() - t_tcp) * 1000.0)

            t_tls = time.perf_counter()
            ssock = ctx.wrap_socket(s, server_hostname=host)
            tls_ms.append((time.perf_counter() - t_tls) * 1000.0)
            ssock.close()
        except Exception as exc:
            err_msg = str(exc)[:100]

    # WS Upgrade + Ping RTT
    try:
        target_ws = ws_url
        if post_init_url:
            async with httpx.AsyncClient(timeout=15.0) as client:
                r = await client.post(
                    post_init_url,
                    json=post_init_body or {},
                    headers=post_init_headers or {},
                )
                r.raise_for_status()
                target_ws = r.json()["url"]

        t_u0 = time.perf_counter()
        async with websockets.connect(
            target_ws, additional_headers=headers, open_timeout=15.0
        ) as ws:
            ws_upg_ms.append((time.perf_counter() - t_u0) * 1000.0)
            for _ in range(samples):
                t_p0 = time.perf_counter()
                pong = await ws.ping()
                await asyncio.wait_for(pong, timeout=5.0)
                ping_rtt_ms.append((time.perf_counter() - t_p0) * 1000.0)
    except Exception as exc:
        err_msg = str(exc)[:100]

    return {
        "name": name,
        "host": host,
        "dns_p50_ms": _pct(dns_ms, 0.5),
        "tcp_p50_ms": _pct(tcp_ms, 0.5),
        "tls_p50_ms": _pct(tls_ms, 0.5),
        "ws_upgrade_ms": _pct(ws_upg_ms, 0.5),
        "ping_rtt_p50_ms": _pct(ping_rtt_ms, 0.5),
        "ping_rtt_p95_ms": _pct(ping_rtt_ms, 0.95),
        "error": err_msg,
    }


async def eval_deepgram_nova3(
    pcm: bytes,
    keyterms: list[str],
    language: str = "en-IN",
    max_keyterms: int = 100,
) -> dict[str, Any]:
    api_key = os.environ.get("DEEPGRAM_API_KEY", "").strip()
    if not api_key:
        return {"status": "NOT_RUN", "error": "missing DEEPGRAM_API_KEY"}

    params: list[tuple[str, str]] = [
        ("model", "nova-3"),
        ("encoding", "linear16"),
        ("sample_rate", "16000"),
        ("channels", "1"),
        ("diarize", "true"),
        ("interim_results", "true"),
        ("language", language),
        ("mip_opt_out", "true"),
    ]
    for kt in keyterms[:max_keyterms]:
        params.append(("keyterm", kt))
    url = f"wss://api.in.deepgram.com/v1/listen?{urllib.parse.urlencode(params)}"

    chunk_bytes = 3200
    finals: list[str] = []
    partials: list[tuple[float, str]] = []
    speakers_seen: set[int] = set()
    first_chunk_perf: float | None = None
    first_interim_perf: float | None = None
    last_speech_perf: float | None = None
    last_final_perf: float | None = None
    audio_dur_s = len(pcm) / 32000.0

    t_conn0 = time.perf_counter()
    try:
        async with websockets.connect(
            url, additional_headers={"Authorization": f"Token {api_key}"}, open_timeout=15.0
        ) as ws:
            connect_ms = (time.perf_counter() - t_conn0) * 1000.0
            done_evt = asyncio.Event()

            async def _recv() -> None:
                nonlocal first_interim_perf, last_final_perf
                try:
                    async for msg in ws:
                        d = json.loads(msg)
                        if d.get("type") == "Results":
                            alts = d.get("channel", {}).get("alternatives", [])
                            txt = (alts[0].get("transcript") or "").strip() if alts else ""
                            is_fin = bool(d.get("is_final", False))
                            now_p = time.perf_counter()
                            if txt and not is_fin:
                                partials.append((now_p, " ".join([*finals, txt]).strip()))
                                if first_interim_perf is None:
                                    first_interim_perf = now_p
                            if txt and is_fin:
                                finals.append(txt)
                                partials.append((now_p, " ".join(finals)))
                                last_final_perf = now_p
                                for w in alts[0].get("words", []):
                                    if "speaker" in w:
                                        speakers_seen.add(int(w["speaker"]))
                        elif d.get("type") == "Metadata":
                            done_evt.set()
                except Exception:
                    pass
                finally:
                    done_evt.set()

            rtask = asyncio.create_task(_recv())
            for i in range(0, len(pcm), chunk_bytes):
                if first_chunk_perf is None:
                    first_chunk_perf = time.perf_counter()
                await ws.send(pcm[i : i + chunk_bytes])
                await asyncio.sleep(0.1)
            last_speech_perf = time.perf_counter()

            for _ in range(12):
                await ws.send(b"\x00" * chunk_bytes)
                await asyncio.sleep(0.1)
            await ws.send(json.dumps({"type": "CloseStream"}))
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(done_evt.wait(), timeout=3.0)
            rtask.cancel()
    except Exception as exc:
        return {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}", "reconnects": 1}

    transcript = " ".join(finals).strip()
    stab = compute_interim_stability_metrics(
        partials, transcript, first_chunk_perf, last_final_perf, audio_dur_s
    )
    ttfb_ms = (
        max(0.0, (first_interim_perf - first_chunk_perf) * 1000.0)
        if first_interim_perf and first_chunk_perf
        else None
    )
    tail_ms = (
        max(0.0, (last_final_perf - last_speech_perf) * 1000.0)
        if last_final_perf and last_speech_perf
        else 0.0
    )
    return {
        "status": "OK",
        "transcript": transcript,
        "connect_ms": round(connect_ms, 2),
        "ttfb_interim_ms": round(ttfb_ms, 2) if ttfb_ms is not None else None,
        "final_flush_ms": round(tail_ms, 2),
        "speakers_detected": len(speakers_seen),
        "speaker_labels": sorted(str(x) for x in speakers_seen),
        "reconnects": 0,
        "stability": stab,
    }


async def eval_deepgram_flux(pcm: bytes, keyterms: list[str]) -> dict[str, Any]:
    api_key = os.environ.get("DEEPGRAM_API_KEY", "").strip()
    if not api_key:
        return {"status": "NOT_RUN", "error": "missing DEEPGRAM_API_KEY"}

    params: list[tuple[str, str]] = [
        ("model", "flux-general-en"),
        ("encoding", "linear16"),
        ("sample_rate", "16000"),
        ("mip_opt_out", "true"),
        ("eot_timeout_ms", "1000"),
    ]
    for kt in keyterms[:100]:
        params.append(("keyterm", kt))
    url = f"wss://api.in.deepgram.com/v2/listen?{urllib.parse.urlencode(params)}"

    chunk_bytes = 3200
    finals: list[str] = []
    partials: list[tuple[float, str]] = []
    latest_update: str = ""
    first_chunk_perf: float | None = None
    first_interim_perf: float | None = None
    last_speech_perf: float | None = None
    last_final_perf: float | None = None
    audio_dur_s = len(pcm) / 32000.0

    t_conn0 = time.perf_counter()
    try:
        async with websockets.connect(
            url, additional_headers={"Authorization": f"Token {api_key}"}, open_timeout=15.0
        ) as ws:
            connect_ms = (time.perf_counter() - t_conn0) * 1000.0
            done_evt = asyncio.Event()

            async def _recv() -> None:
                nonlocal first_interim_perf, last_final_perf, latest_update
                try:
                    async for msg in ws:
                        d = json.loads(msg)
                        if d.get("type") == "TurnInfo":
                            ev = d.get("event")
                            txt = (d.get("transcript") or "").strip()
                            now_p = time.perf_counter()
                            if txt and ev in ("StartOfTurn", "Update", "EagerEndOfTurn"):
                                latest_update = txt
                                partials.append((now_p, " ".join([*finals, txt]).strip()))
                                if first_interim_perf is None:
                                    first_interim_perf = now_p
                            if txt and ev == "EndOfTurn":
                                finals.append(txt)
                                partials.append((now_p, " ".join(finals)))
                                latest_update = ""
                                last_final_perf = now_p
                except Exception:
                    pass
                finally:
                    done_evt.set()

            rtask = asyncio.create_task(_recv())
            for i in range(0, len(pcm), chunk_bytes):
                if first_chunk_perf is None:
                    first_chunk_perf = time.perf_counter()
                await ws.send(pcm[i : i + chunk_bytes])
                await asyncio.sleep(0.1)
            last_speech_perf = time.perf_counter()

            for _ in range(15):
                await ws.send(b"\x00" * chunk_bytes)
                await asyncio.sleep(0.1)
                if last_final_perf and last_final_perf > last_speech_perf:
                    break
            await ws.send(json.dumps({"type": "CloseStream"}))
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(done_evt.wait(), timeout=2.0)
            rtask.cancel()
    except Exception as exc:
        return {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}", "reconnects": 1}

    if not finals and latest_update:
        finals.append(latest_update)
    if last_final_perf is None:
        last_final_perf = time.perf_counter()

    transcript = " ".join(finals).strip()
    stab = compute_interim_stability_metrics(
        partials, transcript, first_chunk_perf, last_final_perf, audio_dur_s
    )
    ttfb_ms = (
        max(0.0, (first_interim_perf - first_chunk_perf) * 1000.0)
        if first_interim_perf and first_chunk_perf
        else None
    )
    tail_ms = max(0.0, (last_final_perf - last_speech_perf) * 1000.0) if last_speech_perf else 0.0
    return {
        "status": "OK",
        "transcript": transcript,
        "connect_ms": round(connect_ms, 2),
        "ttfb_interim_ms": round(ttfb_ms, 2) if ttfb_ms is not None else None,
        "final_flush_ms": round(tail_ms, 2),
        "speakers_detected": 0,
        "speaker_labels": [],
        "reconnects": 0,
        "stability": stab,
    }


async def eval_gladia_solaria1(
    pcm: bytes,
    keyterms: list[str],
    custom_vocab: bool = True,
    code_switching: bool = True,
    region: str = "eu-west",
) -> dict[str, Any]:
    api_key = os.environ.get("GLADIA_API_KEY", "").strip()
    if not api_key:
        return {"status": "NOT_RUN", "error": "missing GLADIA_API_KEY"}

    langs = ["en", "hi"] if code_switching else ["en"]
    body: dict[str, Any] = {
        "model": "solaria-1",
        "encoding": "wav/pcm",
        "bit_depth": 16,
        "sample_rate": 16000,
        "channels": 1,
        "language_config": {"languages": langs, "code_switching": code_switching},
        "messages_config": {
            "receive_partial_transcripts": True,
            "receive_final_transcripts": True,
        },
    }
    if custom_vocab and keyterms:
        body["realtime_processing"] = {
            "custom_vocabulary": True,
            "custom_vocabulary_config": {"vocabulary": keyterms[:100]},
        }

    t_conn0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.post(
                f"https://api.gladia.io/v2/live?region={region}",
                json=body,
                headers={"x-gladia-key": api_key},
            )
            r.raise_for_status()
            ws_url = r.json()["url"]

        chunk_bytes = 3200
        finals: list[str] = []
        partials: list[tuple[float, str]] = []
        speakers_seen: set[str] = set()
        first_chunk_perf: float | None = None
        first_interim_perf: float | None = None
        last_speech_perf: float | None = None
        last_final_perf: float | None = None
        audio_dur_s = len(pcm) / 32000.0

        async with websockets.connect(ws_url, open_timeout=15.0) as ws:
            connect_ms = (time.perf_counter() - t_conn0) * 1000.0
            done_evt = asyncio.Event()

            async def _recv() -> None:
                nonlocal first_interim_perf, last_final_perf
                try:
                    async for msg in ws:
                        d = json.loads(msg)
                        if d.get("type") == "transcript":
                            data = d.get("data", {})
                            utt = data.get("utterance", {})
                            txt = (utt.get("text") or "").strip()
                            is_fin = bool(data.get("is_final", False))
                            now_p = time.perf_counter()
                            if utt.get("speaker") is not None:
                                speakers_seen.add(str(utt["speaker"]))
                            if txt and not is_fin:
                                partials.append((now_p, " ".join([*finals, txt]).strip()))
                                if first_interim_perf is None:
                                    first_interim_perf = now_p
                            if txt and is_fin:
                                finals.append(txt)
                                partials.append((now_p, " ".join(finals)))
                                last_final_perf = now_p
                        elif d.get("type") == "stop_recording_ack":
                            done_evt.set()
                except Exception:
                    pass
                finally:
                    done_evt.set()

            rtask = asyncio.create_task(_recv())
            for i in range(0, len(pcm), chunk_bytes):
                if first_chunk_perf is None:
                    first_chunk_perf = time.perf_counter()
                await ws.send(pcm[i : i + chunk_bytes])
                await asyncio.sleep(0.1)
            last_speech_perf = time.perf_counter()

            for _ in range(10):
                await ws.send(b"\x00" * chunk_bytes)
                await asyncio.sleep(0.1)
            await ws.send(json.dumps({"type": "stop_recording"}))
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(done_evt.wait(), timeout=3.5)
            rtask.cancel()
    except Exception as exc:
        return {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}", "reconnects": 1}

    transcript = " ".join(finals).strip()
    stab = compute_interim_stability_metrics(
        partials, transcript, first_chunk_perf, last_final_perf, audio_dur_s
    )
    ttfb_ms = (
        max(0.0, (first_interim_perf - first_chunk_perf) * 1000.0)
        if first_interim_perf and first_chunk_perf
        else None
    )
    tail_ms = (
        max(0.0, (last_final_perf - last_speech_perf) * 1000.0)
        if last_final_perf and last_speech_perf
        else 0.0
    )
    return {
        "status": "OK",
        "transcript": transcript,
        "connect_ms": round(connect_ms, 2),
        "ttfb_interim_ms": round(ttfb_ms, 2) if ttfb_ms is not None else None,
        "final_flush_ms": round(tail_ms, 2),
        "speakers_detected": len(speakers_seen),
        "speaker_labels": sorted(speakers_seen),
        "reconnects": 0,
        "stability": stab,
    }


async def eval_assemblyai(pcm: bytes, keyterms: list[str]) -> dict[str, Any]:
    api_key = os.environ.get("ASSEMBLYAI_API_KEY", "").strip()
    if not api_key:
        return {"status": "NOT_RUN", "error": "missing ASSEMBLYAI_API_KEY"}

    kt_json = json.dumps(keyterms[:100])
    params = urllib.parse.urlencode(
        [
            ("sample_rate", "16000"),
            ("speech_model", "universal-3-6-pro"),
            ("speaker_labels", "true"),
            ("keyterms_prompt", kt_json),
        ]
    )
    url = f"wss://streaming.assemblyai.com/v3/ws?{params}"

    chunk_bytes = 3200
    finals: list[str] = []
    partials: list[tuple[float, str]] = []
    speakers_seen: set[str] = set()
    first_chunk_perf: float | None = None
    first_interim_perf: float | None = None
    last_speech_perf: float | None = None
    last_final_perf: float | None = None
    audio_dur_s = len(pcm) / 32000.0

    t_conn0 = time.perf_counter()
    try:
        async with websockets.connect(
            url, additional_headers={"Authorization": api_key}, open_timeout=15.0
        ) as ws:
            connect_ms = (time.perf_counter() - t_conn0) * 1000.0
            done_evt = asyncio.Event()

            async def _recv() -> None:
                nonlocal first_interim_perf, last_final_perf
                try:
                    async for msg in ws:
                        d = json.loads(msg)
                        if d.get("type") == "Turn":
                            txt = (d.get("transcript") or "").strip()
                            is_fin = bool(d.get("end_of_turn", False))
                            now_p = time.perf_counter()
                            if txt and not is_fin:
                                partials.append((now_p, " ".join([*finals, txt]).strip()))
                                if first_interim_perf is None:
                                    first_interim_perf = now_p
                            if txt and is_fin:
                                finals.append(txt)
                                partials.append((now_p, " ".join(finals)))
                                last_final_perf = now_p
                                if d.get("speaker_label"):
                                    speakers_seen.add(str(d["speaker_label"]))
                        elif d.get("type") == "Termination":
                            done_evt.set()
                except Exception:
                    pass
                finally:
                    done_evt.set()

            rtask = asyncio.create_task(_recv())
            for i in range(0, len(pcm), chunk_bytes):
                if first_chunk_perf is None:
                    first_chunk_perf = time.perf_counter()
                await ws.send(pcm[i : i + chunk_bytes])
                await asyncio.sleep(0.1)
            last_speech_perf = time.perf_counter()

            for _ in range(10):
                await ws.send(b"\x00" * chunk_bytes)
                await asyncio.sleep(0.1)
            await ws.send(json.dumps({"type": "Terminate"}))
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(done_evt.wait(), timeout=3.0)
            rtask.cancel()
    except Exception as exc:
        return {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}", "reconnects": 1}

    transcript = " ".join(finals).strip()
    stab = compute_interim_stability_metrics(
        partials, transcript, first_chunk_perf, last_final_perf, audio_dur_s
    )
    ttfb_ms = (
        max(0.0, (first_interim_perf - first_chunk_perf) * 1000.0)
        if first_interim_perf and first_chunk_perf
        else None
    )
    tail_ms = (
        max(0.0, (last_final_perf - last_speech_perf) * 1000.0)
        if last_final_perf and last_speech_perf
        else 0.0
    )
    return {
        "status": "OK",
        "transcript": transcript,
        "connect_ms": round(connect_ms, 2),
        "ttfb_interim_ms": round(ttfb_ms, 2) if ttfb_ms is not None else None,
        "final_flush_ms": round(tail_ms, 2),
        "speakers_detected": len(speakers_seen),
        "speaker_labels": sorted(speakers_seen),
        "reconnects": 0,
        "stability": stab,
    }


async def probe_sarvam_availability() -> tuple[str, str]:
    api_key = os.environ.get("SARVAM_API_KEY", "").strip()
    if not api_key:
        return "NOT RUN", "MISSING KEY (SARVAM_API_KEY)"
    url = "wss://api.sarvam.ai/speech-to-text/ws?language-code=en-IN&model=saaras:v3"
    try:
        async with websockets.connect(
            url, additional_headers={"Api-Subscription-Key": api_key}, open_timeout=10.0
        ) as ws:
            b64 = base64.b64encode(b"\x00" * 3200).decode("utf-8")
            payload = {"audio": {"data": b64, "sample_rate": "16000", "encoding": "audio/wav"}}
            await ws.send(json.dumps(payload))
            msg = await asyncio.wait_for(ws.recv(), timeout=4.0)
            return "AVAILABLE", str(msg)[:120]
    except Exception as exc:
        err_str = str(exc)
        if "Credits exhausted" in err_str or "1003" in err_str:
            return "NOT RUN", f"CREDITS EXHAUSTED ({err_str[:80]})"
        return "NOT RUN", f"{type(exc).__name__}: {err_str[:80]}"


async def probe_gladia_live_diarization_param() -> str:
    """Explicitly check Gladia POST /v2/live behavior when diarization=true is sent."""
    api_key = os.environ.get("GLADIA_API_KEY", "").strip()
    if not api_key:
        return "NOT RUN (missing key)"
    body = {
        "model": "solaria-1",
        "encoding": "wav/pcm",
        "bit_depth": 16,
        "sample_rate": 16000,
        "channels": 1,
        "diarization": True,
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(
                "https://api.gladia.io/v2/live?region=eu-west",
                json=body,
                headers={"x-gladia-key": api_key},
            )
            return f"HTTP {r.status_code}: {r.text[:100]}"
    except Exception as exc:
        return f"ERROR: {exc}"


def score_record_trial(record: dict[str, Any], hyp_text: str) -> dict[str, Any]:
    ref_text = record["spoken_text"]
    wer = compute_wer(ref_text, hyp_text)
    nter_res = compute_numeric_token_error_rate(ref_text, hyp_text)
    iemr_res = compute_identifier_exact_match(ref_text, hyp_text)
    eer_res = compute_entity_error_rate(ref_text, hyp_text, record.get("entities", []))
    return {
        "wer": wer,
        "nter": nter_res["nter"],
        "nter_ref": nter_res["total_ref_numbers"],
        "nter_ok": nter_res["correct"],
        "iemr": iemr_res["iemr"],
        "iemr_ref": iemr_res["total_ref"],
        "iemr_ok": iemr_res["correct"],
        "eer": eer_res["entity_error_rate"],
        "eer_ref": eer_res["total_entities"],
        "eer_ok": eer_res["correct"],
    }


def summarize_candidate(
    candidate_key: str,
    trials: list[dict[str, Any]],
    diarize_probe: dict[str, Any] | None = None,
    rtt_delta_ms: float | None = None,
    weights: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Compute aggregate metrics, 95% CIs, hard gates, and owner-weighted scorecard."""
    w = weights or DEFAULT_SCORECARD_WEIGHTS
    ok_trials = [t for t in trials if t["api"]["status"] == "OK"]
    err_count = len(trials) - len(ok_trials)
    reconnects = sum(int(t["api"].get("reconnects", 0)) for t in trials)

    wers = [t["scores"]["wer"] for t in ok_trials]
    h_wers = [t["scores"]["wer"] for t in ok_trials if t["language_tag"] == "hinglish"]

    tot_num_ref = sum(t["scores"]["nter_ref"] for t in ok_trials)
    tot_num_ok = sum(t["scores"]["nter_ok"] for t in ok_trials)
    nter = round((tot_num_ref - tot_num_ok) / tot_num_ref, 4) if tot_num_ref > 0 else 0.0

    tot_id_ref = sum(t["scores"]["iemr_ref"] for t in ok_trials)
    tot_id_ok = sum(t["scores"]["iemr_ok"] for t in ok_trials)
    iemr = round((tot_id_ref - tot_id_ok) / tot_id_ref, 4) if tot_id_ref > 0 else 0.0
    id_exact_match = round(1.0 - iemr, 4)

    tot_ent_ref = sum(t["scores"]["eer_ref"] for t in ok_trials)
    tot_ent_ok = sum(t["scores"]["eer_ok"] for t in ok_trials)
    eer = round((tot_ent_ref - tot_ent_ok) / tot_ent_ref, 4) if tot_ent_ref > 0 else 0.0

    ttfbs = [
        t["api"]["ttfb_interim_ms"]
        for t in ok_trials
        if t["api"].get("ttfb_interim_ms") is not None
    ]
    flushes = [t["api"]["final_flush_ms"] for t in ok_trials]

    all_word_lags: list[float] = []
    all_stable_ms: list[float] = []
    total_retract = 0
    total_interim_toks = 0
    for t in ok_trials:
        st = t["api"].get("stability", {})
        all_word_lags.extend(st.get("word_interim_lags_ms", []))
        all_stable_ms.extend(st.get("stable_times_ms", []))
        total_retract += int(st.get("retractions", 0))
        total_interim_toks += int(st.get("total_interim_words", 0))

    retraction_rate = (
        round(total_retract / float(total_interim_toks), 4) if total_interim_toks > 0 else 0.0
    )

    if not ok_trials:
        wer_ci = {"mean": 1.0, "ci_lower": 1.0, "ci_upper": 1.0}
        h_wer_ci = {"mean": 1.0, "ci_lower": 1.0, "ci_upper": 1.0}
        stable_ci = {"mean": 0.0, "ci_lower": 0.0, "ci_upper": 0.0}
        nter = 1.0
        iemr = 1.0
        id_exact_match = 0.0
        eer = 1.0
    else:
        wer_ci = bootstrap_metric_ci(wers)
        h_wer_ci = bootstrap_metric_ci(h_wers)
        stable_ci = bootstrap_metric_ci(all_stable_ms)

    ttfb_p50 = _pct(ttfbs, 0.50)
    ttfb_p95 = _pct(ttfbs, 0.95)
    word_lag_p50 = _pct(all_word_lags, 0.50)
    word_lag_p95 = _pct(all_word_lags, 0.95)
    stable_p50 = _pct(all_stable_ms, 0.50)
    stable_p95 = _pct(all_stable_ms, 0.95)
    flush_p50 = _pct(flushes, 0.50)
    flush_p95 = _pct(flushes, 0.95)

    meta = VENDOR_METADATA.get(
        candidate_key,
        {
            "name": candidate_key,
            "india_residency": False,
            "live_diarization": False,
            "custom_vocab": False,
            "zero_retention": False,
            "price_mono_usd_hr": 0.55,
            "price_2ch_usd_hr": None,
        },
    )

    if diarize_probe is not None:
        mono_diarize_ok = (
            diarize_probe.get("status") == "OK"
            and int(diarize_probe.get("speakers_detected") or 0) >= 2
        )
    else:
        mono_diarize_ok = bool(meta["live_diarization"])

    rtt_gate_ok = (
        (rtt_delta_ms <= 150.0) if rtt_delta_ms is not None else bool(meta["india_residency"])
    )

    # Hard Gates (A1): mono live diarization + India RTT delta <= 150ms + ZDR + Vocab + Acc
    g_diar = mono_diarize_ok
    g_rtt = rtt_gate_ok
    g_vocab = bool(meta["custom_vocab"])
    g_zdr = bool(meta["zero_retention"])
    g_acc = (wer_ci["mean"] <= 0.15 and id_exact_match >= 0.85) if ok_trials else False
    all_gates = g_diar and g_rtt and g_vocab and g_zdr and g_acc

    # Owner-default weighted composite scorecard [0..100]
    acc_comp = (
        (
            0.40 * max(0.0, 1.0 - wer_ci["mean"])
            + 0.25 * max(0.0, 1.0 - h_wer_ci["mean"])
            + 0.20 * id_exact_match
            + 0.15 * max(0.0, 1.0 - nter)
        )
        if ok_trials
        else 0.0
    )
    diar_comp = 1.0 if mono_diarize_ok else 0.0
    net_add = max(0.0, rtt_delta_ms or 0.0)
    eff_stable_ms = stable_p50 + net_add
    lat_comp = max(0.0, 1.0 - (eff_stable_ms / 3000.0))
    vocab_comp = 1.0 if meta["custom_vocab"] else 0.0
    cost_comp = max(0.0, 1.0 - (float(meta["price_mono_usd_hr"]) / 1.0))
    res_comp = 0.5 * (1.0 if meta["india_residency"] else 0.0) + 0.5 * (
        1.0 if meta["zero_retention"] else 0.0
    )
    rel_comp = (
        max(0.0, 1.0 - (err_count / max(1, len(trials))) - 0.1 * reconnects) if trials else 0.0
    )

    weighted_score = round(
        100.0
        * (
            w["accuracy"] * acc_comp
            + w["diarization"] * diar_comp
            + w["latency_stability"] * lat_comp
            + w["vocabulary"] * vocab_comp
            + w["cost"] * cost_comp
            + w["residency_compliance"] * res_comp
            + w["reliability_sdk"] * rel_comp
        ),
        2,
    )

    return {
        "candidate": candidate_key,
        "name": meta["name"],
        "n_trials": len(trials),
        "n_ok": len(ok_trials),
        "errors": err_count,
        "reconnects": reconnects,
        "wer": wer_ci,
        "hinglish_wer": h_wer_ci,
        "nter": nter,
        "iemr": iemr,
        "id_exact_match": id_exact_match,
        "eer": eer,
        "first_partial_p50_ms": ttfb_p50,
        "first_partial_p95_ms": ttfb_p95,
        "word_interim_p50_ms": word_lag_p50,
        "word_interim_p95_ms": word_lag_p95,
        "stable_text_p50_ms": stable_p50,
        "stable_text_p95_ms": stable_p95,
        "stable_text_ci": stable_ci,
        "retraction_rate": retraction_rate,
        "final_flush_p50_ms": flush_p50,
        "final_flush_p95_ms": flush_p95,
        "rtt_delta_vs_dg_in_ms": round(rtt_delta_ms, 2) if rtt_delta_ms is not None else None,
        "price_mono_usd_hr": meta["price_mono_usd_hr"],
        "price_2ch_usd_hr": meta["price_2ch_usd_hr"],
        "gates": {
            "G_live_diarization_mono": g_diar,
            "G_india_rtt_delta_le_150ms": g_rtt,
            "G_custom_vocab": g_vocab,
            "G_zdr_opt_out": g_zdr,
            "G_acc_wer_le_0.15_iem_ge_0.85": g_acc,
            "ALL_HARD_GATES_PASS": all_gates,
        },
        "weighted_score": weighted_score,
    }


async def run_bakeoff_async(args: argparse.Namespace) -> dict[str, Any]:
    load_dotenv(REPO_ROOT / ".env")
    if not MANIFEST_PATH.exists():
        print(f"ERROR: Manifest not found: {MANIFEST_PATH}")
        return {"error": "manifest_not_found"}

    with MANIFEST_PATH.open("r", encoding="utf-8") as f:
        all_records = [json.loads(line) for line in f if line.strip()]

    sample = select_stratified_corpus(all_records, sample_size=args.sample_size, seed=args.seed)

    # 1. Measure DNS/TCP/TLS/WS-upgrade/Ping RTT across regional endpoints
    dg_key = os.environ.get("DEEPGRAM_API_KEY", "").strip()
    gl_key = os.environ.get("GLADIA_API_KEY", "").strip()
    aai_key = os.environ.get("ASSEMBLYAI_API_KEY", "").strip()

    net_probes: dict[str, dict[str, Any]] = {}
    if dg_key:
        net_probes["deepgram_in"] = await measure_endpoint_network(
            "Deepgram India",
            "api.in.deepgram.com",
            "wss://api.in.deepgram.com/v1/listen?model=nova-3&encoding=linear16&sample_rate=16000",
            headers={"Authorization": f"Token {dg_key}"},
        )
    if gl_key:
        gl_body = {
            "model": "solaria-1",
            "encoding": "wav/pcm",
            "bit_depth": 16,
            "sample_rate": 16000,
            "channels": 1,
        }
        net_probes["gladia_eu_west"] = await measure_endpoint_network(
            "Gladia eu-west",
            "api.gladia.io",
            "",
            post_init_url="https://api.gladia.io/v2/live?region=eu-west",
            post_init_headers={"x-gladia-key": gl_key},
            post_init_body=gl_body,
        )
        net_probes["gladia_us_west"] = await measure_endpoint_network(
            "Gladia us-west",
            "api.gladia.io",
            "",
            post_init_url="https://api.gladia.io/v2/live?region=us-west",
            post_init_headers={"x-gladia-key": gl_key},
            post_init_body=gl_body,
        )
    if aai_key:
        net_probes["assemblyai_global"] = await measure_endpoint_network(
            "AssemblyAI Global",
            "streaming.assemblyai.com",
            "wss://streaming.assemblyai.com/v3/ws?sample_rate=16000&speech_model=universal-3-6-pro",
            headers={"Authorization": aai_key},
        )

    dg_rtt_base = net_probes.get("deepgram_in", {}).get("ping_rtt_p50_ms", 0.0)
    rtt_deltas: dict[str, float | None] = {
        "deepgram_nova3": 0.0,
        "deepgram_nova3_multi": 0.0,
        "deepgram_flux": 0.0,
        "gladia_solaria1": (
            round(net_probes["gladia_eu_west"]["ping_rtt_p50_ms"] - dg_rtt_base, 2)
            if "gladia_eu_west" in net_probes
            else None
        ),
        "assemblyai_universal": (
            round(net_probes["assemblyai_global"]["ping_rtt_p50_ms"] - dg_rtt_base, 2)
            if "assemblyai_global" in net_probes
            else None
        ),
    }

    sarvam_status, sarvam_reason = await probe_sarvam_availability()
    openai_has_key = bool(os.environ.get("OPENAI_API_KEY", "").strip())
    gladia_diar_http = await probe_gladia_live_diarization_param()

    candidates = [
        "deepgram_nova3",
        "deepgram_nova3_multi",
        "deepgram_flux",
        "gladia_solaria1",
        "assemblyai_universal",
    ]
    results_by_cand: dict[str, list[dict[str, Any]]] = {c: [] for c in candidates}

    print(
        f"BAKEOFF_CONFIG: corpus=synthetic_eng sample_segments={len(sample)} "
        f"scenarios_covered={len({r['scenario'] for r in sample})} "
        f"repetitions={args.reps} total_trials_per_candidate={len(sample) * args.reps}",
        flush=True,
    )

    # 2. Interleaved live trials across candidates
    for rep in range(args.reps):
        for seg_idx, rec in enumerate(sample):
            pcm = load_pcm16_mono(REPO_ROOT / rec["audio_path"])
            tasks_map = {
                "deepgram_nova3": eval_deepgram_nova3(
                    pcm, BAKEOFF_KEYTERMS, language="en-IN", max_keyterms=100
                ),
                "deepgram_nova3_multi": eval_deepgram_nova3(
                    pcm, BAKEOFF_KEYTERMS, language="multi", max_keyterms=65
                ),
                "deepgram_flux": eval_deepgram_flux(pcm, BAKEOFF_KEYTERMS),
                "gladia_solaria1": eval_gladia_solaria1(
                    pcm, BAKEOFF_KEYTERMS, custom_vocab=True, code_switching=True
                ),
                "assemblyai_universal": eval_assemblyai(pcm, BAKEOFF_KEYTERMS),
            }
            rot = (rep + seg_idx) % len(candidates)
            ordered_keys = candidates[rot:] + candidates[:rot]
            res_list = await asyncio.gather(*(tasks_map[k] for k in ordered_keys))

            for k, api_res in zip(ordered_keys, res_list, strict=True):
                scores = (
                    score_record_trial(rec, api_res["transcript"])
                    if api_res["status"] == "OK"
                    else {}
                )
                results_by_cand[k].append(
                    {
                        "rep": rep + 1,
                        "segment_id": rec["segment_id"],
                        "scenario": rec["scenario"],
                        "variant": rec["variant"],
                        "language_tag": rec["language_tag"],
                        "api": api_res,
                        "scores": scores,
                    }
                )
            print(
                f"PROGRESS rep={rep + 1}/{args.reps} seg={seg_idx + 1}/{len(sample)} "
                f"id={rec['segment_id']}",
                flush=True,
            )

    # 3. Dedicated 2-speaker mono & overlap diarization probes
    p_m = REPO_ROOT / "ml/eval/audio/synthetic_eng/data/scenario_01_daily_standup/turn_00_clean.wav"
    p_f = REPO_ROOT / "ml/eval/audio/synthetic_eng/data/scenario_01_daily_standup/turn_01_clean.wav"
    p_ovl = (
        REPO_ROOT / "ml/eval/audio/synthetic_eng/data/scenario_01_daily_standup/turn_00_overlap.wav"
    )
    pcm_2spk_seq = load_pcm16_mono(p_m) + (b"\x00" * 16000) + load_pcm16_mono(p_f)
    pcm_ovl = load_pcm16_mono(p_ovl) if p_ovl.exists() else pcm_2spk_seq

    seq_probes = await asyncio.gather(
        eval_deepgram_nova3(pcm_2spk_seq, BAKEOFF_KEYTERMS, language="en-IN", max_keyterms=100),
        eval_deepgram_nova3(pcm_2spk_seq, BAKEOFF_KEYTERMS, language="multi", max_keyterms=65),
        eval_deepgram_flux(pcm_2spk_seq, BAKEOFF_KEYTERMS),
        eval_gladia_solaria1(pcm_2spk_seq, BAKEOFF_KEYTERMS),
        eval_assemblyai(pcm_2spk_seq, BAKEOFF_KEYTERMS),
    )
    diar_seq_results = dict(zip(candidates, seq_probes, strict=True))

    ovl_probes = await asyncio.gather(
        eval_deepgram_nova3(pcm_ovl, BAKEOFF_KEYTERMS, language="en-IN", max_keyterms=100),
        eval_deepgram_nova3(pcm_ovl, BAKEOFF_KEYTERMS, language="multi", max_keyterms=65),
        eval_deepgram_flux(pcm_ovl, BAKEOFF_KEYTERMS),
        eval_gladia_solaria1(pcm_ovl, BAKEOFF_KEYTERMS),
        eval_assemblyai(pcm_ovl, BAKEOFF_KEYTERMS),
    )
    diar_ovl_results = dict(zip(candidates, ovl_probes, strict=True))

    # 4. Gladia ablations: Vocab ON/OFF and Hinglish Code-Switching ON/OFF
    id_segs = [r for r in sample if extract_identifiers(r["spoken_text"])][:6] or sample[:6]
    hinglish_segs = [r for r in sample if r["language_tag"] == "hinglish"][:6]

    gladia_vocab_off_trials: list[dict[str, Any]] = []
    gladia_vocab_on_subset: list[dict[str, Any]] = []
    for rec in id_segs:
        pcm = load_pcm16_mono(REPO_ROOT / rec["audio_path"])
        api_off = await eval_gladia_solaria1(pcm, [], custom_vocab=False, code_switching=True)
        gladia_vocab_off_trials.append(
            {
                "segment_id": rec["segment_id"],
                "variant": rec["variant"],
                "language_tag": rec["language_tag"],
                "api": api_off,
                "scores": score_record_trial(rec, api_off["transcript"])
                if api_off["status"] == "OK"
                else {},
            }
        )
        m = [
            t
            for t in results_by_cand["gladia_solaria1"]
            if t["rep"] == 1 and t["segment_id"] == rec["segment_id"]
        ]
        if m:
            gladia_vocab_on_subset.append(m[0])

    gladia_cs_off_trials: list[dict[str, Any]] = []
    gladia_cs_on_subset: list[dict[str, Any]] = []
    for rec in hinglish_segs:
        pcm = load_pcm16_mono(REPO_ROOT / rec["audio_path"])
        api_cs_off = await eval_gladia_solaria1(
            pcm, BAKEOFF_KEYTERMS, custom_vocab=True, code_switching=False
        )
        gladia_cs_off_trials.append(
            {
                "segment_id": rec["segment_id"],
                "variant": rec["variant"],
                "language_tag": rec["language_tag"],
                "api": api_cs_off,
                "scores": score_record_trial(rec, api_cs_off["transcript"])
                if api_cs_off["status"] == "OK"
                else {},
            }
        )
        m = [
            t
            for t in results_by_cand["gladia_solaria1"]
            if t["rep"] == 1 and t["segment_id"] == rec["segment_id"]
        ]
        if m:
            gladia_cs_on_subset.append(m[0])

    summaries = {
        c: summarize_candidate(
            c,
            results_by_cand[c],
            diarize_probe=diar_seq_results[c],
            rtt_delta_ms=rtt_deltas.get(c),
        )
        for c in candidates
    }
    ablation_summary = {
        "gladia_vocab_on_subset": summarize_candidate("gladia_solaria1", gladia_vocab_on_subset),
        "gladia_vocab_off_subset": summarize_candidate("gladia_solaria1", gladia_vocab_off_trials),
        "gladia_cs_on_hinglish": summarize_candidate("gladia_solaria1", gladia_cs_on_subset),
        "gladia_cs_off_hinglish": summarize_candidate("gladia_solaria1", gladia_cs_off_trials),
    }

    report = {
        "corpus": "ml/eval/audio/synthetic_eng",
        "sample_size": len(sample),
        "scenarios_covered": sorted({r["scenario"] for r in sample}),
        "repetitions": args.reps,
        "weights": DEFAULT_SCORECARD_WEIGHTS,
        "latency_definitions": {
            "first_partial_lag_ms": "first_interim_recv_perf_ms - first_speech_chunk_send_perf_ms",
            "word_interim_latency_ms": "first_partial_word_k_perf_ms - est_word_k_send_perf_ms",
            "time_to_stable_text_ms": "earliest_stable_word_k_perf_ms - est_word_k_send_perf_ms",
            "final_flush_ms": "last_final_recv_perf_ms - last_speech_chunk_send_perf_ms",
        },
        "network_probes": net_probes,
        "rtt_deltas_vs_deepgram_in_ms": rtt_deltas,
        "diarization_2spk_mono": {
            "gladia_post_diarize_check": gladia_diar_http,
            "sequential": {
                k: {
                    "speakers_detected": v.get("speakers_detected"),
                    "speaker_labels": v.get("speaker_labels"),
                }
                for k, v in diar_seq_results.items()
            },
            "overlap": {
                k: {
                    "speakers_detected": v.get("speakers_detected"),
                    "speaker_labels": v.get("speaker_labels"),
                    "transcript": v.get("transcript"),
                }
                for k, v in diar_ovl_results.items()
            },
        },
        "summaries": summaries,
        "gladia_ablations": ablation_summary,
        "skipped_candidates": {
            "sarvam": {"status": sarvam_status, "reason": sarvam_reason},
            "openai": {
                "status": "NOT RUN",
                "reason": "MISSING KEY (OPENAI_API_KEY)" if not openai_has_key else "NOT SCHEDULED",
            },
        },
        "raw_trials": results_by_cand,
    }

    # DATA ONLY printouts
    print("\n" + "=" * 116)
    print("NETWORK PROBE DATA (HOST -> CLOUD ENDPOINTS)")
    print(
        f"{'Endpoint':<22} {'DNS_P50':>8} {'TCP_P50':>8} {'TLS_P50':>8} "
        f"{'WS_Upg':>8} {'Ping_P50':>9} {'Ping_P95':>9}"
    )
    print("-" * 116)
    for np_item in net_probes.values():
        print(
            f"{np_item['name']:<22} {np_item['dns_p50_ms']:>8.1f} {np_item['tcp_p50_ms']:>8.1f} "
            f"{np_item['tls_p50_ms']:>8.1f} {np_item['ws_upgrade_ms']:>8.1f} "
            f"{np_item['ping_rtt_p50_ms']:>9.1f} {np_item['ping_rtt_p95_ms']:>9.1f}"
        )

    print("\n" + "=" * 116)
    print("CANDIDATE ACCURACY MATRIX (DATA ONLY)")
    print(
        f"{'Candidate':<26} {'N':>3} {'Err':>3} {'Rec':>3} {'WER (95% CI)':<21} "
        f"{'Hinglish WER':<20} {'NTER':>7} {'IEMR':>7} {'ID_Match':>8} {'EER':>7}"
    )
    print("-" * 116)
    for c in candidates:
        s = summaries[c]
        wer_s = f"{s['wer']['mean']:.3f}[{s['wer']['ci_lower']:.3f},{s['wer']['ci_upper']:.3f}]"
        hw = s["hinglish_wer"]
        hw_s = f"{hw['mean']:.3f}[{hw['ci_lower']:.3f},{hw['ci_upper']:.3f}]"
        print(
            f"{s['name']:<26} {s['n_ok']:>3} {s['errors']:>3} {s['reconnects']:>3} {wer_s:<21} "
            f"{hw_s:<20} {s['nter']:>7.4f} {s['iemr']:>7.4f} "
            f"{s['id_exact_match']:>8.4f} {s['eer']:>7.4f}"
        )
    print(f"{'Sarvam Saaras v3':<26} NOT RUN: {sarvam_reason}")
    print(f"{'OpenAI Realtime':<26} NOT RUN: MISSING KEY (OPENAI_API_KEY)")

    print("\n" + "=" * 116)
    print("LATENCY, STABILITY, RETRACTION & PRICING MATRIX (DATA ONLY)")
    print(
        f"{'Candidate':<26} {'1stPart_P50':>11} {'WordLag_P50':>11} {'Stable_P50':>11} "
        f"{'Stable_P95':>11} {'Retract%':>9} {'Flush_P95':>10} {'RTT+ms':>8} "
        f"{'$/hr_1ch':>9} {'$/hr_2ch':>10}"
    )
    print("-" * 116)
    for c in candidates:
        s = summaries[c]
        p2 = f"{s['price_2ch_usd_hr']:.3f}" if s["price_2ch_usd_hr"] is not None else "UNVERIFIED"
        rd = (
            f"{s['rtt_delta_vs_dg_in_ms']:+.1f}"
            if s["rtt_delta_vs_dg_in_ms"] is not None
            else "N/A"
        )
        print(
            f"{s['name']:<26} {s['first_partial_p50_ms']:>11.1f} {s['word_interim_p50_ms']:>11.1f} "
            f"{s['stable_text_p50_ms']:>11.1f} {s['stable_text_p95_ms']:>11.1f} "
            f"{s['retraction_rate'] * 100.0:>8.2f}% {s['final_flush_p95_ms']:>10.1f} "
            f"{rd:>8} {s['price_mono_usd_hr']:>9.3f} {p2:>10}"
        )

    print("\n" + "=" * 116)
    print(f"2-SPEAKER MONO DIARIZATION & GLADIA POST CHECK ({gladia_diar_http})")
    for c in candidates:
        sq = diar_seq_results[c]
        ov = diar_ovl_results[c]
        sq_lbl = sq.get("speaker_labels") or ["UNSUPPORTED"]
        ov_lbl = ov.get("speaker_labels") or ["UNSUPPORTED"]
        print(
            f"{c:<26} seq_spk={sq.get('speakers_detected')} labels={sq_lbl} | "
            f"ovl_spk={ov.get('speakers_detected')} labels={ov_lbl}"
        )

    print("\n" + "=" * 116)
    print("OWNER-DEFAULT WEIGHTED SCORECARD & HARD GATES (DATA ONLY)")
    print(
        f"{'Candidate':<26} {'G_Diar':>7} {'G_RTT<=150':>11} {'G_Vocab':>8} {'G_ZDR':>6} "
        f"{'G_Acc':>6} {'ALL_PASS':>9} {'Score(0-100)':>13}"
    )
    print("-" * 116)
    for c in candidates:
        s = summaries[c]
        g = s["gates"]
        print(
            f"{s['name']:<26} {str(g['G_live_diarization_mono']):>7} "
            f"{str(g['G_india_rtt_delta_le_150ms']):>11} {str(g['G_custom_vocab']):>8} "
            f"{str(g['G_zdr_opt_out']):>6} {str(g['G_acc_wer_le_0.15_iem_ge_0.85']):>6} "
            f"{str(g['ALL_HARD_GATES_PASS']):>9} {s['weighted_score']:>13.2f}"
        )

    print("\n" + "=" * 116)
    print(f"GLADIA ABLATION DATA (N_ID={len(id_segs)}, N_HINGLISH={len(hinglish_segs)})")
    for lbl, ab in ablation_summary.items():
        print(
            f"{lbl:<26} n={ab['n_ok']} wer={ab['wer']['mean']:.4f} "
            f"iemr={ab['iemr']:.4f} nter={ab['nter']:.4f} eer={ab['eer']:.4f}"
        )

    out_path = Path(args.json_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nJSON_ARTIFACT_WRITTEN: {out_path}")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="MeetMind AI ASR Bake-off Evaluation Runner")
    parser.add_argument("--sample-size", type=int, default=15, help="Stratified segment count")
    parser.add_argument("--reps", type=int, default=3, help="Interleaved repetition count")
    parser.add_argument("--seed", type=int, default=42, help="Sampling seed")
    parser.add_argument(
        "--json-out",
        type=str,
        default="logs/bakeoff_summary.json",
        help="Path to write JSON results artifact",
    )
    args = parser.parse_args()
    asyncio.run(run_bakeoff_async(args))
    return 0


if __name__ == "__main__":
    sys.exit(main())
