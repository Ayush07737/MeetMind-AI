"""MeetMind AI — ASR Provider Bake-off Evaluation Runner (A1 / B4 / B5).

Evaluates streaming and async ASR candidates against Engineering Corpus v2
(`ml/eval/audio/synthetic_eng/manifest.jsonl`) with >=150 stratified segments across
all 6 variants, 3 interleaved repetitions, two configurations (vendor default vs
aggressive endpointing from official docs), all 3 latency definitions (word-level
interim, time-to-stable-text, vendor end-of-turn), script-aware Hinglish scoring
(`indic-transliteration` MIT + English-token WER + ID/numeric/entity retention),
segment-level 95% bootstrap CIs with explicit N_eff, 100-hour mono/2-ch cost tables,
and `--dry-run` connectivity/auth checks without billed audio streaming.

OUTPUT RULE: Prints DATA ONLY. No explanatory prose or recommendations.
"""

from __future__ import annotations

import argparse
import array
import asyncio
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
from collections import Counter, defaultdict
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
    TRANSLIT_LIBRARY,
    TRANSLIT_LICENSE,
    compute_entity_error_rate,
    compute_hinglish_english_wer,
    compute_identifier_exact_match,
    compute_numeric_token_error_rate,
    detect_script,
    extract_canonical_numbers,
    extract_identifiers,
    transliterate_devanagari_to_latin,
)
from normalize import (  # noqa: E402
    compute_normalized_wer as compute_wer,
)

MANIFEST_PATH = REPO_ROOT / "ml/eval/audio/synthetic_eng/manifest.jsonl"
ALL_VARIANTS = [
    "clean",
    "noise_15db",
    "telephony_8khz",
    "speed_1_15x",
    "two_channel",
    "overlap",
]
EUR_TO_USD_REF = 1.08  # Documented reference FX rate for Gladia EUR prices (2026-11-11)

DEFAULT_SCORECARD_WEIGHTS: dict[str, float] = {
    "accuracy": 0.35,
    "diarization": 0.15,
    "latency_stability": 0.20,
    "vocabulary": 0.10,
    "cost": 0.10,
    "residency_compliance": 0.05,
    "reliability_sdk": 0.05,
}

# Canonical key resolution matching scripts/check_provider_keys.py (A1.9)
PROVIDER_ENV_ALIASES: dict[str, list[str]] = {
    "deepgram": ["DEEPGRAM_API_KEY", "DG_API_KEY"],
    "gladia": ["GLADIA_API_KEY", "GLADIA_KEY"],
    "assemblyai": ["ASSEMBLYAI_API_KEY", "AAI_API_KEY"],
    "sarvam": ["SARVAM_API_KEY", "SARVAM_API_SUBSCRIPTION_KEY"],
    "openai": ["OPENAI_API_KEY", "OPENAI_KEY"],
}


def resolve_provider_key(provider: str) -> tuple[str, str | None]:
    """Resolve API key using the same primary/alias precedence as check_provider_keys.py."""
    for var_name in PROVIDER_ENV_ALIASES[provider]:
        val = os.environ.get(var_name, "").strip()
        if val:
            return val, var_name
    return "", None


# Verified official docs & pricing (access dates 2026-11-11 / 2026-11-12)
VENDOR_METADATA: dict[str, dict[str, Any]] = {
    "deepgram_nova3": {
        "name": "Deepgram Nova-3 (en-IN)",
        "region": "api.in.deepgram.com (India)",
        "india_residency": True,
        "live_diarization": True,
        "custom_vocab": True,
        "zero_retention": True,
        "price_mono_usd_hr": 0.462,  # $0.0077/min PAYG mono base; deepgram.com/pricing
        "price_addon_diar_usd_hr": 0.120,  # +$0.0020/min
        "price_addon_kt_usd_hr": 0.078,  # +$0.0013/min
        "price_with_addons_usd_hr": 0.660,  # $0.0110/min mono (base + diarize + keyterm)
        "price_2ch_usd_hr": 0.924,  # 2x mono base ($0.0154/min multichannel)
        "price_2ch_with_addons_usd_hr": 1.320,  # 2x mono with addons
        "pricing_note": (
            "$0.0077/min PAYG +$0.002/min diarize +$0.0013/min kt; deepgram.com/pricing"
        ),
    },
    "deepgram_nova3_multi": {
        "name": "Deepgram Nova-3 (multi)",
        "region": "api.in.deepgram.com (India)",
        "india_residency": True,
        "live_diarization": True,
        "custom_vocab": True,
        "zero_retention": True,
        "price_mono_usd_hr": 0.552,  # $0.0092/min multi PAYG; deepgram.com/pricing
        "price_addon_diar_usd_hr": 0.120,
        "price_addon_kt_usd_hr": 0.078,
        "price_with_addons_usd_hr": 0.750,
        "price_2ch_usd_hr": 1.104,
        "price_2ch_with_addons_usd_hr": 1.500,
        "pricing_note": "$0.0092/min multi PAYG +$0.002 diarize +$0.0013 kt; deepgram.com/pricing",
    },
    "deepgram_flux": {
        "name": "Deepgram Flux",
        "region": "api.in.deepgram.com (India)",
        "india_residency": True,
        "live_diarization": False,  # TurnInfo lacks speaker attribution
        "custom_vocab": True,
        "zero_retention": True,
        "price_mono_usd_hr": 0.462,  # $0.0077/min PAYG
        "price_addon_diar_usd_hr": 0.0,
        "price_addon_kt_usd_hr": 0.078,
        "price_with_addons_usd_hr": 0.540,
        "price_2ch_usd_hr": None,  # UNVERIFIED on /v2/listen multichannel
        "price_2ch_with_addons_usd_hr": None,
        "pricing_note": "$0.0077/min PAYG +$0.0013 kt; 2-ch UNVERIFIED; deepgram.com/pricing",
    },
    "gladia_solaria1": {
        "name": "Gladia Solaria-1 Live",
        "region": "eu-west / us-west (No India)",
        "india_residency": False,
        "live_diarization": False,  # POST /v2/live returns HTTP 400 when diarization=true
        "custom_vocab": True,
        "zero_retention": False,  # Self-serve requires enterprise for ZDR
        "price_mono_usd_hr": round(0.68 * EUR_TO_USD_REF, 3),  # EUR 0.68/hr live
        "price_addon_diar_usd_hr": 0.0,  # Diarization unsupported on live
        "price_addon_kt_usd_hr": 0.0,  # Custom vocab included
        "price_with_addons_usd_hr": round(0.68 * EUR_TO_USD_REF, 3),
        "price_2ch_usd_hr": round(2.0 * 0.68 * EUR_TO_USD_REF, 3),  # Billed 2x per docs
        "price_2ch_with_addons_usd_hr": round(2.0 * 0.68 * EUR_TO_USD_REF, 3),
        "pricing_note": "EUR 0.68/hr (~$0.734/hr) mono, 2x for 2-ch; gladia.io/pricing",
    },
    "assemblyai_universal": {
        "name": "AssemblyAI U-3.6 Pro",
        "region": "Global / US / EU (No India)",
        "india_residency": False,
        "live_diarization": True,
        "custom_vocab": True,
        "zero_retention": False,  # Streaming ZDR/deletion enterprise-gated; verify DPA
        "price_mono_usd_hr": 0.450,  # $0.45/hr Universal-3.6 Pro Realtime (kt included)
        "price_addon_diar_usd_hr": 0.120,  # +$0.12/hr Speaker Diarization
        "price_addon_kt_usd_hr": 0.0,  # Included in U3.6 Pro Realtime
        "price_with_addons_usd_hr": 0.570,  # $0.45 + $0.12 diarization
        "price_2ch_usd_hr": 0.900,  # 2 separate WS sessions per multichannel docs
        "price_2ch_with_addons_usd_hr": 0.900,  # Separate channels need no per-session diarize
        "pricing_note": "$0.45/hr U3.6 Pro +$0.12 diarize (kt incl), 2x for 2 WS ch; assemblyai",
    },
    "sarvam": {
        "name": "Sarvam Saaras v4/v3",
        "region": "ap-south-1 (India)",
        "india_residency": True,
        "live_diarization": False,  # Streaming has no diarization; batch only
        "custom_vocab": True,
        "zero_retention": False,
        "price_mono_usd_hr": 0.350,  # INR 0.49/min = INR 29.40/hr (~$0.35/hr at 84 INR/USD)
        "price_addon_diar_usd_hr": 0.0,
        "price_addon_kt_usd_hr": 0.0,
        "price_with_addons_usd_hr": 0.350,
        "price_2ch_usd_hr": None,  # Realtime docs specify "Mono only"
        "price_2ch_with_addons_usd_hr": None,
        "pricing_note": "INR 0.49/min (mono only); docs.sarvam.ai",
    },
    "openai": {
        "name": "OpenAI gpt-live-transcribe",
        "region": "US / EU (No India audio residency)",
        "india_residency": False,
        "live_diarization": False,  # Model docs: Speaker Labels Not supported
        "custom_vocab": True,  # keywords array in Realtime transcription
        "zero_retention": True,  # With org ZDR approval; regional India omits /v1/realtime
        "price_mono_usd_hr": 1.020,  # $0.017/min = $1.02/hr
        "price_addon_diar_usd_hr": 0.0,
        "price_addon_kt_usd_hr": 0.0,
        "price_with_addons_usd_hr": 1.020,
        "price_2ch_usd_hr": None,
        "price_2ch_with_addons_usd_hr": None,
        "pricing_note": "$0.017/min ($1.02/hr) gpt-live-transcribe; developers.openai.com",
    },
    "deepgram_async": {
        "name": "Deepgram Nova-3 Async",
        "region": "api.in.deepgram.com (India)",
        "price_mono_usd_hr": 0.258,  # $0.0043/min Pre-recorded PAYG
        "price_with_addons_usd_hr": 0.456,  # +$0.0020 diarize +$0.0013 keyterms
        "price_2ch_usd_hr": 0.516,
        "pricing_note": (
            "$0.0043/min base (+$0.002/min diarize +$0.0013/min kt); deepgram.com/pricing"
        ),
    },
    "assemblyai_async": {
        "name": "AssemblyAI U-3.5 Pro Async",
        "region": "Global / EU / US",
        "price_mono_usd_hr": 0.210,  # $0.21/hr Universal-3.5 Pro
        "price_with_addons_usd_hr": 0.280,  # +$0.02 diarize +$0.05 keyterms
        "price_2ch_usd_hr": 0.210,
        "pricing_note": "$0.21/hr U3.5 Pro +$0.02/hr diarize +$0.05/hr keyterms; assemblyai.com",
    },
    "gladia_solaria1_async": {
        "name": "Gladia Solaria-1 Async",
        "region": "EU / US",
        "price_mono_usd_hr": round(0.27 * EUR_TO_USD_REF, 3),  # EUR 0.27/hr (~$0.292/hr)
        "price_with_addons_usd_hr": round(0.27 * EUR_TO_USD_REF, 3),  # Diarize+vocab incl
        "price_2ch_usd_hr": round(2 * 0.27 * EUR_TO_USD_REF, 3),
        "pricing_note": "EUR 0.27/hr (~$0.292/hr) diarize+vocab incl; gladia.io/pricing",
    },
    "gladia_solaria3_async": {
        "name": "Gladia Solaria-3 Async",
        "region": "EU / US",
        "price_mono_usd_hr": round(0.16 * EUR_TO_USD_REF, 3),  # EUR 0.16/hr (~$0.173/hr)
        "price_with_addons_usd_hr": round(0.16 * EUR_TO_USD_REF, 3),
        "price_2ch_usd_hr": round(2 * 0.16 * EUR_TO_USD_REF, 3),
        "pricing_note": "EUR 0.16/hr (~$0.173/hr) single EU lang only, no CS; gladia.io/pricing",
    },
}

# Streaming configuration profiles (vendor default vs official-docs aggressive endpointing)
STREAMING_CONFIG_SPECS: dict[str, dict[str, str]] = {
    "deepgram_nova3": {
        "default": "endpointing=10ms(default),utterance_end_ms=unset,vad_events=false",
        "aggressive": "endpointing=10ms,utterance_end_ms=1000ms,vad_events=true",
    },
    "deepgram_nova3_multi": {
        "default": "endpointing=10ms(default),utterance_end_ms=unset,vad_events=false",
        "aggressive": "endpointing=10ms,utterance_end_ms=1000ms,vad_events=true",
    },
    "deepgram_flux": {
        "default": (
            "eot_threshold=0.7(default),eager_eot_threshold=unset,eot_timeout_ms=5000(default)"
        ),
        "aggressive": "eot_threshold=0.5,eager_eot_threshold=0.4,eot_timeout_ms=1000",
    },
    "gladia_solaria1": {
        "default": "endpointing=0.05s(default),max_dur_without_endpointing=5s(default)",
        "aggressive": "endpointing=0.01s,max_dur_without_endpointing=5s,speech_events=true",
    },
    "assemblyai_universal": {
        "default": (
            "mode=balanced/diarize_defaults(min_turn_silence=640,max_turn_silence=768,"
            "speaker_labels=true)"
        ),
        "aggressive": (
            "mode=min_latency,min_turn_silence=128,max_turn_silence=400,"
            "interruption_delay=0,eot_conf=0.4,speaker_labels=true"
        ),
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
    """Compute 95% bootstrap CI by resampling across independent segment-level values."""
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
    t = transliterate_devanagari_to_latin(text or "")
    cleaned = re.sub(r"[^a-z0-9\s]", " ", t.lower())
    return [tok for tok in cleaned.split() if tok]


def load_pcm16_audio(wav_path: Path) -> tuple[bytes, bytes, int, float]:
    """Read WAV returning (mono_pcm16, raw_pcm16, nchannels, duration_s).

    For stereo (`two_channel`) corpus WAVs, cleanly mixes ch0 + ch1 with int16
    clipping so mono-only providers receive 16kHz 1x-speed audio rather than
    interleaved stereo played at 0.5x speed.
    """
    with wave.open(str(wav_path), "rb") as wf:
        nch = wf.getnchannels()
        sr = wf.getframerate()
        nframes = wf.getnframes()
        raw = wf.readframes(nframes)
    dur_s = nframes / float(sr)
    if nch == 1:
        return raw, raw, 1, dur_s
    samples = array.array("h")
    samples.frombytes(raw)
    mono = array.array("h")
    for idx in range(0, len(samples), nch):
        acc = sum(samples[idx : idx + nch])
        if acc > 32767:
            acc = 32767
        elif acc < -32768:
            acc = -32768
        mono.append(acc)
    return mono.tobytes(), raw, nch, dur_s


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

        first_seen_ts: float | None = None
        for ts, p_toks in tok_partials:
            if len(p_toks) > k:
                first_seen_ts = ts
                break
        if first_seen_ts is not None:
            word_interim_lags.append(max(0.0, (first_seen_ts - est_spoken_perf) * 1000.0))

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


def select_stratified_corpus(
    all_records: list[dict[str, Any]], sample_size: int = 150, seed: int = 42
) -> list[dict[str, Any]]:
    """Stratify >= sample_size unique segments across all 6 variants and all 10 scenarios.

    Balances variants equally (e.g. 25 per variant at N=150) while ensuring coverage across
    every scenario, Hinglish turns, identifiers, and numeric tokens.
    """
    rng = random.Random(seed)
    by_variant: dict[str, list[dict[str, Any]]] = {v: [] for v in ALL_VARIANTS}
    for r in all_records:
        v = r.get("variant", "")
        if v in by_variant:
            by_variant[v].append(r)

    per_variant_target = max(1, sample_size // len(ALL_VARIANTS))
    remainder = sample_size - per_variant_target * len(ALL_VARIANTS)

    selected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for v_idx, variant in enumerate(ALL_VARIANTS):
        target_v = per_variant_target + (1 if v_idx < remainder else 0)
        pool = list(by_variant[variant])
        scenarios = sorted({r["scenario"] for r in pool})
        rng.shuffle(pool)

        picked_v: list[dict[str, Any]] = []

        def _try_add(
            rec: dict[str, Any],
            _picked: list[dict[str, Any]] = picked_v,
            _target: int = target_v,
        ) -> bool:
            if len(_picked) >= _target or rec["segment_id"] in seen_ids:
                return False
            _picked.append(rec)
            seen_ids.add(rec["segment_id"])
            return True

        # 1. Cover every scenario in this variant first (rotating Hinglish / ID / Numeric)
        for s_idx, sc in enumerate(scenarios):
            if len(picked_v) >= target_v:
                break
            sc_recs = [r for r in pool if r["scenario"] == sc and r["segment_id"] not in seen_ids]
            if not sc_recs:
                continue
            hing = [r for r in sc_recs if r.get("language_tag") == "hinglish"]
            ids = [r for r in sc_recs if extract_identifiers(r["spoken_text"])]
            nums = [r for r in sc_recs if extract_canonical_numbers(r["spoken_text"])]
            if s_idx % 4 == 0 and hing:
                cand = hing[0]
            elif s_idx % 2 == 1 and ids:
                cand = ids[0]
            else:
                cand = (nums or ids or hing or sc_recs)[0]
            _try_add(cand)

        # 2. Priority fill: Hinglish -> Identifiers -> Numeric -> General
        for pred in (
            lambda r: r.get("language_tag") == "hinglish",
            lambda r: bool(extract_identifiers(r["spoken_text"])),
            lambda r: bool(extract_canonical_numbers(r["spoken_text"])),
            lambda _r: True,
        ):
            for rec in pool:
                if len(picked_v) >= target_v:
                    break
                if pred(rec):
                    _try_add(rec)

        selected.extend(picked_v)

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
    skip_ws: bool = False,
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

    if not skip_ws:
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
                target_ws,
                additional_headers=headers,
                open_timeout=15.0,
                family=socket.AF_INET,
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
    config_mode: str = "default",
    channels: int = 1,
) -> dict[str, Any]:
    api_key, _ = resolve_provider_key("deepgram")
    if not api_key:
        return {"status": "NOT_RUN", "error": "missing DEEPGRAM_API_KEY"}

    params: list[tuple[str, str]] = [
        ("model", "nova-3"),
        ("encoding", "linear16"),
        ("sample_rate", "16000"),
        ("channels", str(channels)),
        ("diarize", "true"),
        ("interim_results", "true"),
        ("language", language),
        ("mip_opt_out", "true"),
    ]
    if channels > 1:
        params.append(("multichannel", "true"))
    if config_mode == "aggressive":
        params.extend([("endpointing", "10"), ("utterance_end_ms", "1000"), ("vad_events", "true")])
    for kt in keyterms[:max_keyterms]:
        params.append(("keyterm", kt))
    url = f"wss://api.in.deepgram.com/v1/listen?{urllib.parse.urlencode(params)}"

    chunk_bytes = 3200 * channels
    finals: list[str] = []
    partials: list[tuple[float, str]] = []
    speakers_seen: set[int] = set()
    channels_seen: set[int] = set()
    first_chunk_perf: float | None = None
    first_interim_perf: float | None = None
    last_speech_perf: float | None = None
    last_final_perf: float | None = None
    speech_final_perf: float | None = None
    utterance_end_perf: float | None = None
    audio_dur_s = len(pcm) / float(32000 * channels)

    t_conn0 = time.perf_counter()
    try:
        async with websockets.connect(
            url,
            additional_headers={"Authorization": f"Token {api_key}"},
            open_timeout=20.0,
            family=socket.AF_INET,
        ) as ws:
            connect_ms = (time.perf_counter() - t_conn0) * 1000.0
            done_evt = asyncio.Event()
            eot_evt = asyncio.Event()

            async def _recv() -> None:
                nonlocal first_interim_perf, last_final_perf, speech_final_perf
                nonlocal utterance_end_perf
                try:
                    async for msg in ws:
                        d = json.loads(msg)
                        mtype = d.get("type")
                        now_p = time.perf_counter()
                        if mtype == "Results":
                            alts = d.get("channel", {}).get("alternatives", [])
                            ch_idx = d.get("channel_index", [0])[0]
                            txt = (alts[0].get("transcript") or "").strip() if alts else ""
                            is_fin = bool(d.get("is_final", False))
                            is_sf = bool(d.get("speech_final", False))
                            if txt and not is_fin:
                                partials.append((now_p, " ".join([*finals, txt]).strip()))
                                if first_interim_perf is None:
                                    first_interim_perf = now_p
                            if txt and is_fin:
                                finals.append(txt)
                                channels_seen.add(int(ch_idx))
                                partials.append((now_p, " ".join(finals)))
                                last_final_perf = now_p
                                if is_sf:
                                    speech_final_perf = now_p
                                for w in alts[0].get("words", []):
                                    if "speaker" in w:
                                        speakers_seen.add(int(w["speaker"]))
                            if is_sf and last_speech_perf and now_p >= last_speech_perf:
                                speech_final_perf = now_p
                                eot_evt.set()
                        elif mtype == "UtteranceEnd":
                            utterance_end_perf = now_p
                            if last_speech_perf and now_p >= last_speech_perf:
                                eot_evt.set()
                        elif mtype == "Metadata":
                            done_evt.set()
                except Exception:
                    pass
                finally:
                    done_evt.set()
                    eot_evt.set()

            rtask = asyncio.create_task(_recv())
            for i in range(0, len(pcm), chunk_bytes):
                if first_chunk_perf is None:
                    first_chunk_perf = time.perf_counter()
                await ws.send(pcm[i : i + chunk_bytes])
                await asyncio.sleep(0.1)
            last_speech_perf = time.perf_counter()

            # Send trailing silence up to 2.0s; stop once endpoint final arrives
            for _ in range(20):
                if eot_evt.is_set():
                    break
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
    eot_anchor = speech_final_perf or last_final_perf or utterance_end_perf
    eot_ms = (
        max(0.0, (eot_anchor - last_speech_perf) * 1000.0)
        if eot_anchor and last_speech_perf
        else 0.0
    )
    return {
        "status": "OK",
        "transcript": transcript,
        "connect_ms": round(connect_ms, 2),
        "ttfb_interim_ms": round(ttfb_ms, 2) if ttfb_ms is not None else None,
        "end_of_turn_latency_ms": round(eot_ms, 2),
        "speakers_detected": len(speakers_seen),
        "speaker_labels": sorted(str(x) for x in speakers_seen),
        "channels_detected": sorted(channels_seen),
        "reconnects": 0,
        "stability": stab,
    }


async def eval_deepgram_flux(
    pcm: bytes,
    keyterms: list[str],
    config_mode: str = "default",
) -> dict[str, Any]:
    api_key, _ = resolve_provider_key("deepgram")
    if not api_key:
        return {"status": "NOT_RUN", "error": "missing DEEPGRAM_API_KEY"}

    params: list[tuple[str, str]] = [
        ("model", "flux-general-en"),
        ("encoding", "linear16"),
        ("sample_rate", "16000"),
        ("mip_opt_out", "true"),
    ]
    if config_mode == "aggressive":
        params.extend(
            [
                ("eot_threshold", "0.5"),
                ("eager_eot_threshold", "0.4"),
                ("eot_timeout_ms", "1000"),
            ]
        )
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
    last_eager_perf: float | None = None
    audio_dur_s = len(pcm) / 32000.0

    t_conn0 = time.perf_counter()
    try:
        async with websockets.connect(
            url,
            additional_headers={"Authorization": f"Token {api_key}"},
            open_timeout=20.0,
            family=socket.AF_INET,
        ) as ws:
            connect_ms = (time.perf_counter() - t_conn0) * 1000.0
            done_evt = asyncio.Event()
            eot_evt = asyncio.Event()

            async def _recv() -> None:
                nonlocal first_interim_perf, last_final_perf, last_eager_perf, latest_update
                try:
                    async for msg in ws:
                        d = json.loads(msg)
                        if d.get("type") == "TurnInfo":
                            ev = d.get("event")
                            txt = (d.get("transcript") or "").strip()
                            now_p = time.perf_counter()
                            if txt and ev in (
                                "StartOfTurn",
                                "Update",
                                "EagerEndOfTurn",
                                "TurnResumed",
                            ):
                                latest_update = txt
                                partials.append((now_p, " ".join([*finals, txt]).strip()))
                                if first_interim_perf is None:
                                    first_interim_perf = now_p
                            if ev == "EagerEndOfTurn":
                                last_eager_perf = now_p
                            if txt and ev == "EndOfTurn":
                                finals.append(txt)
                                partials.append((now_p, " ".join(finals)))
                                latest_update = ""
                                last_final_perf = now_p
                                if last_speech_perf and now_p >= last_speech_perf:
                                    eot_evt.set()
                except Exception:
                    pass
                finally:
                    done_evt.set()
                    eot_evt.set()

            rtask = asyncio.create_task(_recv())
            for i in range(0, len(pcm), chunk_bytes):
                if first_chunk_perf is None:
                    first_chunk_perf = time.perf_counter()
                await ws.send(pcm[i : i + chunk_bytes])
                await asyncio.sleep(0.1)
            last_speech_perf = time.perf_counter()

            # Allow up to 2.5s trailing silence so TurnInfo EndOfTurn fires naturally
            max_wait = 55 if config_mode == "default" else 20
            for _ in range(max_wait):
                if eot_evt.is_set():
                    break
                await ws.send(b"\x00" * chunk_bytes)
                await asyncio.sleep(0.1)
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
    eot_ms = max(0.0, (last_final_perf - last_speech_perf) * 1000.0) if last_speech_perf else 0.0
    eager_ms = (
        max(0.0, (last_eager_perf - last_speech_perf) * 1000.0)
        if last_eager_perf and last_speech_perf
        else None
    )
    return {
        "status": "OK",
        "transcript": transcript,
        "connect_ms": round(connect_ms, 2),
        "ttfb_interim_ms": round(ttfb_ms, 2) if ttfb_ms is not None else None,
        "end_of_turn_latency_ms": round(eot_ms, 2),
        "eager_eot_latency_ms": round(eager_ms, 2) if eager_ms is not None else None,
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
    config_mode: str = "default",
    channels: int = 1,
) -> dict[str, Any]:
    api_key, _ = resolve_provider_key("gladia")
    if not api_key:
        return {"status": "NOT_RUN", "error": "missing GLADIA_API_KEY"}

    langs = ["en", "hi"] if code_switching else ["en"]
    body: dict[str, Any] = {
        "model": "solaria-1",
        "encoding": "wav/pcm",
        "bit_depth": 16,
        "sample_rate": 16000,
        "channels": channels,
        "language_config": {"languages": langs, "code_switching": code_switching},
        "messages_config": {
            "receive_partial_transcripts": True,
            "receive_final_transcripts": True,
            "receive_speech_events": True,
        },
    }
    if config_mode == "aggressive":
        body["endpointing"] = 0.01
        body["maximum_duration_without_endpointing"] = 5
    if custom_vocab and keyterms:
        body["realtime_processing"] = {
            "custom_vocabulary": True,
            "custom_vocabulary_config": {"vocabulary": keyterms[:100]},
        }

    t_conn0 = time.perf_counter()
    try:
        async with _ipv4_client(timeout=20.0) as client:
            r = await client.post(
                f"https://api.gladia.io/v2/live?region={region}",
                json=body,
                headers={"x-gladia-key": api_key},
            )
            r.raise_for_status()
            ws_url = r.json()["url"]

        chunk_bytes = 3200 * channels
        finals: list[str] = []
        partials: list[tuple[float, str]] = []
        speakers_seen: set[str] = set()
        channels_seen: set[int] = set()
        first_chunk_perf: float | None = None
        first_interim_perf: float | None = None
        last_speech_perf: float | None = None
        last_final_perf: float | None = None
        audio_dur_s = len(pcm) / float(32000 * channels)

        async with websockets.connect(ws_url, open_timeout=20.0, family=socket.AF_INET) as ws:
            connect_ms = (time.perf_counter() - t_conn0) * 1000.0
            done_evt = asyncio.Event()
            eot_evt = asyncio.Event()

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
                            if utt.get("channel") is not None:
                                channels_seen.add(int(utt["channel"]))
                            if txt and not is_fin:
                                partials.append((now_p, " ".join([*finals, txt]).strip()))
                                if first_interim_perf is None:
                                    first_interim_perf = now_p
                            if txt and is_fin:
                                finals.append(txt)
                                partials.append((now_p, " ".join(finals)))
                                last_final_perf = now_p
                                if last_speech_perf and now_p >= last_speech_perf:
                                    eot_evt.set()
                        elif d.get("type") == "stop_recording_ack":
                            done_evt.set()
                except Exception:
                    pass
                finally:
                    done_evt.set()
                    eot_evt.set()

            rtask = asyncio.create_task(_recv())
            for i in range(0, len(pcm), chunk_bytes):
                if first_chunk_perf is None:
                    first_chunk_perf = time.perf_counter()
                await ws.send(pcm[i : i + chunk_bytes])
                await asyncio.sleep(0.1)
            last_speech_perf = time.perf_counter()

            for _ in range(20):
                if eot_evt.is_set():
                    break
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
    eot_ms = (
        max(0.0, (last_final_perf - last_speech_perf) * 1000.0)
        if last_final_perf and last_speech_perf
        else 0.0
    )
    return {
        "status": "OK",
        "transcript": transcript,
        "connect_ms": round(connect_ms, 2),
        "ttfb_interim_ms": round(ttfb_ms, 2) if ttfb_ms is not None else None,
        "end_of_turn_latency_ms": round(eot_ms, 2),
        "speakers_detected": len(speakers_seen),
        "speaker_labels": sorted(speakers_seen),
        "channels_detected": sorted(channels_seen),
        "reconnects": 0,
        "stability": stab,
    }


async def eval_assemblyai(
    pcm: bytes,
    keyterms: list[str],
    config_mode: str = "default",
    speaker_labels: bool = True,
) -> dict[str, Any]:
    api_key, _ = resolve_provider_key("assemblyai")
    if not api_key:
        return {"status": "NOT_RUN", "error": "missing ASSEMBLYAI_API_KEY"}

    kt_json = json.dumps(keyterms[:100])
    q: list[tuple[str, str]] = [
        ("sample_rate", "16000"),
        ("speech_model", "universal-3-6-pro"),
        ("speaker_labels", "true" if speaker_labels else "false"),
        ("keyterms_prompt", kt_json),
    ]
    if config_mode == "aggressive":
        q.extend(
            [
                ("mode", "min_latency"),
                ("min_turn_silence", "128"),
                ("max_turn_silence", "400"),
                ("interruption_delay", "0"),
                ("end_of_turn_confidence_threshold", "0.4"),
            ]
        )
    url = f"wss://streaming.assemblyai.com/v3/ws?{urllib.parse.urlencode(q)}"

    chunk_bytes = 3200
    finals: list[str] = []
    partials: list[tuple[float, str]] = []
    speakers_seen: set[str] = set()
    first_chunk_perf: float | None = None
    first_interim_perf: float | None = None
    last_speech_perf: float | None = None
    last_final_perf: float | None = None
    server_cfg: dict[str, Any] = {}
    audio_dur_s = len(pcm) / 32000.0

    t_conn0 = time.perf_counter()
    try:
        async with websockets.connect(
            url,
            additional_headers={"Authorization": api_key},
            open_timeout=20.0,
            family=socket.AF_INET,
        ) as ws:
            connect_ms = (time.perf_counter() - t_conn0) * 1000.0
            done_evt = asyncio.Event()
            eot_evt = asyncio.Event()

            async def _recv() -> None:
                nonlocal first_interim_perf, last_final_perf, server_cfg
                try:
                    async for msg in ws:
                        d = json.loads(msg)
                        mtype = d.get("type")
                        if mtype == "Begin":
                            server_cfg = d.get("configuration") or {}
                        elif mtype == "Turn":
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
                                if d.get("speaker_label") and d["speaker_label"] != "PENDING":
                                    speakers_seen.add(str(d["speaker_label"]))
                                for w in d.get("words") or []:
                                    sp = w.get("speaker")
                                    if sp and sp != "PENDING":
                                        speakers_seen.add(str(sp))
                                if last_speech_perf and now_p >= last_speech_perf:
                                    eot_evt.set()
                        elif mtype == "Termination":
                            done_evt.set()
                except Exception:
                    pass
                finally:
                    done_evt.set()
                    eot_evt.set()

            rtask = asyncio.create_task(_recv())
            for i in range(0, len(pcm), chunk_bytes):
                if first_chunk_perf is None:
                    first_chunk_perf = time.perf_counter()
                await ws.send(pcm[i : i + chunk_bytes])
                await asyncio.sleep(0.1)
            last_speech_perf = time.perf_counter()

            for _ in range(20):
                if eot_evt.is_set():
                    break
                await ws.send(b"\x00" * chunk_bytes)
                await asyncio.sleep(0.1)
            await ws.send(json.dumps({"type": "Terminate"}))
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
    eot_ms = (
        max(0.0, (last_final_perf - last_speech_perf) * 1000.0)
        if last_final_perf and last_speech_perf
        else 0.0
    )
    return {
        "status": "OK",
        "transcript": transcript,
        "connect_ms": round(connect_ms, 2),
        "ttfb_interim_ms": round(ttfb_ms, 2) if ttfb_ms is not None else None,
        "end_of_turn_latency_ms": round(eot_ms, 2),
        "speakers_detected": len(speakers_seen),
        "speaker_labels": sorted(speakers_seen),
        "server_config": server_cfg,
        "reconnects": 0,
        "stability": stab,
    }


async def probe_sarvam_availability() -> tuple[str, str]:
    api_key, _ = resolve_provider_key("sarvam")
    if not api_key:
        return "NOT RUN", "MISSING KEY (SARVAM_API_KEY)"
    rt_url = (
        "wss://api.sarvam.ai/speech-to-text-realtime/ws"
        "?language_code=en-IN&model=saaras:v4&stream_type=fast&sample_rate=16000&encoding=linear16"
    )
    rt_msg = ""
    try:
        async with websockets.connect(
            rt_url, additional_headers={"api-subscription-key": api_key}, open_timeout=10.0
        ) as ws:
            msg = await asyncio.wait_for(ws.recv(), timeout=4.0)
            rt_msg = str(msg)[:120]
            if "quota_exceeded" in rt_msg or "402" in rt_msg:
                return "NOT RUN", f"HTTP 402 CREDITS EXHAUSTED (WS: {rt_msg})"
            return "AVAILABLE", rt_msg
    except Exception as exc:
        err_str = str(exc)
        if "Credits exhausted" in err_str or "1003" in err_str or "402" in err_str:
            return "NOT RUN", f"CREDITS EXHAUSTED ({err_str[:90]})"
        return "NOT RUN", f"{type(exc).__name__}: {err_str[:90]}"


async def probe_sarvam_rest_availability() -> tuple[str, str]:
    api_key, _ = resolve_provider_key("sarvam")
    if not api_key:
        return "NOT RUN", "MISSING KEY (SARVAM_API_KEY)"
    wav_sample = (
        REPO_ROOT / "ml/eval/audio/synthetic_eng/data/scenario_01_daily_standup/turn_00_clean.wav"
    )
    if not wav_sample.exists():
        return "NOT RUN", "sample wav missing"
    try:
        async with _ipv4_client(timeout=15.0) as client:
            r = await client.post(
                "https://api.sarvam.ai/speech-to-text",
                headers={"api-subscription-key": api_key},
                data={"model": "saaras:v3", "language_code": "en-IN", "mode": "transcribe"},
                files={"file": ("turn_00_clean.wav", wav_sample.read_bytes(), "audio/wav")},
            )
        if r.status_code == 200:
            return "AVAILABLE", f"HTTP 200: {r.text[:100]}"
        return "NOT RUN", f"HTTP {r.status_code}: {r.text[:140]}"
    except Exception as exc:
        return "NOT RUN", f"ERROR: {exc}"


async def probe_gladia_live_diarization_param() -> dict[str, Any]:
    """Explicitly check Gladia POST /v2/live JSON body with diarization=true (A1.5)."""
    api_key, _ = resolve_provider_key("gladia")
    redacted_body = {
        "model": "solaria-1",
        "encoding": "wav/pcm",
        "bit_depth": 16,
        "sample_rate": 16000,
        "channels": 1,
        "diarization": True,
    }
    if not api_key:
        return {"status": "NOT_RUN", "request_body": redacted_body, "response": "missing key"}
    try:
        async with _ipv4_client(timeout=10.0) as client:
            r = await client.post(
                "https://api.gladia.io/v2/live?region=eu-west",
                json=redacted_body,
                headers={"x-gladia-key": api_key},
            )
            return {
                "endpoint": "POST https://api.gladia.io/v2/live?region=eu-west",
                "headers_redacted": {"x-gladia-key": "<REDACTED>"},
                "request_body": redacted_body,
                "http_status": r.status_code,
                "response_body": r.text[:300],
            }
    except Exception as exc:
        return {"request_body": redacted_body, "http_status": 0, "response_body": str(exc)}


async def eval_async_deepgram(
    wav_path: Path,
    keyterms: list[str],
    language: str = "en-IN",
    max_keyterms: int = 100,
    multichannel: bool = False,
) -> dict[str, Any]:
    api_key, _ = resolve_provider_key("deepgram")
    if not api_key:
        return {"status": "NOT_RUN", "error": "missing DEEPGRAM_API_KEY"}
    params: list[tuple[str, str]] = [
        ("model", "nova-3"),
        ("language", language),
        ("diarize", "true"),
        ("smart_format", "true"),
        ("mip_opt_out", "true"),
    ]
    if multichannel:
        params.append(("multichannel", "true"))
    for kt in keyterms[:max_keyterms]:
        params.append(("keyterm", kt))
    url = f"https://api.in.deepgram.com/v1/listen?{urllib.parse.urlencode(params)}"
    t0 = time.perf_counter()
    try:
        async with _ipv4_client(timeout=30.0) as client:
            r = await client.post(
                url,
                headers={"Authorization": f"Token {api_key}", "Content-Type": "audio/wav"},
                content=wav_path.read_bytes(),
            )
            r.raise_for_status()
            data = r.json()
    except Exception as exc:
        return {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}"}

    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    chs = data.get("results", {}).get("channels", [])
    words: list[dict[str, Any]] = []
    texts: list[str] = []
    speakers_seen: set[str] = set()
    for ch_idx, ch in enumerate(chs):
        alts = ch.get("alternatives", [])
        if alts:
            t = (alts[0].get("transcript") or "").strip()
            if t:
                texts.append(t)
            for w in alts[0].get("words", []):
                sp = w.get("speaker")
                if sp is not None:
                    speakers_seen.add(str(sp))
                words.append(
                    {
                        "word": w.get("punctuated_word") or w.get("word") or "",
                        "start": float(w.get("start", 0.0)),
                        "end": float(w.get("end", 0.0)),
                        "speaker": str(sp) if sp is not None else f"ch{ch_idx}",
                    }
                )
    return {
        "status": "OK",
        "transcript": " ".join(texts).strip(),
        "latency_ms": round(elapsed_ms, 2),
        "speakers_detected": len(speakers_seen),
        "speaker_labels": sorted(speakers_seen),
        "words": words,
    }


async def eval_async_assemblyai(
    wav_path: Path,
    keyterms: list[str],
    multichannel: bool = False,
) -> dict[str, Any]:
    api_key, _ = resolve_provider_key("assemblyai")
    if not api_key:
        return {"status": "NOT_RUN", "error": "missing ASSEMBLYAI_API_KEY"}
    headers = {"authorization": api_key}
    t0 = time.perf_counter()
    try:
        async with _ipv4_client(timeout=45.0) as client:
            up = await client.post(
                "https://api.assemblyai.com/v2/upload",
                headers=headers,
                content=wav_path.read_bytes(),
            )
            up.raise_for_status()
            audio_url = up.json()["upload_url"]

            payload: dict[str, Any] = {
                "audio_url": audio_url,
                "speech_models": ["universal-3-5-pro", "universal-2"],
                "language_detection": True,
                "speaker_labels": True,
                "keyterms_prompt": keyterms[:100],
            }
            if multichannel:
                payload["multichannel"] = True
            sub = await client.post(
                "https://api.assemblyai.com/v2/transcript",
                headers=headers,
                json=payload,
            )
            sub.raise_for_status()
            tid = sub.json()["id"]

            res: dict[str, Any] = {}
            for _ in range(60):
                poll = await client.get(
                    f"https://api.assemblyai.com/v2/transcript/{tid}", headers=headers
                )
                poll.raise_for_status()
                res = poll.json()
                if res.get("status") == "completed":
                    break
                if res.get("status") == "error":
                    return {"status": "ERROR", "error": str(res.get("error"))}
                await asyncio.sleep(1.0)
            else:
                return {"status": "ERROR", "error": "poll_timeout"}
    except Exception as exc:
        return {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}"}

    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    words: list[dict[str, Any]] = []
    speakers_seen: set[str] = set()
    for u in res.get("utterances") or []:
        if u.get("speaker") is not None:
            speakers_seen.add(str(u["speaker"]))
        for w in u.get("words") or []:
            sp = w.get("speaker") or u.get("speaker")
            if sp is not None:
                speakers_seen.add(str(sp))
            words.append(
                {
                    "word": w.get("text", ""),
                    "start": float(w.get("start", 0.0)) / 1000.0,
                    "end": float(w.get("end", 0.0)) / 1000.0,
                    "speaker": str(sp) if sp is not None else "0",
                }
            )
    if not speakers_seen and res.get("words"):
        for w in res["words"]:
            if w.get("speaker"):
                speakers_seen.add(str(w["speaker"]))
            words.append(
                {
                    "word": w.get("text", ""),
                    "start": float(w.get("start", 0.0)) / 1000.0,
                    "end": float(w.get("end", 0.0)) / 1000.0,
                    "speaker": str(w.get("speaker", "0")),
                }
            )
    return {
        "status": "OK",
        "transcript": (res.get("text") or "").strip(),
        "latency_ms": round(elapsed_ms, 2),
        "speech_model_used": res.get("speech_model_used"),
        "speakers_detected": len(speakers_seen),
        "speaker_labels": sorted(speakers_seen),
        "words": words,
    }


async def eval_async_gladia(
    wav_path: Path,
    keyterms: list[str],
    model: str = "solaria-1",
    code_switching: bool = True,
) -> dict[str, Any]:
    api_key, _ = resolve_provider_key("gladia")
    if not api_key:
        return {"status": "NOT_RUN", "error": "missing GLADIA_API_KEY"}
    headers = {"x-gladia-key": api_key}
    t0 = time.perf_counter()
    try:
        async with _ipv4_client(timeout=45.0) as client:
            up = await client.post(
                "https://api.gladia.io/v2/upload",
                headers=headers,
                files={"audio": (wav_path.name, wav_path.read_bytes(), "audio/wav")},
            )
            up.raise_for_status()
            audio_url = up.json()["audio_url"]

            langs = ["en", "hi"] if (code_switching and model == "solaria-1") else ["en"]
            payload: dict[str, Any] = {
                "audio_url": audio_url,
                "model": model,
                "diarization": True,
                "custom_vocabulary": True,
                "custom_vocabulary_config": {"vocabulary": keyterms[:100]},
                "language_config": {
                    "languages": langs,
                    "code_switching": bool(code_switching and model == "solaria-1"),
                },
            }
            sub = await client.post(
                "https://api.gladia.io/v2/pre-recorded",
                headers=headers,
                json=payload,
            )
            sub.raise_for_status()
            jid = sub.json()["id"]

            res: dict[str, Any] = {}
            for _ in range(60):
                poll = await client.get(
                    f"https://api.gladia.io/v2/pre-recorded/{jid}", headers=headers
                )
                poll.raise_for_status()
                res = poll.json()
                if res.get("status") == "done":
                    break
                if res.get("status") == "error":
                    return {"status": "ERROR", "error": str(res.get("error"))}
                await asyncio.sleep(1.0)
            else:
                return {"status": "ERROR", "error": "poll_timeout"}
    except Exception as exc:
        return {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}"}

    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    tx = (res.get("result") or {}).get("transcription") or {}
    full_txt = (tx.get("full_transcript") or "").strip()
    speakers_seen: set[str] = set()
    words: list[dict[str, Any]] = []
    for u in tx.get("utterances") or []:
        sp = u.get("speaker")
        if sp is not None:
            speakers_seen.add(str(sp))
        for w in u.get("words") or []:
            words.append(
                {
                    "word": w.get("word", ""),
                    "start": float(w.get("start", 0.0)),
                    "end": float(w.get("end", 0.0)),
                    "speaker": str(sp) if sp is not None else "0",
                }
            )
    return {
        "status": "OK",
        "transcript": full_txt,
        "latency_ms": round(elapsed_ms, 2),
        "speakers_detected": len(speakers_seen),
        "speaker_labels": sorted(speakers_seen),
        "words": words,
    }


def score_record_trial(record: dict[str, Any], hyp_text: str) -> dict[str, Any]:
    ref_text = record["spoken_text"]
    script = detect_script(hyp_text)
    wer_translit = compute_wer(ref_text, hyp_text, transliterate_devanagari=True)
    wer_raw = compute_wer(ref_text, hyp_text, transliterate_devanagari=False)
    is_h = record.get("language_tag") == "hinglish"
    h_eng_wer = compute_hinglish_english_wer(ref_text, hyp_text) if is_h else None
    nter_res = compute_numeric_token_error_rate(ref_text, hyp_text)
    iemr_res = compute_identifier_exact_match(ref_text, hyp_text)
    eer_res = compute_entity_error_rate(ref_text, hyp_text, record.get("entities", []))
    return {
        "script": script,
        "wer": wer_translit,
        "wer_raw_no_translit": wer_raw,
        "hinglish_eng_wer": h_eng_wer,
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


def _group_segment_means(trials: list[dict[str, Any]], extractor: Any) -> tuple[list[float], int]:
    """Group trial values by unique segment_id -> list of segment means and N_eff."""
    by_seg: dict[str, list[float]] = defaultdict(list)
    for t in trials:
        val = extractor(t)
        if val is not None:
            by_seg[t["segment_id"]].append(float(val))
    means = [sum(v) / len(v) for v in by_seg.values() if v]
    return means, len(means)


def _compute_slice_metrics(trials: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute segment-level CIs & rates across a trial subset."""
    ok_trials = [t for t in trials if t["api"]["status"] == "OK"]
    err_count = len(trials) - len(ok_trials)
    reconnects = sum(int(t["api"].get("reconnects", 0)) for t in trials)
    unique_segs = {t["segment_id"] for t in ok_trials}
    n_eff = len(unique_segs)

    script_counts = dict(Counter(t["scores"]["script"] for t in ok_trials))
    h_trials = [t for t in ok_trials if t["language_tag"] == "hinglish"]
    h_script_counts = dict(Counter(t["scores"]["script"] for t in h_trials))

    wer_seg_means, n_eff_wer = _group_segment_means(ok_trials, lambda t: t["scores"]["wer"])
    h_trans_means, n_eff_h = _group_segment_means(h_trials, lambda t: t["scores"]["wer"])
    h_raw_means, _ = _group_segment_means(h_trials, lambda t: t["scores"]["wer_raw_no_translit"])
    h_eng_means, _ = _group_segment_means(h_trials, lambda t: t["scores"]["hinglish_eng_wer"])

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

    # Hinglish-only numeric, ID, entity retention
    h_num_ref = sum(t["scores"]["nter_ref"] for t in h_trials)
    h_num_ok = sum(t["scores"]["nter_ok"] for t in h_trials)
    h_nter = round((h_num_ref - h_num_ok) / h_num_ref, 4) if h_num_ref > 0 else 0.0
    h_id_ref = sum(t["scores"]["iemr_ref"] for t in h_trials)
    h_id_ok = sum(t["scores"]["iemr_ok"] for t in h_trials)
    h_id_match = round(h_id_ok / h_id_ref, 4) if h_id_ref > 0 else 1.0
    h_ent_ref = sum(t["scores"]["eer_ref"] for t in h_trials)
    h_ent_ok = sum(t["scores"]["eer_ok"] for t in h_trials)
    h_eer = round((h_ent_ref - h_ent_ok) / h_ent_ref, 4) if h_ent_ref > 0 else 0.0

    # Latencies: pooled word/trial + segment-level mean CIs
    all_word_lags: list[float] = []
    all_stable_ms: list[float] = []
    eots: list[float] = []
    eager_eots: list[float] = []
    ttfbs: list[float] = []
    total_retract = 0
    total_interim_toks = 0

    for t in ok_trials:
        st = t["api"].get("stability", {})
        all_word_lags.extend(st.get("word_interim_lags_ms", []))
        all_stable_ms.extend(st.get("stable_times_ms", []))
        total_retract += int(st.get("retractions", 0))
        total_interim_toks += int(st.get("total_interim_words", 0))
        if t["api"].get("ttfb_interim_ms") is not None:
            ttfbs.append(float(t["api"]["ttfb_interim_ms"]))
        if t["api"].get("end_of_turn_latency_ms") is not None:
            eots.append(float(t["api"]["end_of_turn_latency_ms"]))
        if t["api"].get("eager_eot_latency_ms") is not None:
            eager_eots.append(float(t["api"]["eager_eot_latency_ms"]))

    seg_stable_means, n_eff_lat = _group_segment_means(
        ok_trials,
        lambda t: (
            sum(t["api"].get("stability", {}).get("stable_times_ms", []))
            / len(t["api"].get("stability", {}).get("stable_times_ms", []))
            if t["api"].get("stability", {}).get("stable_times_ms")
            else None
        ),
    )
    seg_eot_means, _ = _group_segment_means(
        ok_trials, lambda t: t["api"].get("end_of_turn_latency_ms")
    )

    retraction_rate = (
        round(total_retract / float(total_interim_toks), 4) if total_interim_toks > 0 else 0.0
    )

    return {
        "n_trials": len(trials),
        "n_ok": len(ok_trials),
        "n_eff": n_eff,
        "errors": err_count,
        "reconnects": reconnects,
        "script_dist": script_counts,
        "hinglish_script_dist": h_script_counts,
        "wer": bootstrap_metric_ci(wer_seg_means)
        if wer_seg_means
        else {"mean": 1.0, "ci_lower": 1.0, "ci_upper": 1.0},
        "wer_n_eff": n_eff_wer,
        "hinglish_wer": bootstrap_metric_ci(h_trans_means)
        if h_trans_means
        else {"mean": 1.0, "ci_lower": 1.0, "ci_upper": 1.0},
        "hinglish_english_wer": bootstrap_metric_ci(h_eng_means)
        if h_eng_means
        else {"mean": 1.0, "ci_lower": 1.0, "ci_upper": 1.0},
        "hinglish_raw_no_translit_wer_mean": round(sum(h_raw_means) / len(h_raw_means), 4)
        if h_raw_means
        else 1.0,
        "hinglish_n_eff": n_eff_h,
        "hinglish_nter": h_nter,
        "hinglish_id_exact_match": h_id_match,
        "hinglish_eer": h_eer,
        "nter": nter,
        "iemr": iemr,
        "id_exact_match": id_exact_match,
        "eer": eer,
        "first_partial_p50_ms": _pct(ttfbs, 0.50),
        "first_partial_p95_ms": _pct(ttfbs, 0.95),
        "word_interim_p50_ms": _pct(all_word_lags, 0.50),
        "word_interim_p95_ms": _pct(all_word_lags, 0.95),
        "stable_text_p50_ms": _pct(all_stable_ms, 0.50),
        "stable_text_p95_ms": _pct(all_stable_ms, 0.95),
        "stable_text_n": n_eff_lat,
        "stable_text_ci": bootstrap_metric_ci(seg_stable_means),
        "eot_p50_ms": _pct(eots, 0.50),
        "eot_p95_ms": _pct(eots, 0.95),
        "eot_ci": bootstrap_metric_ci(seg_eot_means),
        "eager_eot_p50_ms": _pct(eager_eots, 0.50) if eager_eots else None,
        "eager_eot_p95_ms": _pct(eager_eots, 0.95) if eager_eots else None,
        "retraction_rate": retraction_rate,
    }


def summarize_candidate(
    entry_key: str,
    base_candidate: str,
    config_mode: str,
    trials: list[dict[str, Any]],
    diarize_probe: dict[str, Any] | None = None,
    diarize_ovl_probe: dict[str, Any] | None = None,
    rtt_delta_ms: float | None = None,
    weights: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Compute overall + per-variant stats, segment-level CIs, hard gates, scorecard, 100h cost."""
    w = weights or DEFAULT_SCORECARD_WEIGHTS
    overall = _compute_slice_metrics(trials)
    per_variant = {
        v: _compute_slice_metrics([t for t in trials if t["variant"] == v]) for v in ALL_VARIANTS
    }

    meta = VENDOR_METADATA[base_candidate]
    probes = [p for p in (diarize_probe, diarize_ovl_probe) if p is not None]
    if probes:
        mono_diarize_ok = any(
            p.get("status") == "OK" and int(p.get("speakers_detected") or 0) >= 2 for p in probes
        )
    else:
        mono_diarize_ok = bool(meta["live_diarization"])

    rtt_gate_ok = (
        (rtt_delta_ms <= 150.0) if rtt_delta_ms is not None else bool(meta["india_residency"])
    )

    g_diar = mono_diarize_ok
    g_rtt = rtt_gate_ok
    g_vocab = bool(meta["custom_vocab"])
    g_zdr = bool(meta["zero_retention"])
    ok_n = overall["n_ok"]
    g_acc = (
        (overall["wer"]["mean"] <= 0.15 and overall["id_exact_match"] >= 0.85) if ok_n else False
    )
    all_gates = g_diar and g_rtt and g_vocab and g_zdr and g_acc

    acc_comp = (
        (
            0.40 * max(0.0, 1.0 - overall["wer"]["mean"])
            + 0.25 * max(0.0, 1.0 - overall["hinglish_english_wer"]["mean"])
            + 0.20 * overall["id_exact_match"]
            + 0.15 * max(0.0, 1.0 - overall["nter"])
        )
        if ok_n
        else 0.0
    )
    diar_comp = 1.0 if mono_diarize_ok else 0.0
    eff_stable_ms = overall["stable_text_p50_ms"] + max(0.0, rtt_delta_ms or 0.0)
    lat_comp = max(0.0, 1.0 - (eff_stable_ms / 3000.0))
    vocab_comp = 1.0 if meta["custom_vocab"] else 0.0
    cost_comp = max(0.0, 1.0 - (float(meta["price_mono_usd_hr"]) / 1.0))
    res_comp = 0.5 * (1.0 if meta["india_residency"] else 0.0) + 0.5 * (
        1.0 if meta["zero_retention"] else 0.0
    )
    rel_comp = (
        max(
            0.0,
            1.0 - (overall["errors"] / max(1, overall["n_trials"])) - 0.1 * overall["reconnects"],
        )
        if overall["n_trials"]
        else 0.0
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

    p1 = meta["price_mono_usd_hr"]
    pa = meta["price_with_addons_usd_hr"]
    p2 = meta["price_2ch_usd_hr"]
    return {
        "entry_key": entry_key,
        "candidate": base_candidate,
        "config_mode": config_mode,
        "config_params": STREAMING_CONFIG_SPECS[base_candidate][config_mode],
        "name": f"{meta['name']} [{config_mode}]",
        **overall,
        "per_variant": per_variant,
        "rtt_delta_vs_dg_in_ms": round(rtt_delta_ms, 2) if rtt_delta_ms is not None else None,
        "price_mono_usd_hr": p1,
        "price_with_addons_usd_hr": pa,
        "price_2ch_usd_hr": p2,
        "cost_100h_mono_usd": round(100.0 * p1, 2),
        "cost_100h_with_addons_usd": round(100.0 * pa, 2),
        "cost_100h_2ch_usd": round(100.0 * p2, 2) if p2 is not None else None,
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


def _score_2spk_conversation(
    words: list[dict[str, Any]], expected_spans: list[dict[str, Any]]
) -> dict[str, Any]:
    """Evaluate speaker turn assignment accuracy on stitched multi-speaker WAVs."""
    if not words:
        return {"turn_acc": 0.0, "speakers_detected": 0, "n_spans": len(expected_spans)}
    # Assign each span's dominant hypothesized speaker label, find optimal bijective/majority map
    votes: dict[str, Counter[str]] = defaultdict(Counter)
    span_preds: list[tuple[str, str | None]] = []
    for sp in expected_spans:
        s_t = sp["start_s"]
        e_t = sp["end_s"]
        mid_words = [
            w
            for w in words
            if (w["start"] + w["end"]) * 0.5 >= s_t and (w["start"] + w["end"]) * 0.5 <= e_t
        ]
        if not mid_words:
            span_preds.append((sp["speaker"], None))
            continue
        cnt = Counter(str(w.get("speaker", "0")) for w in mid_words)
        hyp_sp = cnt.most_common(1)[0][0]
        votes[sp["speaker"]][hyp_sp] += 1
        span_preds.append((sp["speaker"], hyp_sp))

    # Map each ref speaker greedily to most-voted hyp label
    ref_to_hyp: dict[str, str] = {}
    used_hyp: set[str] = set()
    pairs: list[tuple[int, str, str]] = []
    for r_sp, c in votes.items():
        for h_sp, count in c.items():
            pairs.append((count, r_sp, h_sp))
    pairs.sort(reverse=True)
    for _, r_sp, h_sp in pairs:
        if r_sp not in ref_to_hyp and h_sp not in used_hyp:
            ref_to_hyp[r_sp] = h_sp
            used_hyp.add(h_sp)

    correct = sum(
        1 for r_sp, h_sp in span_preds if h_sp is not None and ref_to_hyp.get(r_sp) == h_sp
    )
    return {
        "turn_acc": round(correct / max(1, len(expected_spans)), 4),
        "correct_turns": correct,
        "n_spans": len(expected_spans),
        "speakers_detected": len({str(w.get("speaker")) for w in words}),
    }


def _ipv4_client(timeout: float = 20.0) -> httpx.AsyncClient:
    """Force IPv4 in httpx to avoid Windows/ISP IPv6 blackhole ConnectTimeouts."""
    return httpx.AsyncClient(
        timeout=timeout,
        transport=httpx.AsyncHTTPTransport(local_address="0.0.0.0"),
    )


async def run_dry_run_checks(sample: list[dict[str, Any]]) -> dict[str, Any]:
    """Validate keys, host reachability, HTTP auth & Gladia diarization=true (0 audio billed)."""
    key_info = {p: (resolve_provider_key(p)[1] or "ABSENT") for p in PROVIDER_ENV_ALIASES}
    var_counts = dict(Counter(r["variant"] for r in sample))
    sc_counts = dict(Counter(r["scenario"] for r in sample))
    h_count = sum(1 for r in sample if r.get("language_tag") == "hinglish")

    dg_key, _ = resolve_provider_key("deepgram")
    gl_key, _ = resolve_provider_key("gladia")
    aai_key, _ = resolve_provider_key("assemblyai")
    sk_key, _ = resolve_provider_key("sarvam")

    auth_checks: dict[str, Any] = {}
    async with _ipv4_client(timeout=15.0) as client:
        if dg_key:
            try:
                r = await client.get(
                    "https://api.deepgram.com/v1/projects",
                    headers={"Authorization": f"Token {dg_key}"},
                )
                rb = await client.get(
                    f"https://api.deepgram.com/v1/projects/{r.json()['projects'][0]['project_id']}/balances"
                    if r.status_code == 200 and r.json().get("projects")
                    else "https://api.deepgram.com/v1/projects",
                    headers={"Authorization": f"Token {dg_key}"},
                )
                auth_checks["deepgram"] = {
                    "projects_status": r.status_code,
                    "balances_status": rb.status_code,
                }
            except Exception as exc:
                auth_checks["deepgram"] = {"error": f"{type(exc).__name__}: {exc}"}
        if gl_key:
            try:
                r = await client.get(
                    "https://api.gladia.io/v2/pre-recorded?limit=1",
                    headers={"x-gladia-key": gl_key},
                )
                auth_checks["gladia"] = {"prerecorded_list_status": r.status_code}
            except Exception as exc:
                auth_checks["gladia"] = {"error": f"{type(exc).__name__}: {exc}"}
        if aai_key:
            try:
                r = await client.get(
                    "https://api.assemblyai.com/v2/transcript?limit=1",
                    headers={"authorization": aai_key},
                )
                auth_checks["assemblyai"] = {"transcript_list_status": r.status_code}
            except Exception as exc:
                auth_checks["assemblyai"] = {"error": f"{type(exc).__name__}: {exc}"}
        if sk_key:
            try:
                r = await client.post(
                    "https://api.sarvam.ai/speech-to-text",
                    headers={"api-subscription-key": sk_key},
                )
                auth_checks["sarvam_no_file"] = {"status": r.status_code, "body": r.text[:120]}
            except Exception as exc:
                auth_checks["sarvam_no_file"] = {"error": f"{type(exc).__name__}: {exc}"}

    gladia_diar = await probe_gladia_live_diarization_param()
    net = {
        "deepgram_in": await measure_endpoint_network(
            "Deepgram India", "api.in.deepgram.com", "", skip_ws=True
        ),
        "gladia_eu": await measure_endpoint_network("Gladia EU", "api.gladia.io", "", skip_ws=True),
        "aai_global": await measure_endpoint_network(
            "AssemblyAI", "streaming.assemblyai.com", "", skip_ws=True
        ),
    }
    res = {
        "dry_run": True,
        "key_vars": key_info,
        "sample_size": len(sample),
        "variant_counts": var_counts,
        "scenario_count": len(sc_counts),
        "hinglish_count": h_count,
        "auth_checks": auth_checks,
        "gladia_live_diarization_probe": gladia_diar,
        "network_tcp_tls": net,
    }
    print("DRY_RUN_DATA:", json.dumps(res, indent=2))
    return res


async def run_async_bakeoff(sample: list[dict[str, Any]], concurrency: int = 8) -> dict[str, Any]:
    """Run A1.8 async pre-recorded pass across Deepgram, AssemblyAI, Gladia, Sarvam, OpenAI."""
    async_cands = [
        "deepgram_async",
        "assemblyai_async",
        "gladia_solaria1_async",
        "gladia_solaria3_async",
    ]
    results: dict[str, list[dict[str, Any]]] = {c: [] for c in async_cands}
    sem = asyncio.Semaphore(concurrency)

    async def _run_one(rec: dict[str, Any]) -> None:
        wav_path = REPO_ROOT / rec["audio_path"]
        is_2ch = rec.get("variant") == "two_channel"
        async with sem:
            res_map = await asyncio.gather(
                eval_async_deepgram(wav_path, BAKEOFF_KEYTERMS, multichannel=is_2ch),
                eval_async_assemblyai(wav_path, BAKEOFF_KEYTERMS, multichannel=is_2ch),
                eval_async_gladia(
                    wav_path, BAKEOFF_KEYTERMS, model="solaria-1", code_switching=True
                ),
                eval_async_gladia(
                    wav_path, BAKEOFF_KEYTERMS, model="solaria-3", code_switching=False
                ),
            )
        for ck, api_res in zip(async_cands, res_map, strict=True):
            scores = (
                score_record_trial(rec, api_res["transcript"]) if api_res["status"] == "OK" else {}
            )
            results[ck].append(
                {
                    "rep": 1,
                    "segment_id": rec["segment_id"],
                    "scenario": rec["scenario"],
                    "variant": rec["variant"],
                    "language_tag": rec["language_tag"],
                    "api": {
                        "status": api_res["status"],
                        "transcript": api_res.get("transcript", ""),
                        "error": api_res.get("error"),
                        "latency_ms": api_res.get("latency_ms"),
                        "speech_model_used": api_res.get("speech_model_used"),
                        "reconnects": 0,
                        "stability": {},
                    },
                    "scores": scores,
                }
            )

    tasks = [asyncio.create_task(_run_one(rec)) for rec in sample]
    for idx, t in enumerate(asyncio.as_completed(tasks), start=1):
        await t
        if idx % 25 == 0 or idx == len(sample):
            print(f"ASYNC_PROGRESS completed={idx}/{len(sample)}", flush=True)

    # 2-speaker multi-turn stitched WAVs (all 10 scenarios) for diarization turn assignment
    diar_conv_results: dict[str, list[float]] = {c: [] for c in async_cands}
    tmp_dir = REPO_ROOT / "logs" / "tmp_async_2spk"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    scenarios = sorted({r["scenario"] for r in sample})
    conv_tasks = []
    for sc in scenarios:
        # Use clean WAVs directly for scenario turn diarization check
        sc_dir = REPO_ROOT / "ml/eval/audio/synthetic_eng/data" / sc
        p0 = sc_dir / "turn_00_clean.wav"
        p1 = sc_dir / "turn_01_clean.wav"
        p2 = sc_dir / "turn_02_clean.wav"
        if not (p0.exists() and p1.exists()):
            continue
        parts = [(p0, "spk_a"), (p1, "spk_b")]
        if p2.exists():
            parts.append((p2, "spk_a"))
        combined_pcm = bytearray()
        spans: list[dict[str, Any]] = []
        cursor_s = 0.0
        gap_pcm = b"\x00" * 16000  # 0.5s pause
        for wp, spk in parts:
            pcm_m, _, _, d_s = load_pcm16_audio(wp)
            combined_pcm.extend(pcm_m)
            spans.append({"speaker": spk, "start_s": cursor_s, "end_s": cursor_s + d_s})
            cursor_s += d_s + 0.5
            combined_pcm.extend(gap_pcm)
        conv_wav = tmp_dir / f"{sc}_2spk.wav"
        with wave.open(str(conv_wav), "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(16000)
            wf.writeframes(bytes(combined_pcm))

        async def _eval_conv(cw: Path = conv_wav, sp_list: list[dict[str, Any]] = spans) -> None:
            r_map = await asyncio.gather(
                eval_async_deepgram(cw, BAKEOFF_KEYTERMS),
                eval_async_assemblyai(cw, BAKEOFF_KEYTERMS),
                eval_async_gladia(cw, BAKEOFF_KEYTERMS, model="solaria-1", code_switching=True),
                eval_async_gladia(cw, BAKEOFF_KEYTERMS, model="solaria-3", code_switching=False),
            )
            for ck, r_item in zip(async_cands, r_map, strict=True):
                if r_item["status"] == "OK":
                    sc_res = _score_2spk_conversation(r_item.get("words", []), sp_list)
                    diar_conv_results[ck].append(sc_res["turn_acc"])

        conv_tasks.append(_eval_conv())
    await asyncio.gather(*conv_tasks)
    for f in tmp_dir.glob("*.wav"):
        f.unlink(missing_ok=True)
    with contextlib.suppress(OSError):
        tmp_dir.rmdir()

    sarvam_rest_status, sarvam_rest_reason = await probe_sarvam_rest_availability()
    _, oa_var = resolve_provider_key("openai")

    summaries: dict[str, Any] = {}
    for ck in async_cands:
        sl = _compute_slice_metrics(results[ck])
        per_var = {
            v: _compute_slice_metrics([t for t in results[ck] if t["variant"] == v])
            for v in ALL_VARIANTS
        }
        lats = [float(t["api"]["latency_ms"]) for t in results[ck] if t["api"].get("latency_ms")]
        d_accs = diar_conv_results[ck]
        meta = VENDOR_METADATA[ck]
        summaries[ck] = {
            **sl,
            "name": meta["name"],
            "per_variant": per_var,
            "job_turnaround_p50_ms": _pct(lats, 0.50),
            "job_turnaround_p95_ms": _pct(lats, 0.95),
            "diarization_2spk_turn_acc": round(sum(d_accs) / len(d_accs), 4) if d_accs else 0.0,
            "diarization_2spk_n_convs": len(d_accs),
            "price_mono_usd_hr": meta["price_mono_usd_hr"],
            "price_with_addons_usd_hr": meta["price_with_addons_usd_hr"],
            "cost_100h_mono_usd": round(100.0 * meta["price_mono_usd_hr"], 2),
            "cost_100h_with_addons_usd": round(100.0 * meta["price_with_addons_usd_hr"], 2),
        }

    print("\n" + "=" * 124)
    print(f"ASYNC PRE-RECORDED BAKE-OFF MATRIX (N_eff={len(sample)}, DATA ONLY)")
    print(
        f"{'Candidate':<26} {'N_ok':>4} {'Err':>3} {'WER (95% CI)':<21} "
        f"{'H_Eng_WER':>10} {'NTER':>7} {'ID_Match':>8} {'EER':>7} "
        f"{'2SpkAcc':>8} {'JobP50ms':>9} {'$/100h':>8}"
    )
    print("-" * 124)
    for ck in async_cands:
        s = summaries[ck]
        w = s["wer"]
        ws = f"{w['mean']:.3f}[{w['ci_lower']:.3f},{w['ci_upper']:.3f}]"
        print(
            f"{s['name']:<26} {s['n_ok']:>4} {s['errors']:>3} {ws:<21} "
            f"{s['hinglish_english_wer']['mean']:>10.4f} {s['nter']:>7.4f} "
            f"{s['id_exact_match']:>8.4f} {s['eer']:>7.4f} "
            f"{s['diarization_2spk_turn_acc']:>8.4f} {s['job_turnaround_p50_ms']:>9.1f} "
            f"{s['cost_100h_with_addons_usd']:>8.1f}"
        )
    print(f"{'Sarvam REST (saaras:v3)':<26} {sarvam_rest_status}: {sarvam_rest_reason}")
    oa_msg = "MISSING KEY (OPENAI_API_KEY)" if not oa_var else "NOT SCHEDULED"
    print(f"{'OpenAI gpt-transcribe':<26} NOT RUN: {oa_msg}")

    return {
        "mode": "async",
        "sample_size": len(sample),
        "transliteration": {"library": TRANSLIT_LIBRARY, "license": TRANSLIT_LICENSE},
        "summaries": summaries,
        "skipped_candidates": {
            "sarvam_rest": {"status": sarvam_rest_status, "reason": sarvam_rest_reason},
            "openai_async": {"status": "NOT RUN", "reason": oa_msg},
        },
        "raw_trials": results,
    }


async def run_streaming_bakeoff(
    sample: list[dict[str, Any]],
    reps: int,
    configs: list[str],
    concurrency: int,
) -> dict[str, Any]:
    """Run interleaved live streaming bake-off across both default & aggressive configs."""
    dg_key, _ = resolve_provider_key("deepgram")
    gl_key, _ = resolve_provider_key("gladia")
    aai_key, _ = resolve_provider_key("assemblyai")

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
    _, oa_var = resolve_provider_key("openai")
    gladia_diar_http = await probe_gladia_live_diarization_param()

    base_candidates = [
        "deepgram_nova3",
        "deepgram_nova3_multi",
        "deepgram_flux",
        "gladia_solaria1",
        "assemblyai_universal",
    ]
    results_by_entry: dict[str, list[dict[str, Any]]] = {
        f"{c}:{cfg}": [] for cfg in configs for c in base_candidates
    }

    # Bounded semaphores keep concurrency inside vendor ceilings while staying fast
    seg_sem = asyncio.Semaphore(concurrency)
    dg_sem = asyncio.Semaphore(max(4, concurrency * 2))
    gl_sem = asyncio.Semaphore(min(4, max(2, concurrency)))
    aai_sem = asyncio.Semaphore(min(5, max(2, concurrency)))

    print(
        f"BAKEOFF_CONFIG: corpus=synthetic_eng N_eff={len(sample)} "
        f"scenarios_covered={len({r['scenario'] for r in sample})} "
        f"variants_covered={dict(Counter(r['variant'] for r in sample))} "
        f"configs={configs} reps={reps} total_trials_per_entry={len(sample) * reps} "
        f"translit={TRANSLIT_LIBRARY}({TRANSLIT_LICENSE})",
        flush=True,
    )

    async def _run_entry(cfg_mode: str, rep: int, seg_idx: int, rec: dict[str, Any]) -> None:
        pcm_mono, _, _, _ = load_pcm16_audio(REPO_ROOT / rec["audio_path"])

        async def _dg1() -> dict[str, Any]:
            async with dg_sem:
                return await eval_deepgram_nova3(
                    pcm_mono,
                    BAKEOFF_KEYTERMS,
                    language="en-IN",
                    max_keyterms=100,
                    config_mode=cfg_mode,
                )

        async def _dg2() -> dict[str, Any]:
            async with dg_sem:
                return await eval_deepgram_nova3(
                    pcm_mono,
                    BAKEOFF_KEYTERMS,
                    language="multi",
                    max_keyterms=65,
                    config_mode=cfg_mode,
                )

        async def _dgf() -> dict[str, Any]:
            async with dg_sem:
                return await eval_deepgram_flux(pcm_mono, BAKEOFF_KEYTERMS, config_mode=cfg_mode)

        async def _gl() -> dict[str, Any]:
            async with gl_sem:
                return await eval_gladia_solaria1(
                    pcm_mono,
                    BAKEOFF_KEYTERMS,
                    custom_vocab=True,
                    code_switching=True,
                    config_mode=cfg_mode,
                )

        async def _aai() -> dict[str, Any]:
            async with aai_sem:
                return await eval_assemblyai(pcm_mono, BAKEOFF_KEYTERMS, config_mode=cfg_mode)

        fn_map = {
            "deepgram_nova3": _dg1,
            "deepgram_nova3_multi": _dg2,
            "deepgram_flux": _dgf,
            "gladia_solaria1": _gl,
            "assemblyai_universal": _aai,
        }
        rot = (rep + seg_idx) % len(base_candidates)
        ordered = base_candidates[rot:] + base_candidates[:rot]
        async with seg_sem:
            res_list = await asyncio.gather(*(fn_map[k]() for k in ordered))

        for k, api_res in zip(ordered, res_list, strict=True):
            entry_k = f"{k}:{cfg_mode}"
            scores = (
                score_record_trial(rec, api_res["transcript"]) if api_res["status"] == "OK" else {}
            )
            results_by_entry[entry_k].append(
                {
                    "rep": rep + 1,
                    "config_mode": cfg_mode,
                    "segment_id": rec["segment_id"],
                    "scenario": rec["scenario"],
                    "variant": rec["variant"],
                    "language_tag": rec["language_tag"],
                    "api": {k2: v2 for k2, v2 in api_res.items() if k2 != "words"},
                    "scores": scores,
                }
            )

    for cfg_mode in configs:
        for rep in range(reps):
            tasks = [
                asyncio.create_task(_run_entry(cfg_mode, rep, idx, rec))
                for idx, rec in enumerate(sample)
            ]
            for done_n, t in enumerate(asyncio.as_completed(tasks), start=1):
                await t
                if done_n % 30 == 0 or done_n == len(sample):
                    print(
                        f"PROGRESS config={cfg_mode} rep={rep + 1}/{reps} "
                        f"completed={done_n}/{len(sample)}",
                        flush=True,
                    )

    # Dedicated 2-speaker mono sequential, overlap, and native 2-channel stereo probes
    p_m = REPO_ROOT / "ml/eval/audio/synthetic_eng/data/scenario_01_daily_standup/turn_00_clean.wav"
    p_f = REPO_ROOT / "ml/eval/audio/synthetic_eng/data/scenario_01_daily_standup/turn_01_clean.wav"
    p_ovl = (
        REPO_ROOT / "ml/eval/audio/synthetic_eng/data/scenario_01_daily_standup/turn_00_overlap.wav"
    )
    p_m_2ch = (
        REPO_ROOT
        / "ml/eval/audio/synthetic_eng/data/scenario_01_daily_standup/turn_00_two_channel.wav"
    )
    p_f_2ch = (
        REPO_ROOT
        / "ml/eval/audio/synthetic_eng/data/scenario_01_daily_standup/turn_01_two_channel.wav"
    )

    pcm_m, _, _, _ = load_pcm16_audio(p_m)
    pcm_f, _, _, _ = load_pcm16_audio(p_f)
    pcm_2spk_seq = pcm_m + (b"\x00" * 16000) + pcm_f
    pcm_ovl, _, _, _ = (
        load_pcm16_audio(p_ovl) if p_ovl.exists() else (pcm_2spk_seq, pcm_2spk_seq, 1, 5.0)
    )
    _, pcm_s0, ch_n, _ = load_pcm16_audio(p_m_2ch)
    if p_f_2ch.exists():
        _, pcm_s1, _, _ = load_pcm16_audio(p_f_2ch)
        pcm_stereo = pcm_s0 + pcm_s1
    else:
        pcm_stereo = pcm_s0

    seq_probes = await asyncio.gather(
        eval_deepgram_nova3(pcm_2spk_seq, BAKEOFF_KEYTERMS, language="en-IN", max_keyterms=100),
        eval_deepgram_nova3(pcm_2spk_seq, BAKEOFF_KEYTERMS, language="multi", max_keyterms=65),
        eval_deepgram_flux(pcm_2spk_seq, BAKEOFF_KEYTERMS),
        eval_gladia_solaria1(pcm_2spk_seq, BAKEOFF_KEYTERMS),
        eval_assemblyai(pcm_2spk_seq, BAKEOFF_KEYTERMS),
    )
    if seq_probes[4].get("status") != "OK":
        seq_probes[4] = await eval_assemblyai(pcm_2spk_seq, BAKEOFF_KEYTERMS)
    diar_seq_results = dict(zip(base_candidates, seq_probes, strict=True))

    ovl_probes = await asyncio.gather(
        eval_deepgram_nova3(pcm_ovl, BAKEOFF_KEYTERMS, language="en-IN", max_keyterms=100),
        eval_deepgram_nova3(pcm_ovl, BAKEOFF_KEYTERMS, language="multi", max_keyterms=65),
        eval_deepgram_flux(pcm_ovl, BAKEOFF_KEYTERMS),
        eval_gladia_solaria1(pcm_ovl, BAKEOFF_KEYTERMS),
        eval_assemblyai(pcm_ovl, BAKEOFF_KEYTERMS),
    )
    if ovl_probes[4].get("status") != "OK":
        ovl_probes[4] = await eval_assemblyai(pcm_ovl, BAKEOFF_KEYTERMS)
    diar_ovl_results = dict(zip(base_candidates, ovl_probes, strict=True))

    stereo_probes = await asyncio.gather(
        eval_deepgram_nova3(pcm_stereo, BAKEOFF_KEYTERMS, language="en-IN", channels=ch_n),
        eval_gladia_solaria1(pcm_stereo, BAKEOFF_KEYTERMS, channels=ch_n),
    )
    two_channel_live = {
        "deepgram_nova3_2ch": {
            "status": stereo_probes[0]["status"],
            "channels_detected": stereo_probes[0].get("channels_detected"),
            "transcript": stereo_probes[0].get("transcript"),
        },
        "gladia_solaria1_2ch": {
            "status": stereo_probes[1]["status"],
            "channels_detected": stereo_probes[1].get("channels_detected"),
            "transcript": stereo_probes[1].get("transcript"),
        },
    }

    summaries: dict[str, Any] = {}
    for cfg_mode in configs:
        for c in base_candidates:
            ek = f"{c}:{cfg_mode}"
            summaries[ek] = summarize_candidate(
                ek,
                c,
                cfg_mode,
                results_by_entry[ek],
                diarize_probe=diar_seq_results[c],
                diarize_ovl_probe=diar_ovl_results[c],
                rtt_delta_ms=rtt_deltas.get(c),
            )

    # DATA ONLY printouts
    print("\n" + "=" * 124)
    print("NETWORK PROBE DATA (HOST -> CLOUD ENDPOINTS)")
    print(
        f"{'Endpoint':<22} {'DNS_P50':>8} {'TCP_P50':>8} {'TLS_P50':>8} "
        f"{'WS_Upg':>8} {'Ping_P50':>9} {'Ping_P95':>9}"
    )
    print("-" * 124)
    for np_item in net_probes.values():
        print(
            f"{np_item['name']:<22} {np_item['dns_p50_ms']:>8.1f} {np_item['tcp_p50_ms']:>8.1f} "
            f"{np_item['tls_p50_ms']:>8.1f} {np_item['ws_upgrade_ms']:>8.1f} "
            f"{np_item['ping_rtt_p50_ms']:>9.1f} {np_item['ping_rtt_p95_ms']:>9.1f}"
        )

    print("\n" + "=" * 138)
    print(f"STREAMING ACCURACY & HINGLISH MATRIX (N_eff={len(sample)}, reps={reps}, DATA ONLY)")
    print(
        f"{'Candidate [Config]':<35} {'Neff':>4} {'Nok':>4} {'Err':>3} "
        f"{'WER (Seg 95% CI)':<21} {'H_Tr_WER':>8} {'H_EngWER':>8} "
        f"{'NTER':>7} {'ID_Match':>8} {'EER':>7} {'H_Script'}"
    )
    print("-" * 138)
    for _ek, s in summaries.items():
        w = s["wer"]
        ws = f"{w['mean']:.3f}[{w['ci_lower']:.3f},{w['ci_upper']:.3f}]"
        print(
            f"{s['name']:<35} {s['n_eff']:>4} {s['n_ok']:>4} {s['errors']:>3} {ws:<21} "
            f"{s['hinglish_wer']['mean']:>8.4f} {s['hinglish_english_wer']['mean']:>8.4f} "
            f"{s['nter']:>7.4f} {s['id_exact_match']:>8.4f} {s['eer']:>7.4f} "
            f"{json.dumps(s['hinglish_script_dist'])}"
        )
    print(f"{'Sarvam Saaras v4/v3':<35} NOT RUN: {sarvam_reason}")
    oa_msg = "MISSING KEY (OPENAI_API_KEY)" if not oa_var else "NOT SCHEDULED"
    print(f"{'OpenAI gpt-live-transcribe':<35} NOT RUN: {oa_msg}")

    print("\n" + "=" * 138)
    print("STREAMING LATENCY (ALL 3 DEFINITIONS), STABILITY & 100H COST (DATA ONLY)")
    print(
        f"{'Candidate [Config]':<35} {'WordLag50':>9} {'StabP50':>8} {'StabP95':>8} "
        f"{'EOT_P50':>8} {'EOT_P95':>8} {'Eager50':>8} {'Retr%':>7} "
        f"{'100h_1ch$':>10} {'100h_Add$':>10} {'100h_2ch$':>10}"
    )
    print("-" * 138)
    for _ek, s in summaries.items():
        eag = f"{s['eager_eot_p50_ms']:.1f}" if s["eager_eot_p50_ms"] is not None else "N/A"
        c2 = f"{s['cost_100h_2ch_usd']:.1f}" if s["cost_100h_2ch_usd"] is not None else "UNVERIF"
        print(
            f"{s['name']:<35} {s['word_interim_p50_ms']:>9.1f} {s['stable_text_p50_ms']:>8.1f} "
            f"{s['stable_text_p95_ms']:>8.1f} {s['eot_p50_ms']:>8.1f} {s['eot_p95_ms']:>8.1f} "
            f"{eag:>8} {s['retraction_rate'] * 100.0:>6.2f}% "
            f"{s['cost_100h_mono_usd']:>10.1f} {s['cost_100h_with_addons_usd']:>10.1f} {c2:>10}"
        )

    print("\n" + "=" * 124)
    print("PER-VARIANT BREAKDOWN TABLE (WER / ID_Match / Stable_P50ms / EOT_P50ms)")
    for _ek, s in summaries.items():
        print(f"CANDIDATE={s['name']} | PARAMS={s['config_params']}")
        for v in ALL_VARIANTS:
            pv = s["per_variant"][v]
            w_ci = pv["wer"]
            print(
                f"  variant={v:<15} N_eff={pv['n_eff']:>2} N_ok={pv['n_ok']:>3} "
                f"WER={w_ci['mean']:.4f}[{w_ci['ci_lower']:.3f},{w_ci['ci_upper']:.3f}] "
                f"ID={pv['id_exact_match']:.4f} NTER={pv['nter']:.4f} "
                f"Word50={pv['word_interim_p50_ms']:.1f} Stable50={pv['stable_text_p50_ms']:.1f} "
                f"EOT50={pv['eot_p50_ms']:.1f}"
            )

    print("\n" + "=" * 124)
    print(f"GLADIA POST /v2/live DIARIZATION=TRUE PROBE (A1.5): {json.dumps(gladia_diar_http)}")
    print(f"STEREO 2-CH CHECK: {json.dumps(two_channel_live)}")
    for c in base_candidates:
        sq = diar_seq_results[c]
        ov = diar_ovl_results[c]
        sq_lbl = sq.get("speaker_labels") or ["UNSUPPORTED"]
        ov_lbl = ov.get("speaker_labels") or ["UNSUPPORTED"]
        print(
            f"{c:<26} seq_spk={sq.get('speakers_detected')} labels={sq_lbl} | "
            f"ovl_spk={ov.get('speakers_detected')} labels={ov_lbl}"
        )

    print("\n" + "=" * 124)
    print("WEIGHTED SCORECARD & HARD GATES (DATA ONLY)")
    for _ek, s in summaries.items():
        g = s["gates"]
        g_rtt_v = g["G_india_rtt_delta_le_150ms"]
        g_acc_v = g["G_acc_wer_le_0.15_iem_ge_0.85"]
        print(
            f"{s['name']:<35} G_Diar={str(g['G_live_diarization_mono']):<5} "
            f"G_RTT={str(g_rtt_v):<5} G_Vocab={str(g['G_custom_vocab']):<5} "
            f"G_ZDR={str(g['G_zdr_opt_out']):<5} G_Acc={str(g_acc_v):<5} "
            f"ALL_PASS={str(g['ALL_HARD_GATES_PASS']):<5} Score={s['weighted_score']:.2f}"
        )

    return {
        "mode": "streaming",
        "corpus": "ml/eval/audio/synthetic_eng",
        "sample_size": len(sample),
        "n_eff": len(sample),
        "scenarios_covered": sorted({r["scenario"] for r in sample}),
        "variants_covered": ALL_VARIANTS,
        "repetitions": reps,
        "configs": configs,
        "transliteration": {"library": TRANSLIT_LIBRARY, "license": TRANSLIT_LICENSE},
        "weights": DEFAULT_SCORECARD_WEIGHTS,
        "latency_definitions": {
            "word_interim_latency_ms": "first_partial_word_k_perf_ms - est_word_k_send_perf_ms",
            "time_to_stable_text_ms": "earliest_stable_word_k_perf_ms - est_word_k_send_perf_ms",
            "end_of_turn_latency_ms": (
                "vendor_eot_final_recv_perf_ms - last_speech_chunk_sent_perf_ms"
            ),
        },
        "network_probes": net_probes,
        "rtt_deltas_vs_deepgram_in_ms": rtt_deltas,
        "diarization_2spk_mono": {
            "gladia_post_diarize_check": gladia_diar_http,
            "two_channel_stereo_check": two_channel_live,
            "sequential": {
                k: {
                    "status": v.get("status"),
                    "error": v.get("error"),
                    "speakers_detected": v.get("speakers_detected"),
                    "speaker_labels": v.get("speaker_labels"),
                }
                for k, v in diar_seq_results.items()
            },
            "overlap": {
                k: {
                    "status": v.get("status"),
                    "error": v.get("error"),
                    "speakers_detected": v.get("speakers_detected"),
                    "speaker_labels": v.get("speaker_labels"),
                    "transcript": v.get("transcript"),
                }
                for k, v in diar_ovl_results.items()
            },
        },
        "summaries": summaries,
        "skipped_candidates": {
            "sarvam": {"status": sarvam_status, "reason": sarvam_reason},
            "openai": {"status": "NOT RUN", "reason": oa_msg},
        },
        "raw_trials": results_by_entry,
    }


async def run_bakeoff_async(args: argparse.Namespace) -> dict[str, Any]:
    load_dotenv(REPO_ROOT / ".env", override=False)
    if not MANIFEST_PATH.exists():
        print(f"ERROR: Manifest not found: {MANIFEST_PATH}")
        return {"error": "manifest_not_found"}

    with MANIFEST_PATH.open("r", encoding="utf-8") as f:
        all_records = [json.loads(line) for line in f if line.strip()]

    sample = select_stratified_corpus(all_records, sample_size=args.sample_size, seed=args.seed)

    if args.dry_run:
        res = await run_dry_run_checks(sample)
        out_path = Path(args.json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(res, indent=2), encoding="utf-8")
        print(f"JSON_ARTIFACT_WRITTEN: {out_path}")
        return res

    configs = [c.strip() for c in args.configs.split(",") if c.strip()]
    out_report: dict[str, Any] = {}

    if args.mode in ("streaming", "both"):
        stream_rep = await run_streaming_bakeoff(
            sample, reps=args.reps, configs=configs, concurrency=args.concurrency
        )
        out_report = stream_rep if args.mode == "streaming" else {"streaming": stream_rep}

    if args.mode in ("async", "both"):
        async_rep = await run_async_bakeoff(sample, concurrency=max(4, args.concurrency))
        if args.mode == "async":
            out_report = async_rep
        else:
            out_report["async"] = async_rep

    out_path = Path(args.json_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out_report, indent=2), encoding="utf-8")
    print(f"\nJSON_ARTIFACT_WRITTEN: {out_path}")
    return out_report


def main() -> int:
    parser = argparse.ArgumentParser(description="MeetMind AI ASR Bake-off Evaluation Runner")
    parser.add_argument(
        "--mode",
        choices=["streaming", "async", "both"],
        default="streaming",
        help="Evaluation mode: live streaming, async pre-recorded, or both (default: streaming)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=150,
        help="Stratified unique segment count (default: 150)",
    )
    parser.add_argument(
        "--reps", type=int, default=3, help="Interleaved streaming repetitions (default: 3)"
    )
    parser.add_argument(
        "--configs",
        type=str,
        default="default,aggressive",
        help="Comma-separated streaming configs (default: default,aggressive)",
    )
    parser.add_argument(
        "--concurrency", type=int, default=8, help="Concurrent segment workers (default: 8)"
    )
    parser.add_argument("--seed", type=int, default=42, help="Stratified sampling seed")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate keys, stratification, network & Gladia probe without streaming paid audio",
    )
    parser.add_argument(
        "--json-out",
        type=str,
        default="logs/bakeoff_streaming_q0d.json",
        help="Path to write JSON results artifact",
    )
    args = parser.parse_args()
    asyncio.run(run_bakeoff_async(args))
    return 0


if __name__ == "__main__":
    sys.exit(main())
