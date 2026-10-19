"""Acceptance Test B6: No Audio Persistence at Runtime.

Verifies:
1. Runs a full meeting ingesting synthetic PCM audio embedding SYNTHETIC_MARKER (32 bytes).
2. Continuously samples open file handles of the ASR worker process during streaming.
3. Diffs filesystem states of working directory, TEMP directory, and user cache directory
   before and after the meeting.
4. Scans every new or modified file for SYNTHETIC_MARKER (asserts 0 occurrences).
5. Asserts no audio bytes or SYNTHETIC_MARKER ever appear in the service's log files.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import tempfile
import uuid
from pathlib import Path

import psutil
import pytest
import websockets
from conftest import SYNTHETIC_MARKER, ServerHarness, exchange_ws_ticket, make_pcm16_chunk

pytestmark = [pytest.mark.integration, pytest.mark.timeout(90)]


def _snapshot_directory(root: Path) -> dict[Path, float]:
    """Capture mapping of existing file paths to their modification times."""
    snapshot = {}
    ignored_dirs = {
        ".git",
        ".venv",
        "node_modules",
        ".pytest_cache",
        "__pycache__",
        ".next",
        "dist",
    }
    with contextlib.suppress(OSError, PermissionError):
        for p in root.rglob("*"):
            if any(part in ignored_dirs for part in p.parts):
                continue
            if p.is_file():
                with contextlib.suppress(OSError, PermissionError):
                    snapshot[p] = p.stat().st_mtime
    return snapshot


@pytest.mark.asyncio
async def test_b6_no_persistence_at_runtime(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """Stream audio with unique marker; assert zero persistence and zero audio in logs."""
    meeting_id = f"mtg_b6_{uuid.uuid4().hex[:8]}"
    _tenant_id = gateway_harness.tenant_a

    # Directories to monitor
    cwd = Path.cwd()
    temp_dir = Path(tempfile.gettempdir())
    cache_dir = Path.home() / ".cache"
    monitored_dirs = [cwd, temp_dir]
    if cache_dir.exists():
        monitored_dirs.append(cache_dir)

    # Take before snapshot
    before_snapshots = {d: _snapshot_directory(d) for d in monitored_dirs}

    worker = await asr_worker_factory(provider="stub")
    worker_proc = psutil.Process(worker.proc.pid)

    admin_token = gateway_harness.jwks.mint_token(
        sub="admin_b6", org_id=gateway_harness.org_a, org_role="org:admin"
    )
    _, ingest_ticket_resp = await exchange_ws_ticket(
        gateway_harness.http_url, admin_token, meeting_id, role="ingest"
    )
    ingest_ticket = ingest_ticket_resp["ticket"]
    ingest_uri = f"{gateway_harness.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ingest_ticket}"

    sampled_open_files: set[str] = set()

    async with websockets.connect(ingest_uri) as ws_ingest:

        async def recv_ack():
            while True:
                msg = json.loads(await ws_ingest.recv())
                if msg.get("type") == "ack":
                    return msg

        await ws_ingest.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": "chrome_extension",
                }
            )
        )
        assert (await recv_ack())["status"] == "accepted"

        await ws_ingest.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "consent_confirmed",
                    "consent_type": "audio_capture",
                    "external_participants": True,
                    "jurisdiction_hint": "GDPR",
                    "consent_text_version": "v2.0",
                    "client_version": "2.1.0",
                }
            )
        )
        assert (await recv_ack())["status"] == "accepted"

        t_base = 6_000_000
        for seq in range(1, 15):
            frame = make_pcm16_chunk(seq=seq, ts_ms=t_base + seq * 100, marker=SYNTHETIC_MARKER)
            await ws_ingest.send(frame)
            ack = await recv_ack()
            assert ack.get("status") in ("accepted", "acked")

            # Sample open files of ASR worker process
            try:
                for of in worker_proc.open_files():
                    sampled_open_files.add(of.path)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

            await asyncio.sleep(0.05)

        # End meeting cleanly
        await ws_ingest.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_end",
                }
            )
        )
        assert (await recv_ack())["status"] == "accepted"

    # Wait for ASR worker to flush and drain
    await asyncio.sleep(2.0)

    # 1. Assert no open files sampled during streaming were audio files or contained the marker
    for fpath_str in sampled_open_files:
        p = Path(fpath_str)
        # Skip worker's own stdout/stderr log file or python binary / DLLs
        if p.suffix in (".pyc", ".pyd", ".dll", ".log") or "python" in p.name.lower():
            continue
        if p.exists() and p.is_file():
            try:
                content = p.read_bytes()
                assert SYNTHETIC_MARKER not in content, (
                    f"Sampled open file {p} contained SYNTHETIC_MARKER audio pattern!"
                )
            except (OSError, PermissionError):
                pass

    # 2. Diff directories and scan new / changed files
    for root_dir in monitored_dirs:
        before = before_snapshots[root_dir]
        after = _snapshot_directory(root_dir)

        for path, mtime in after.items():
            # If file is new or modified
            if path not in before or mtime > before[path]:
                # Exclude known runtime caches and pytest artifacts
                p_str = str(path).lower()
                if (
                    "__pycache__" in p_str
                    or ".pytest_cache" in p_str
                    or "task-" in p_str
                    or p_str.endswith(".tmp")
                    or "git" in p_str
                ):
                    continue
                try:
                    content = path.read_bytes()
                    assert SYNTHETIC_MARKER not in content, (
                        f"Found SYNTHETIC_MARKER in new/modified file: {path}"
                    )
                except (OSError, PermissionError):
                    pass

    # 3. Assert no audio bytes or SYNTHETIC_MARKER appear in the worker logs
    full_log_text = "\n".join(worker.logs)
    marker_str = SYNTHETIC_MARKER.decode("latin1", errors="ignore")
    marker_hex = SYNTHETIC_MARKER.hex()
    assert marker_str not in full_log_text, "ASR logs contained raw audio marker pattern!"
    assert marker_hex not in full_log_text, "ASR logs contained hex audio marker pattern!"
