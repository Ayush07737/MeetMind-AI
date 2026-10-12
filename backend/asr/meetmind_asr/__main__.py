"""MeetMind AI — ASR Pipeline Main Entrypoint.

Executed via `uv run python -m meetmind_asr`.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import sys

from meetmind_asr.config import get_settings
from meetmind_asr.service import AsrService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("meetmind_asr")


async def _run() -> None:
    settings = get_settings()
    service = AsrService(settings)

    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("Received termination signal; initiating graceful shutdown...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    if sys.platform != "win32":
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, _signal_handler)
    else:
        # On Windows, signal handlers can be limited; handle SIGINT
        signal.signal(signal.SIGINT, lambda *_: _signal_handler())
        signal.signal(signal.SIGTERM, lambda *_: _signal_handler())

    await service.start()
    try:
        await stop_event.wait()
    finally:
        await service.stop()


def main() -> None:
    with contextlib.suppress(KeyboardInterrupt, SystemExit):
        asyncio.run(_run())


if __name__ == "__main__":
    main()
