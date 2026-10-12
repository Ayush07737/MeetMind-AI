"""MeetMind AI - Redis non-persistence verification (WP6).

Enforces invariant: Raw audio never persists to disk.
"""

from __future__ import annotations

import logging
from typing import Any

from .config import GatewaySettings

logger = logging.getLogger(__name__)


async def assert_audio_redis_not_persistent(
    redis_client: Any,
    settings: GatewaySettings,
) -> None:
    """Startup check (WP6): CONFIG GET save is empty and appendonly is 'no'.

    Otherwise refuses to start when environment is not development.
    If CONFIG is disallowed (managed Redis), logs CRITICAL and requires
    AUDIO_REDIS_PERSISTENCE_ACKNOWLEDGED=true.
    """
    try:
        config = await redis_client.config_get("save", "appendonly")
    except Exception as exc:
        logger.critical(
            "CONFIG command disallowed on audio Redis: %s. "
            "Managed Redis requires AUDIO_REDIS_PERSISTENCE_ACKNOWLEDGED=true",
            exc,
        )
        if not settings.audio_redis_persistence_acknowledged and settings.app_env != "development":
            raise RuntimeError(
                "CONFIG command disallowed on audio Redis and "
                "AUDIO_REDIS_PERSISTENCE_ACKNOWLEDGED is not true"
            ) from exc
        return

    save_val = config.get("save", "")
    appendonly_val = str(config.get("appendonly", "no")).lower()

    # save is empty string or '""', and appendonly is 'no'
    is_non_persistent = (not save_val or save_val in ('""', "''")) and appendonly_val == "no"
    if not is_non_persistent:
        msg = (
            f"Audio Redis has persistence enabled: save={save_val!r}, appendonly={appendonly_val!r}"
        )
        logger.error(msg)
        if settings.app_env != "development":
            raise RuntimeError(
                f"Startup check failed (WP6): {msg}. Refusing to start in {settings.app_env}."
            )
