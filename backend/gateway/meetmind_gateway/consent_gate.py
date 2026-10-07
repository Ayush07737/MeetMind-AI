"""MeetMind AI - Consent verification gate.

Enforces the hard compliance invariant: no audio or transcript frame may be
processed before a ``consent_confirmed`` control frame has been received and
logged. This module integrates with the §15 consent service.
"""

from __future__ import annotations

import logging

from meetmind_security.audit_log import write_audit_event
from meetmind_security.consent import check_consent, record_consent

logger = logging.getLogger(__name__)


class ConsentGate:
    """Verifies and records meeting consent.

    The gate starts closed. It opens when ``confirm_consent`` is called
    successfully. ``is_open`` returns True only after consent is confirmed.
    """

    def __init__(
        self,
        meeting_id: str,
        tenant_id: str,
        user_id: str,
    ) -> None:
        self.meeting_id = meeting_id
        self.tenant_id = tenant_id
        self.user_id = user_id
        self._consented = False

    @property
    def is_open(self) -> bool:
        """Return True if consent has been confirmed for this meeting."""
        return self._consented

    async def confirm_consent(self, consent_type: str = "audio_capture") -> None:
        """Record consent via the §15 consent service.

        This writes both a consent record and an audit event.

        Raises:
            Exception: If the consent service fails.
        """
        await record_consent(
            tenant_id=self.tenant_id,
            user_id=self.user_id,
            meeting_id=self.meeting_id,
            consent_type=consent_type,
        )
        self._consented = True
        logger.info(
            "Consent confirmed for meeting=%s user=%s type=%s",
            self.meeting_id,
            self.user_id,
            consent_type,
        )

    async def check_existing_consent(self, consent_type: str = "audio_capture") -> bool:
        """Check if consent was already granted (e.g. on reconnect)."""
        has_consent = await check_consent(
            tenant_id=self.tenant_id,
            user_id=self.user_id,
            meeting_id=self.meeting_id,
            consent_type=consent_type,
        )
        if has_consent:
            self._consented = True
        return has_consent

    async def reject_frame(self, frame_type: str) -> None:
        """Log a rejected frame due to missing consent."""
        await write_audit_event(
            tenant_id=self.tenant_id,
            user_id=self.user_id,
            event_type="frame_rejected_no_consent",
            payload={
                "meeting_id": self.meeting_id,
                "frame_type": frame_type,
                "reason": "consent_not_confirmed",
            },
        )
        logger.warning(
            "Rejected %s frame for meeting=%s: consent not confirmed",
            frame_type,
            self.meeting_id,
        )
