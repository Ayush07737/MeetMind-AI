"""MeetMind AI — Shared Python Schemas.

Pydantic models that mirror the Zod schemas in src/index.ts.
These are the canonical Python-side contracts shared across backend services.
"""

from .events import AuditEventSchema, ConsentRecordSchema, TenantId

__all__ = [
    "AuditEventSchema",
    "ConsentRecordSchema",
    "TenantId",
]