"""MeetMind AI — Security & Compliance Layer (§15).

This package provides the foundational security primitives that every other
service in MeetMind AI depends on:

- **TenantRouter**: Multi-tenancy routing (resolve tenant → infra connections)
- **AuditLog**: Append-only audit event logging
- **ConsentService**: Consent record management with audit trail
- **DB**: Async PostgreSQL connection pool management
"""

from . import audit_log, consent, db, migrations, tenant
from . import audit_log as audit

__all__ = ["audit", "audit_log", "consent", "db", "migrations", "tenant"]
