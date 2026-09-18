# ADR 002 — Multi-Tenancy From Day One

**Status**: Accepted
**Date**: 2026-09-17
**Decision makers**: Project founders

## Context

MeetMind AI will eventually serve multiple tenants (organizations). The cheapest
time to add tenant isolation is *before the first line of business logic* — retro-
fitting multi-tenancy is effectively a rewrite of every database query, every cache
key, and every collection name.

## Decision

Every service that touches persistent state MUST route through the `TenantRouter`
class (`backend/security/src/tenant.py`) from its first commit.

### Isolation strategies per infrastructure:

| Infrastructure | Strategy | Rationale |
|----------------|----------|-----------|
| PostgreSQL | Schema-per-tenant (`tenant_{id}`) | Strong isolation, standard PostgreSQL feature |
| Neo4j CE | Namespace-prefixed labels | CE doesn't support multiple databases; this provides logical isolation while preserving the option to upgrade to Enterprise Edition for true database-per-tenant |
| Qdrant | Collection-name prefix (`{tenant}_{name}`) | Logical isolation at the collection level |
| Redis | Key prefix (`{tenant}:key`) | Standard namespace pattern |

### Day-one implementation

The `TenantRouter` starts with a single tenant (`"default"`) in a plain Python
dictionary. This is intentionally trivial — the point is the *seam*, not the
sophistication of the routing logic.

Adding tenant #2 means:
1. Adding one entry to `TENANT_CONFIGS` (or loading from a database)
2. Calling `router.register(new_config)`

It does NOT mean rewriting queries, changing connection strings, or auditing
every service for hardcoded values.

### Neo4j CE upgrade path

When/if we move to Neo4j Enterprise Edition:
1. Change `TenantRouter.resolve()` to return a database name instead of a namespace prefix
2. Update the Neo4j driver layer to use `session(database=...)` instead of label prefixing
3. No other service changes required — they all go through the router

## Consequences

- **Positive**: "Onboard tenant two" is a config change, not a rewrite
- **Positive**: Every service is testable with tenant isolation from day one
- **Negative**: Slight overhead of passing `tenant_id` through every call path
- **Binding**: No service may use a hardcoded connection string or skip the router
