# ADR 003 — Neon as the Relational Database for Dev & Prod

**Status**: Accepted
**Date**: 2026-09-21
**Decision makers**: Project founders

## Context

Running PostgreSQL locally via Docker Desktop introduces operational burden that
is premature for an early-stage project:

- Backups, patching, and uptime monitoring
- Volume management and disk space on developer machines
- Differences between local Postgres behavior and production

MeetMind AI's Postgres usage is intermittent (development workloads, not always-on
production traffic), making a serverless, scale-to-zero database a natural fit.

## Decision

### Use Neon (Managed Serverless Postgres) for Both Dev and Prod

- Remove PostgreSQL from `docker-compose.dev.yml`. Local infra becomes
  **Neo4j + Qdrant + Redis only**.
- Add `NEON_DATABASE_URL` to `.env.example`, pointing at a Neon "dev" branch.
- Production deployments use a "prod" branch created from the **same Neon project**,
  keeping schema migrations traceable across both environments.

### Dev/Prod Branching Strategy

```
Neon Project: meetmind-ai
+-- main (base branch)
+-- dev  --> local development connects here
+-- prod --> production deployment connects here
```

Migrations applied to `dev` can be promoted to `prod` via Neon's branch management,
ensuring schema consistency.

### Per-Tenant Physical Isolation (Future Carve-Out)

If a regulated customer (e.g., a bank, healthcare org) requires physical data
isolation, the answer is **not** "migrate the whole platform off Neon." Instead:

1. Provision a dedicated, self-hosted Postgres instance for that one tenant.
2. Add a `TenantConfig` entry with a custom `pg_connection_url` pointing to the
   dedicated instance.
3. Every other tenant stays on Neon.

This is the same per-customer carve-out pattern already designed into the
`TenantRouter` — a config change, not a platform rewrite.

### Cost Re-Evaluation Trigger

Revisit this decision when:

- Postgres usage becomes **genuinely always-on** (sustained production traffic)
  rather than intermittent development/demo workloads
- Monthly Neon cost exceeds the cost of a managed Postgres instance (e.g., RDS)
  by a meaningful margin (see §21's cost table for the comparison math)

At that point, the migration is straightforward: change `NEON_DATABASE_URL` to a
self-hosted/RDS connection string. The `TenantRouter` and schema-based isolation
remain unchanged.

## Consequences

- **Positive**: Zero local Postgres ops burden — no backups, patching, or volume management
- **Positive**: Scale-to-zero billing — no cost during idle development hours
- **Positive**: Branching for dev/prod keeps migrations traceable without separate instances
- **Positive**: SSL/TLS enforced by default (Neon requires it)
- **Negative**: Adds a network hop for local development (latency ~50-100ms)
- **Negative**: Requires a Neon account and internet connectivity for development
- **Binding**: All services connect via `NEON_DATABASE_URL` with `sslmode=require`.
  No service may use a local Postgres connection or skip TLS.
