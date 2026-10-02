# MeetMind AI — Agent & Developer Standards

> **This file is binding on every human developer and every AI coding agent working in this repository.**
> If a standard below conflicts with an agent's default behavior, this file wins.

## Project Identity

- **Name**: MeetMind AI
- Use this name in every file, package name, code comment, and README.
- No abbreviations, no alternate names.

## Monorepo Layout

```
meetmind-ai/
+-- backend/           # All Python services (one sub-directory per service)
+-- extension/         # Chrome/Chromium Extension (TypeScript)
+-- frontend/          # Dashboard / Side Panel (TypeScript)
+-- packages/          # Shared packages (shared-schemas used by extension + frontend)
+-- ml/                # ML training pipelines
+-- docs/              # Architecture docs, ADRs, security docs
+-- docker-compose.dev.yml
+-- .env.example
+-- (this file)
```

The `desktop/` directory (§4 Native Desktop Capture Adapter) is **not scaffolded** until that
section is actively started. Do not create it preemptively.

## Package Managers — LOCKED

| Scope                                                          | Manager | Reason                                         |
| -------------------------------------------------------------- | ------- | ---------------------------------------------- |
| Python (all `backend/` services, `ml/`)                        | `uv`    | Speed, lockfile determinism, workspace support |
| TypeScript/JavaScript (`extension/`, `frontend/`, `packages/`) | `pnpm`  | Workspace linking for `shared-schemas`         |

**Do not** introduce `pip`, `poetry`, `npm`, or `yarn` anywhere in this repository.

## Testing Frameworks — LOCKED

| Language   | Framework                   | Notes                                                          |
| ---------- | --------------------------- | -------------------------------------------------------------- |
| Python     | `pytest` + `pytest-asyncio` | `asyncio_mode = "auto"` — matches async FastAPI/WebSocket code |
| TypeScript | `vitest`                    | Pairs with Vite-based extension and frontend builds            |

**Do not** introduce `unittest`, `jest`, `mocha`, or any other test framework.

## Lint & Format — LOCKED

| Language   | Tools                  |
| ---------- | ---------------------- |
| Python     | `ruff` (lint + format) |
| TypeScript | `eslint` + `prettier`  |

## Authentication

- **Provider**: Clerk (not Auth0, not custom JWT).
- The backend gateway validates Clerk session JWTs via Clerk's backend SDK.
- There is **no** separate `JWT_SECRET` — Clerk owns token issuance and signing.
- Migrate to Auth0/SAML only when a specific enterprise deal requires SSO that Clerk cannot cover.

## Multi-Tenancy — MANDATORY FROM DAY ONE

Every service that will eventually need tenant isolation **must** route through the
`TenantRouter` in `backend/security/meetmind_security/tenant.py` from its very first commit.

- Neon (Postgres): schema-based isolation (`tenant_{id}` schemas)
- Neo4j: namespace-prefixed labels (CE doesn't support multi-database)
- Qdrant: collection-name prefixes (`{tenant_id}_collection_name`)
- Redis: key prefixes (`{tenant_id}:key`)

**Never** use a hardcoded connection string. Always resolve through `TenantRouter`.

## Security Invariants

1. **No audio frame processed without a prior logged `consent_confirmed` event.**
2. The `audit_events` table is **append-only** — no UPDATE, no DELETE.
3. All consent state changes generate audit events.
4. These primitives exist in `backend/security/` and must be operational before any
   other component's development begins.

## Code Style

- Python line length: 100 characters
- TypeScript: follow Prettier defaults (printWidth 100)
- Every acceptance criterion from the build spec must be implemented as an automated test,
  not just verified manually once.
- Preserve all existing comments and docstrings unrelated to your changes.

## Database — Neon (Managed Serverless Postgres)

- **DEV/PROD**: Both environments use Neon — not local Docker Postgres.
- `docker-compose.dev.yml` starts **Neo4j + Qdrant + Redis only**.
- The `NEON_DATABASE_URL` env var points at a Neon "dev" branch for local work.
- For production, create a separate "prod" branch from the same Neon project
  (keeps schema migrations traceable across both).
- If a regulated customer requires physical data isolation, carve out a dedicated
  self-hosted Postgres instance for that one tenant — don't migrate the whole platform
  off Neon. (See ADR 003 for the full rationale.)

## Environment

- Local development: Docker Desktop with WSL2 backend on Windows.
- `docker-compose.dev.yml` brings up Redis 7, Neo4j CE, and Qdrant.
- Neon handles all PostgreSQL needs (dev branch for local, prod branch for deploy).
- All env vars documented in `.env.example`.
