# ADR 001 — Monorepo Structure & Toolchain

**Status**: Accepted
**Date**: 2026-09-17
**Decision makers**: Project founders

## Context

MeetMind AI consists of ~20 components spanning Python backend services, a Chrome
extension, a frontend dashboard, shared schemas, and ML training pipelines. We need
a repository structure and toolchain that:

1. Allows shared code (schemas, types) to be imported by multiple consumers
2. Keeps dependency management deterministic across machines and CI
3. Locks down tool choices to prevent drift between developers/AI agents

## Decision

### Monorepo

All components live in a single repository (`meetmind-ai/`). This ensures:

- Atomic cross-component changes (e.g., updating a shared schema)
- Single CI pipeline
- Unified code review

### Package Managers

| Scope      | Tool   | Why                                                                        |
| ---------- | ------ | -------------------------------------------------------------------------- |
| Python     | `uv`   | 10–100x faster than pip, deterministic lockfiles, native workspace support |
| TypeScript | `pnpm` | Workspace linking for `@meetmind-ai/shared-schemas`, strict node_modules   |

### Testing

| Language   | Framework                   | Why                                                     |
| ---------- | --------------------------- | ------------------------------------------------------- |
| Python     | `pytest` + `pytest-asyncio` | Native async support matches FastAPI/WebSocket codebase |
| TypeScript | `vitest`                    | Pairs with Vite build tooling, fast, ESM-native         |

### Linting/Formatting

| Language   | Tools                 | Why                                                 |
| ---------- | --------------------- | --------------------------------------------------- |
| Python     | `ruff`                | Single tool for lint + format, same ecosystem as uv |
| TypeScript | `eslint` + `prettier` | Industry standard, well-supported                   |

## Consequences

- **Positive**: Consistency across all agents and developers. No drift.
- **Positive**: Shared schemas are always in sync.
- **Negative**: Monorepo requires discipline — large PRs, CI must be fast.
- **Binding**: No alternative test framework, package manager, or linter may be
  introduced without a new ADR superseding this one.
