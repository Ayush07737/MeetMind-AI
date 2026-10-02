# MeetMind AI

> AI-powered meeting intelligence platform that captures, understands, and acts on meeting conversations in real time.

## Quick Start

### Prerequisites

| Tool                                                              | Version | Purpose                                             |
| ----------------------------------------------------------------- | ------- | --------------------------------------------------- |
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | >= 27.x | Infrastructure containers (WSL2 backend on Windows) |
| [Python](https://www.python.org/)                                 | >= 3.12 | Backend services                                    |
| [uv](https://docs.astral.sh/uv/)                                  | >= 0.12 | Python package management                           |
| [Node.js](https://nodejs.org/)                                    | >= 22.x | Extension & frontend                                |
| [pnpm](https://pnpm.io/)                                          | >= 12.x | JS/TS package management with workspace support     |
| [Neon account](https://neon.tech/)                                | —       | Managed serverless Postgres (dev & prod)            |

### Setup

```bash
# 1. Clone and enter the repository
git clone <repo-url> meetmind-ai
cd meetmind-ai

# 2. Create environment file
cp .env.example .env
# Edit .env:
#   - Set NEON_DATABASE_URL to your Neon dev branch connection string
#   - Set Clerk keys

# 3. Start local infrastructure (Redis, Neo4j, Qdrant — Postgres is on Neon)
docker compose -f docker-compose.dev.yml up -d

# 4. Install Python dependencies
uv sync

# 5. Install JS/TS dependencies
pnpm install

# 6. Run tests
uv run pytest          # Python test suites
pnpm test              # TypeScript test suites
```

## Architecture

MeetMind AI is a monorepo organized by domain concern:

```
meetmind-ai/
+-- backend/                  # Python services (FastAPI + async)
¦   +-- gateway/              # §1  Ingestion Gateway & WebSocket Core
¦   +-- asr/                  # §2  Speech-to-Text Streaming Pipeline
¦   +-- classifier/           # §6  Trigger Classification Model
¦   +-- orchestration/        # §7  Agent Orchestration Layer (LangGraph)
¦   +-- decision-genome/      # §8  Decision Genome (Neo4j)
¦   +-- semantic-memory/      # §9  Semantic Memory Layer (Qdrant)
¦   +-- agents/               # §10-12  Meeting Agents (Pre/Live/Post)
¦   +-- trust-gate/           # §13 Action & Trust Gate
¦   +-- integrations/         # §14 Integration Fabric (Composio)
¦   +-- security/             # §15 Security & Compliance Layer
¦   +-- web-enrichment/       # §17 Web Enrichment Tool
+-- extension/                # §3  Chrome/Chromium Extension
+-- frontend/                 # §16 Side Panel / Dashboard
+-- packages/shared-schemas/  # Pydantic + Zod contracts shared across services
+-- ml/                       # §6  Training pipeline
+-- docs/                     # Architecture docs & ADRs
```

## Tech Stack

| Layer                   | Technology                                                       |
| ----------------------- | ---------------------------------------------------------------- |
| **Backend**             | Python 3.12+, FastAPI, WebSockets, asyncpg                       |
| **Agent Orchestration** | LangGraph                                                        |
| **Graph Database**      | Neo4j Community Edition                                          |
| **Vector Database**     | Qdrant                                                           |
| **Relational Database** | Neon (managed serverless Postgres)                               |
| **Cache**               | Redis 7                                                          |
| **Authentication**      | Clerk                                                            |
| **Frontend**            | TypeScript, Vite                                                 |
| **Extension**           | Chrome Extension (Manifest V3)                                   |
| **Python Tooling**      | uv (packages), ruff (lint/format), pytest (tests)                |
| **JS/TS Tooling**       | pnpm (packages), ESLint + Prettier (lint/format), Vitest (tests) |

## Standards

All development conventions are documented in [`AGENTS.md`](AGENTS.md). Key rules:

- **Package managers**: `uv` for Python, `pnpm` for JS/TS — no exceptions.
- **Test frameworks**: `pytest` + `pytest-asyncio` for Python, `vitest` for TypeScript — locked in.
- **Lint/format**: `ruff` for Python, `eslint` + `prettier` for TypeScript — locked in.
- **Database**: Neon (managed serverless Postgres) for dev & prod. Local Docker for Neo4j, Qdrant, Redis only.
- **Multi-tenancy**: Every service uses the tenant routing layer from day one.
- **Security**: No audio processed without logged consent. Append-only audit log.

## License

Proprietary — All rights reserved.
