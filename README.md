# OpenArchive

OpenArchive is a local-first archive and memory layer for AI-era personal
data.

The pitch is simple: a lot of your best thinking is trapped inside AI chat
sessions, exports, and tool-specific histories that are hard to query, reuse,
or hand back to another machine. OpenArchive is the substrate for preserving
that work, enriching it asynchronously, and making it retrievable through a
machine-first interface.

## Mental Model

OpenArchive is a pipeline:

```text
ingest -> normalize -> store -> enrich -> retrieve
```

Slice one is intentionally local-first:

- `make up` should bring up the stack with Docker Compose
- Postgres stores canonical relational state and job coordination
- a local filesystem-backed object store keeps raw payloads out of the database
- enrichment runs asynchronously and may use a stub provider or local Ollama
- MCP is the primary external interface

## Why This Project Exists

Most AI tooling treats conversation history as an application detail.
OpenArchive treats it as durable user data.

The project is aimed at building a user-owned archive and memory substrate
rather than another chat client. It starts with AI chats, but the longer-term
shape is broader: preserve source material, attach provenance, extract useful
structure, and return compact machine-usable context for future tools and
agents.

## Status

OpenArchive is pre-alpha and still building slice one.

What exists today:

- a Rust codebase with the first import, storage, and enrichment boundaries
- ChatGPT-export-oriented parsing and canonical archive work
- slice-one brain-layer docs and schema planning
- the old Oracle-first implementation path, now being replaced

What is being built now:

- provider-based service assembly
- Postgres as the default relational backend
- local filesystem-backed object storage
- local MCP as the primary transport
- a one-command local developer stack

## Running Locally

The default local stack is Postgres plus the OpenArchive app container.

```bash
cp .env.example .env
make up
```

That starts:

- Postgres for canonical relational state and job coordination
- OpenArchive with filesystem-backed object storage on a Docker volume

The app runs migrations on startup and serves on `http://localhost:3000`.
OpenArchive reads process env once into typed config in
`src/config.rs`; `.env` is just the local development source for those
settings.

Useful commands:

```bash
make logs
make down
make up-ollama
make up-oracle-db
make test-postgres-integration
make test-oracle-integration
```

`make up-ollama` and `make up-oracle-db` currently add those containers to the
local stack for provider and inference development. The default app path still
runs against Postgres unless you explicitly reconfigure it.

Live provider tests follow the same pattern for both databases: start the local
container, then run the matching `make test-...-integration` target. Both
providers require `OA_ALLOW_SCHEMA_RESET=1` because the test harness recreates
its schema/database.

## Architecture Direction

The current architecture is intentionally opinionated:

- transport-agnostic application core
- explicit provider seams for relational storage, object storage, and inference
- database-backed durable job queue rather than a separate MQ
- local-first defaults without blocking future remote MCP deployment

OpenArchive uses synchronous Rust throughout. This is intentional. The
workload here — ingestion, job polling, enrichment coordination, and MCP
serving — does not benefit meaningfully from async complexity. Synchronous
Rust is easier to understand, easier to contribute to, and fits the system's
explicit worker-pool model. If a transport edge eventually requires an async
library, it should remain a thin boundary layer rather than reshape the
application core.

This is not intended to become a generic plugin system. The target is simpler:
if someone wants to add a provider, they should be able to implement the
relevant traits, wire config parsing, update the factory, and move on.

## Open Problems

The most interesting open problems are architectural, not cosmetic:

- Postgres-backed repository implementations
- filesystem-backed object store abstraction
- provider/factory assembly from config
- local MCP server over the application use cases
- ChatGPT export ingestion hardening
- durable enrichment worker behavior and job leasing
- optional Ollama-backed inference provider

If you like Rust, AI systems, storage boundaries, retrieval, provenance, or
personal data infrastructure, this is the class of problem the repo is trying
to solve.

## Docs

Project direction:

- [docs/01-product-overview.md](docs/01-product-overview.md)
- [docs/02-architecture.md](docs/02-architecture.md)
- [docs/05-roadmap.md](docs/05-roadmap.md)
- [docs/06-brain-overview.md](docs/06-brain-overview.md)
- [docs/11-first-vertical-slice-plan.md](docs/11-first-vertical-slice-plan.md)
- [docs/12-first-slice-schema.md](docs/12-first-slice-schema.md)

Brain-layer design:

- [docs/07-artifact-model.md](docs/07-artifact-model.md)
- [docs/08-provenance-model.md](docs/08-provenance-model.md)
- [docs/09-derived-metadata-model.md](docs/09-derived-metadata-model.md)
- [docs/10-context-pack-model.md](docs/10-context-pack-model.md)

## Historical Notes

Earlier Oracle ADB and OCI exploration documents remain in the repo as
historical references. They are no longer the default slice-one path.
