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
ingest -> normalize -> store -> preprocess -> extract -> retrieve_context -> reconcile -> retrieve
```

The current MVP build phase is intentionally local-first:

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

OpenArchive is pre-alpha and has moved past architecture proof into MVP
development.

What exists today:

- a Rust codebase with the first import, storage, and enrichment boundaries
- ChatGPT-export-oriented parsing and canonical archive work
- durable multistage enrichment with direct and batch inference modes
- provider support for Gemini, OpenAI, Anthropic, and Grok
- local-first provider wiring around Postgres and filesystem-backed object storage
- the old Oracle-first implementation path, now being replaced

What is being built now:

- search, artifact detail retrieval, and context-pack assembly over the enriched archive
- local MCP as the primary machine-facing MVP product surface
- richer source imports beyond the initial ChatGPT export path
- remote MCP over the same application-layer use cases
- product-oriented local and remote deployment flows

What is now working end to end:

- import persists raw payloads, canonical artifacts, participants, and segments
- the enrichment pipeline runs as durable staged jobs:
  - `artifact_preprocess`
  - `artifact_extract`
  - `artifact_retrieve_context`
  - `artifact_reconcile`
- app-layer retrieval services expose:
  - archive search
  - artifact detail
  - artifact context packs
- local MCP exposes those retrieval use cases through:
  - `search_archive`
  - `get_artifact`
  - `get_context_pack`
- real persisted archive data can now be searched and retrieved through the MCP
  surface

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

For object storage, `local_fs` is the local development path. The `s3`
provider is intended for real remote buckets, configured through env vars.

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

## Current Data Flow

The current implemented pipeline is:

1. import accepts a source payload, parses it, copies the raw payload into
   object storage, writes canonical rows, and enqueues `artifact_preprocess`
2. preprocess inspects the artifact and decides extraction shape, including
   whole-artifact, windowed, or topic-thread-oriented extraction
3. extract runs the main semantic derivation pass and persists one durable
   extraction result
4. retrieve-context runs archive retrieval from extraction-produced intents and
   persists one retrieval result set
5. reconcile combines extraction outputs with retrieved context, persists
   reconciliation decisions, and writes the final derivation attempt with
   active derived objects and evidence links
6. retrieval services read the persisted artifact, segment, derived-object, and
   evidence state to support search, artifact detail, and artifact-context
   assembly

The artifact-level `enrichment_status` is intended to be derived from the
durable job and output state rather than inferred by retrieval at read time.

## Open Problems

The most interesting open problems are now MVP product and retrieval oriented:

- useful archive search and ranking over titles, transcript segments, and derived metadata
- stable artifact detail and artifact-context retrieval contracts
- local and remote MCP surfaces over the application use cases
- richer imports across more AI tools and artifact types
- optional Ollama-backed local inference provider
- remote deployment and first-run setup flows that are not repo-first

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
historical references. They are no longer the default product path.
