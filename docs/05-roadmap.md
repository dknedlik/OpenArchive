# OpenArchive Roadmap

This is a sequencing sketch, not a committed delivery plan.

## Phase 0: Reset The Foundation

- rewrite the project around a local-first, open source slice one
- remove Oracle/OCI assumptions from the mainline path
- define provider boundaries for relational storage, object storage, and
  inference
- make MCP the primary external interface

Exit criteria:

- docs and code describe the same architecture
- the default local path does not require Oracle-specific tooling or accounts

## Phase 1: Local Archive Slice

- provide a `make up` path backed by Docker Compose
- run Postgres, core services, and optional Ollama locally
- ingest one concrete source format end to end
- persist canonical rows plus copied raw payloads
- expose artifact retrieval and context-oriented queries through local MCP
- run enrichment asynchronously through a database-backed job queue

Exit criteria:

- one real export can be imported end to end
- imported artifact data can be queried through MCP
- import is idempotent by source identity or content hash
- the local stack is usable with modest setup

## Phase 2: Stronger Retrieval And Search

- add metadata-aware retrieval and full-text search
- improve context-pack generation
- add richer query flows for machine consumers

Exit criteria:

- stored artifacts are retrievable without manually browsing raw source
  material
- the MCP surface is useful as a daily machine-facing archive interface

## Phase 3: Better Enrichment

- support local inference through Ollama as a first-class provider
- improve stored summaries, classifications, and memories
- allow enrichment workers to run outside the main local stack when needed
- add native hosted providers in a deliberate sequence:
  Gemini first, then OpenAI, then Anthropic, then Grok
- treat OpenAI-compatible endpoints as a follow-on mode built on the OpenAI
  provider shape rather than the primary inference abstraction

Exit criteria:

- a user with local inference capacity can run meaningful enrichment locally
- enrichment remains durable even when workers are restarted or moved

## Phase 4: Remote Deployment Shapes

- add S3-compatible object storage
- support remote MCP deployment for personal cloud-hosted use
- keep local and remote deployment shapes aligned around the same application
  core
- start shifting the primary user experience away from repo-local `.env` and
  Docker-first setup toward a first-run configuration flow
- let users choose inference provider and point at any Postgres-compatible
  connection string, while still offering an easy local Postgres path for
  users who want the app to bootstrap the default stack

Exit criteria:

- OpenArchive can run locally or remotely without re-architecting the core
- the remote MCP path is structurally natural rather than bolted on
