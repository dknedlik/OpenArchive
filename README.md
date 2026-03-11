# OpenArchive

OpenArchive is a local-first archive and memory layer for AI-era personal data.

The current project direction is intentionally biased toward an open source,
easy-to-run slice one:

- one-command local startup with `make up`
- containerized services with Docker Compose
- Postgres for canonical relational state and job coordination
- local filesystem-backed object storage on a Docker volume
- MCP as the primary external interface
- optional local inference via Ollama for enrichment

The goal is simple: ingest source artifacts, preserve them faithfully, enrich
them asynchronously, and make them queryable by machines with minimal setup.

## Current Working Idea

- Build a user-owned archive and memory substrate rather than a new chat client
- Start with AI chats as the primary early source, but support broader artifact
  types over time
- Preserve raw source material while extracting structured, machine-usable
  meaning
- Keep the application core transport-agnostic so local MCP, remote MCP, CLI,
  and later HTTP can all sit on the same use cases
- Keep provider seams explicit:
  - relational store provider
  - object store provider
  - inference provider

## Slice 1 Direction

Slice 1 is now local-first rather than OCI-first.

The working target is:

- `docker compose` starts the stack locally
- ingestion, query, and enrichment run as separate modules/processes
- raw payloads are copied into OpenArchive-managed storage rather than
  referencing arbitrary user filesystem paths
- large artifact bytes live outside the relational database from day one
- the enrichment queue remains database-backed rather than requiring a
  standalone MQ

Planned concrete slice-one defaults:

- relational store: Postgres
- object store: local filesystem volume
- inference: stub or Ollama-backed local provider
- external interface: local MCP server

## Architecture Notes

OpenArchive is not aiming for a generic plugin system.

The intended extension model is narrower:

- contributors add a provider module for one concern
- the module implements the relevant traits
- config parsing is updated to recognize the provider
- factory wiring is updated to construct it

That should make new providers straightforward to add without pushing the
application core into provider-specific branching or protocol-shaped logic.

## Current Repo Contents

- [docs/01-product-overview.md](/Users/david/src/open_archive/docs/01-product-overview.md)
- [docs/02-architecture.md](/Users/david/src/open_archive/docs/02-architecture.md)
- [docs/03-data-model.md](/Users/david/src/open_archive/docs/03-data-model.md)
- [docs/05-roadmap.md](/Users/david/src/open_archive/docs/05-roadmap.md)
- [docs/06-brain-overview.md](/Users/david/src/open_archive/docs/06-brain-overview.md)
- [docs/07-artifact-model.md](/Users/david/src/open_archive/docs/07-artifact-model.md)
- [docs/08-provenance-model.md](/Users/david/src/open_archive/docs/08-provenance-model.md)
- [docs/09-derived-metadata-model.md](/Users/david/src/open_archive/docs/09-derived-metadata-model.md)
- [docs/10-context-pack-model.md](/Users/david/src/open_archive/docs/10-context-pack-model.md)
- [docs/11-first-vertical-slice-plan.md](/Users/david/src/open_archive/docs/11-first-vertical-slice-plan.md)
- [docs/12-first-slice-schema.md](/Users/david/src/open_archive/docs/12-first-slice-schema.md)
- [src/main.rs](/Users/david/src/open_archive/src/main.rs)

## Historical Notes

Earlier Oracle ADB and OCI exploration documents remain in the repo as
historical references. They should not be treated as the default slice-one
path.
