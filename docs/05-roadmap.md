# OpenArchive Roadmap

This is a sequencing sketch, not a committed delivery plan.

## Completed Foundation Work

- complete the local-first architecture proof around import, storage, and enrichment
- remove Oracle/OCI assumptions from the mainline path
- define provider boundaries for relational storage, object storage, and
  inference
- make MCP the primary external interface

Completed outcomes:

- docs and code describe the same architecture
- the default local path does not require Oracle-specific tooling or accounts
- one real export can be imported end to end
- raw payloads are copied into managed object storage
- canonical records and enrichment jobs are durable
- asynchronous enrichment is real, provider-backed, and restart-tolerant

## Current MVP Priorities

### Retrieval And Search

- split retrieval read models from the import write path
- add archive search with ranked match-aware hits
- add artifact detail retrieval as a stable read use case
- assemble compact artifact-context packs for downstream agents
- implement the MVP MCP tool surface over those application-layer services

Exit criteria:

- stored artifacts are retrievable without manually browsing raw source
  material
- search results carry enough match context to be useful without follow-up
  guesswork
- the MCP surface is useful as a daily machine-facing archive interface

### Broader Imports And Better Brain Quality

- support local inference through Ollama as a first-class provider
- improve stored summaries, classifications, and memories
- allow enrichment workers to run outside the main local stack when needed
- expand import coverage across more AI tools and artifact types

Exit criteria:

- a user with local inference capacity can run meaningful enrichment locally
- enrichment remains durable even when workers are restarted or moved

### Deployment And Productization

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
