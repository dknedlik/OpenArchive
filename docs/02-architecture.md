# OpenArchive Architecture

## Status

This is a working architecture sketch for the current product direction.

The initial local-first archive slice has been proved out. The current focus is
building a useful retrieval and MCP-facing product on top of that foundation.

## Candidate System Shape

OpenArchive is a modular archive and memory system with four major
responsibilities:

1. ingest source artifacts from external tools
2. normalize them into a canonical archive
3. derive searchable metadata and reusable memory
4. expose retrieval through MCP and other transport adapters

## Current Deployment Shape

The current default deployment should run locally with one command.

Target shape:

- Docker Compose brings up the system
- Postgres stores canonical metadata, provenance, and jobs
- a local filesystem-backed object store on Docker volumes stores raw payloads
  and other large objects
- ingestion/query and enrichment run as separate modules or processes
- Ollama is optional for local inference
- local MCP is the primary external interface

## Major Components

### Ingestion adapters

Per-source adapters translate exports or captures into a canonical envelope.

Examples:

- ChatGPT export adapter
- Claude export adapter
- Codex thread adapter
- copy-paste transcript adapter

### Canonical archive service

This service owns:

- artifact, segment, participant, and source models
- normalization and idempotency rules
- references to raw payloads stored in the object store
- import and enrichment job orchestration

### Object storage service

Binary and large text objects should live behind a dedicated object-store
interface from day one.

That includes:

- raw import payloads
- canonical source copies when needed
- larger derived artifacts if they outgrow practical row storage

Slice-one default:

- local filesystem-backed object store on a Docker volume

Near-term follow-on:

- S3-compatible object store provider

### Relational storage service

Canonical records, provenance, and jobs should live behind a relational-store
boundary.

Slice-one default:

- Postgres

Possible future providers:

- SQLite
- other SQL backends that implement the required traits

### Enrichment pipeline

Derived artifacts are separate from raw archive data.

Initial derived outputs:

- conversation summary
- classifications
- extracted memories

The enrichment pipeline should be durable, asynchronous, and location-flexible.

That means workers may run:

- in the same local Compose stack
- on another local machine
- in a remote deployment later

The current implemented stages are:

1. `artifact_preprocess`
2. `artifact_extract`
3. `artifact_retrieve_context`
4. `artifact_reconcile`

Current responsibilities:

- preprocess:
  - inspects the artifact shape
  - decides whether extraction should run whole-artifact, by conversation
    windows, or by topic-thread inputs
- extract:
  - runs the primary semantic derivation pass
  - can merge chunked outputs when multiple windows or threads are processed
  - persists one extraction result
- retrieve-context:
  - executes archive retrieval from extraction-produced retrieval intents
  - persists one retrieval result set
- reconcile:
  - combines extraction outputs with retrieved context
  - persists reconciliation decisions
  - writes the final derivation attempt with active derived objects and
    evidence links

Artifact-level `enrichment_status` is then derived from durable job and output
state in the relational store.

### Inference boundary

Inference should follow the same boundary discipline as storage.

The application layer should depend on feature-specific, provider-agnostic
interfaces such as:

- `ConversationSummarizer`
- `MemoryExtractor`
- `ClassificationExtractor`

Concrete inference providers live below that boundary.

Likely providers:

- local Ollama-backed inference
- stub provider for development
- later, remote hosted model providers

### Retrieval surface

MCP is the primary external interface for the product.

That does not mean the application core should be shaped like the MCP
protocol. Instead:

- use-case services live in the middle
- local MCP and remote MCP become transport adapters over those use cases
- CLI and later HTTP can reuse the same application services

The next useful operators are not “dump every artifact.” They are:

- archive search
- artifact detail retrieval
- artifact-context assembly
- later, broader cross-artifact context-pack retrieval

The current local MCP tool surface now implements the first three directly:

- `search_archive`
- `get_artifact`
- `get_context_pack`

## Provider Model

OpenArchive should use a bounded provider model, not a generic plugin system.

Rules:

- providers implement concern-specific traits
- config selects explicit provider types
- factory functions assemble the configured services
- the domain layer does not branch on provider type

The important concern boundaries are:

- relational store
- object store
- inference provider

Near-term hosted inference direction:

- native Gemini provider
- native OpenAI provider
- native Anthropic provider
- native Grok provider
- OpenAI-compatible endpoints as a secondary mode layered on the OpenAI
  provider shape, not the main abstraction

It is acceptable for a higher-level data service to aggregate repositories from
multiple providers.

## Execution Model Direction

The preferred execution model is pipeline-first rather than
request-lifecycle-first.

Stages:

1. intake and validation
2. canonical normalization and persistence
3. preprocess
4. extract
5. retrieve-context
6. reconcile and derivation persistence
7. retrieval and context assembly

These stages may start in one local stack, but they should behave as decoupled
pipeline stages rather than one long synchronous request path.

## Job Queue Direction

The current preferred queue model is database-backed job coordination rather
than a dedicated MQ.

That is a good fit for this system because jobs are:

- durable
- coarse-grained
- provenance-sensitive
- low enough throughput that SQL visibility matters more than queue fan-out

The job system should support:

- lease/claim semantics
- retries and backoff
- crash recovery through lease expiry
- idempotent handlers
- job inspection with ordinary SQL

## Constraints And Heuristics

- preserve source fidelity while still normalizing aggressively
- keep ingestion-specific logic outside the core model
- keep extracted memory traceable back to exact source messages
- keep provider seams explicit without overbuilding extensibility
- make local startup and contribution materially easier than the earlier
  OCI/Oracle path

## Future User Setup Shape

The current developer path is still Docker Compose plus env-driven config.
That is acceptable for slice-one development, but it should not remain the
primary onboarding experience once the core providers are stable.

The intended user-facing direction is:

- first-run setup flow inside the app
- choose inference provider and enter API credentials
- choose a Postgres target or accept an easy local default
- allow any Postgres-compatible connection string instead of forcing a bundled
  local stack

Docker Compose should remain a strong development and local bootstrap path, but
the product should eventually lead with guided configuration rather than manual
`.env` editing.
