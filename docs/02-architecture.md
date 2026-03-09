# OpenArchive Architecture

## Status

This is a candidate architecture sketch, not a committed design.

It is also intentionally brain-layer biased at this stage. Concerns outside the
brain layer, such as polished mobile UX, final auth flows, and non-core access
surface details, may be stubbed or underdesigned on purpose for now.

## Candidate System Shape

OpenArchive may become a cloud-first service with four major responsibilities:

1. ingest source conversations from multiple tools
2. normalize them into a canonical archive
3. derive searchable metadata and reusable memories
4. expose retrieval through an API and later remote MCP

## Major Components

### Ingestion adapters

Per-source adapters translate exports or captures into a canonical envelope.

Examples:

- ChatGPT export adapter
- Claude export adapter
- Codex thread adapter
- copy-paste transcript adapter

### Canonical archive service

This service would likely own:

- conversation, message, participant, and source models
- raw transcript storage references
- normalization and idempotency rules
- extraction job orchestration

### Extraction pipeline

Derived artifacts are separate from raw archive data.

Initial derived outputs:

- conversation summary
- tags
- participants and tool metadata
- extracted memories
- embeddings or other search features later

### Retrieval API

A likely initial API surface:

- archive import
- conversation lookup
- full-text and metadata search
- summary retrieval
- memory retrieval

### Later integration surfaces

- remote MCP server
- webhook-driven ingestion
- browser capture helpers

These access-surface ideas are intentionally secondary to the current brain
layer work and should not be mistaken for detailed interface designs yet.

## Candidate Deployment Direction

- Rust service on OCI Compute or equivalent OCI-hosted runtime
- Oracle Autonomous Database as a possible primary system of record
- New database schema in the existing ADB instance as the default starting
  assumption
- OCI Vault and the existing wallet pattern as reusable ingredients

## Execution Model Direction

The current preferred execution model is pipeline-first rather than
request-lifecycle-first.

- Keep user-facing request handlers short and bounded
- Move slow enrichment and hydration work behind durable job boundaries
- Allow the system to start as one process while preserving seams that let
  stages move into separate workers later
- Prefer synchronous Rust and bounded worker pools by default where the
  underlying client libraries are already blocking

This implies a staged flow such as:

1. intake and validation
2. canonical normalization and persistence
3. enrichment and derivation
4. retrieval and context assembly

These stages may reside in one application initially, but they should behave as
decoupled pipeline stages rather than one long end-to-end request path.

### Concurrency Principles

- Use fixed-size worker pools rather than ad hoc thread spawning
- Keep separate worker classes for different load profiles when useful
- treat CPU-heavy normalization separately from latency-heavy enrichment
- Bound Oracle connection usage explicitly with small pools sized to the target
  machine
- Treat the database as the durable source of truth for job lifecycle and stage
  progress
- Use in-process channels only as an optimization, not as the sole source of
  work state

### Async Versus Sync Guidance

Async is not a requirement for every layer.

- Synchronous HTTP and storage layers are acceptable if request concurrency is
  modest and long-running work is pushed out of band
- Blocking Oracle access is an acceptable implementation choice if worker and
  connection counts are bounded
- Async should be introduced only where it provides clear value, especially for
  services that spend large amounts of time waiting on remote providers

The architecture should optimize first for clear stage boundaries, backpressure,
and operability. Concurrency model details can then evolve per stage without
rewriting the full system shape.

## Constraints And Heuristics

- preserve source fidelity while still normalizing aggressively
- avoid lock-in to any one AI tool or model provider
- keep ingestion-specific logic outside the core model
- keep extracted memory traceable back to exact source messages

## Why The Strongest Idea Is Not A Chat Client

Replacing the front-end chat experience would expand scope into:

- real-time conversation UX
- per-provider message streaming
- auth/session management per tool
- tool invocation UX

That does not currently look like the leverage point. Durable memory and reuse
across tools looks more promising.
