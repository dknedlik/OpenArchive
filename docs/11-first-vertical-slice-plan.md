# First Vertical Slice Plan

## Summary

Build one end-to-end brain slice that ingests a `ChatGPT export`, persists
canonical records in `Postgres`, stores copied raw payloads in a local
filesystem-backed object store, enriches the archive asynchronously, and
returns machine-usable retrieval through a local MCP server.

This slice should prove the core architecture, not the whole product.

It should validate:

- the artifact model for a real source
- segment-level provenance and derivation lineage
- persisted machine-readable metadata
- database-backed asynchronous enrichment
- MCP-first retrieval for machine consumers

## Current Status

As of the current local-first rewrite, slice one is partially complete.

What is in place now:

- provider-shaped runtime config and composition-root factory wiring
- Postgres as the default relational provider
- Oracle as a secondary relational provider kept current through the same
  schema contract
- provider-specific migrations for Oracle and Postgres
- local Docker Compose stack with `make up`
- copied raw payloads stored outside the relational database
- local filesystem object store as the default object-store provider
- S3-compatible remote object-store provider, validated against OCI Object
  Storage
- idempotent import write path with payload/object references
- durable database-backed enrichment job queue
- in-process enrichment worker skeleton
- artifact listing and ChatGPT import HTTP paths
- provider-parity integration coverage for import, job lifecycle, and derived
  metadata persistence

What is not done yet:

- local MCP server
- real enrichment processor boundary and first concrete summary /
  classification / memory pipeline
- artifact enrichment-status finalization semantics
- `conversation_resume` context-pack retrieval and not-ready / partial behavior
- large-conversation chunk-and-reduce path
- final slice end-to-end validation for the full retrieval flow

## Implementation Changes

### Slice behavior

- Support exactly one source type: `ChatGPT export`
- Treat imported conversations as `conversation` artifacts
- Limit slice-one parsing to text-oriented conversation content
- Normalize one export into:
  - artifact record
  - participant records
  - message segments
  - raw payload object-store reference
  - import and derivation lineage
- Persist a minimal but real metadata layer:
  - `Classification`
  - `Memory`
  - `Summary`
- Produce exactly one context-pack type: `conversation_resume`

### Public interfaces

- Make local MCP the primary external interface
- Keep the application core transport-agnostic so local MCP, remote MCP, CLI,
  and later HTTP can all reuse the same use cases
- Keep response shapes compact and machine-first

### Storage and service boundaries

- Define thin internal seams for:
  - relational persistence
  - object storage
  - inference/extraction
  - context-pack assembly
  - enrichment job store/dispatcher/executor
- Implement concrete slice-one defaults:
  - relational store: Postgres
  - object store: local filesystem volume
  - inference: stub or Ollama-backed local provider
- Do not build a generic plugin system
- Make adding a new provider straightforward through traits, config parsing,
  and factory wiring

### Execution and concurrency guardrails

- Slice one may run in one local Compose stack, but internal execution should
  still follow explicit pipeline-stage boundaries
- Keep long-running enrichment out of ingestion/query handlers
- Use fixed-size worker pools rather than ad hoc thread creation
- Treat the database as the durable source of truth for job lifecycle and
  retries
- Design the job contract so workers can run in-process, in another container,
  or on another machine later

### Raw payload storage

- Raw payload bytes should not live in the relational database
- OpenArchive copies ingested artifacts into OpenArchive-managed object storage
- Slice-one object storage is a local Docker volume behind an object-store
  interface
- S3-compatible remote storage already fits under the same object-store
  boundary

### Enrichment execution model

- Enrichment is asynchronous
- Import success means raw/canonical persistence succeeded
- Enrichment success is tracked separately per artifact
- Use a database-backed job model with:
  - durable payloads
  - claim/update/complete/fail lifecycle
  - retryable execution
  - explicit job and enrichment statuses
- Do not introduce a dedicated MQ unless the workload proves the need

### MCP scope

- Keep slice-one MCP narrow and useful
- Prioritize:
  - list artifacts
  - fetch artifact details
  - fetch `conversation_resume` context packs
  - basic search or retrieval if it lands cleanly
- Do not let MCP transport concerns leak into the application core

## Success Criteria

Current completed success criteria:

- `make up` brings the local system up with Docker Compose
- one real ChatGPT export can be ingested end to end
- raw payloads are copied into managed object storage outside the relational
  database
- canonical records and jobs are durable in Postgres
- Oracle remains viable as a secondary relational provider
- enrichment can run asynchronously without request-path blocking

Remaining slice-one exit criteria:

- a local MCP client can retrieve useful machine-facing results
- one real enrichment path persists summary, classification, and memory
  outputs with evidence
- `conversation_resume` retrieval works for ready, partial, and not-ready
  artifacts
