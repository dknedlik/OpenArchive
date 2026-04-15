# OpenArchive Architecture

## Current Shape

OpenArchive is a synchronous Rust application with explicit boundaries between:

- transport adapters
- application services
- relational persistence
- object storage
- inference and embeddings

The composition root is:

- `src/main.rs`
- `src/bootstrap.rs`

Those files wire providers into application-facing services. Provider-specific
branching belongs there, not inside domain logic or request handlers.

## Mainline Deployment

The default deployment shape is local-first:

- direct host startup with `cargo run --bin open_archive -- serve`
- SQLite as the default relational backend
- Qdrant as the default vector store, managed as a colocated native sidecar
- local filesystem object storage for raw payloads
- hosted inference providers or stub inference
- MCP as the primary external interface

Optional provider paths:

- bundled Qdrant binary in installer artifacts, with release-download fallback
  for source builds
- Postgres as alternate relational backend
- S3-compatible object storage instead of local filesystem storage
- Oracle as a legacy relational provider that still exists in the codebase but
  is no longer the mainline contributor path

## Transport Surfaces

OpenArchive has two transport layers today.

### HTTP

HTTP is intentionally small and import-oriented:

- `POST /imports/chatgpt`
- `POST /imports/claude`
- `POST /imports/grok`
- `POST /imports/gemini`
- `GET /artifacts`

This surface exists for ingestion and simple listing, not as the main product
API.

### MCP

MCP is the primary consumer interface.

The current MCP surface includes:

- archive search
- artifact detail retrieval
- artifact context packs
- imported note metadata and note-link retrieval
- derived-object search
- artifact listing and timeline views
- related-object traversal
- writeback of memories, entities, links, and object status changes

MCP is a transport adapter over application services. The application core
should not become MCP-protocol-shaped.

## Storage Boundaries

OpenArchive keeps relational state and object storage separate.

### Relational store

The relational layer holds:

- imports and artifacts
- segments and participants
- derivation and evidence state
- durable enrichment jobs
- retrieval-facing read models
- derived objects and archive links

### Object storage

The object layer holds:

- raw import payload bytes
- other large objects that should not live in relational rows

The relational database stores references to managed objects, not host
filesystem paths and not raw payload blobs.

## Application Layer

The application layer lives under `src/app/`.

It owns the use cases that transports call into:

- imports
- artifact queries
- archive retrieval
- archive search
- artifact detail
- context pack assembly
- object search
- writeback

Transport adapters should stay thin. They should parse requests, call the app
layer, and serialize responses.

## Enrichment Pipeline

The enrichment pipeline is durable and database-backed.

Current job flow:

1. import writes canonical archive rows and enqueues work
2. structural link writes deterministic artifact links available at import time
3. extract produces summaries, classifications, memories, entities, and
   relationships
4. reconcile performs deterministic key matching, embedding-based candidate
   lookup, and narrow ambiguous-case adjudication
5. optional embedding jobs create vectors for derived-object search

Execution modes:

- `direct`: workers execute provider calls directly
- `batch`: workers execute the same extraction and reconcile stages through
  provider batch processors without blocking a thread on completion

The pipeline must remain:

- durable across restarts
- inspectable with ordinary SQL
- idempotent enough to recover safely
- bounded in thread count and request handling

## Provider Model

OpenArchive uses a bounded provider model rather than a generic plugin system.

Rules:

- providers implement concern-specific traits
- config selects explicit provider types
- bootstrap assembles the configured services
- domain and request-handling code do not branch on provider type

Current provider families:

- relational storage: SQLite, Postgres, Oracle
- vector storage: Qdrant, Postgres pgvector, disabled
- object storage: local filesystem, S3-compatible
- inference: stub, OpenAI, Gemini, Anthropic, Grok
- embeddings: disabled, stub, Gemini, OpenAI

## Architectural Constraints

The following constraints are intentional and should not be eroded casually:

- synchronous Rust by default
- no async runtime in the application core without demonstrated need
- raw payload bytes stay out of the relational layer
- provider-specific branching stays out of the app and domain layers
- MCP remains an adapter, not the shape of the core
- retrieval contracts are artifact- and object-aware, not only vector search
