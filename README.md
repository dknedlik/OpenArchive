# OpenArchive

OpenArchive is a local-first archive and memory layer for personal data in the
AI era. It ingests source material — conversation exports, text documents, and
other personal artifacts — preserves the raw payloads, normalizes them into a
canonical model, runs asynchronous enrichment, and exposes the result through a
machine-first interface.

The goal is not just to make old files searchable. It is to build a grounded
personal archive that accumulates durable knowledge over time: summaries,
classifications, entities, relationships, memories, archive links, and
user- or agent-authored writebacks. Source material goes in; structured,
retrievable understanding comes out and grows with every import.

## What OpenArchive Does

- Import source material through format-specific adapters (conversation
  exports today, with text documents and other formats following).
- Preserve raw payload bytes outside the relational database.
- Normalize source material into a canonical artifact and segment model.
- Run a durable enrichment pipeline that structurally links, extracts,
  reconciles, and embeds structured knowledge from stored artifacts.
- Expose retrieval and writeback through MCP, with a small HTTP surface for
  imports and basic listing.

## How This Relates to RAG

OpenArchive overlaps with retrieval-augmented generation but sits at a
different level. A typical RAG system chunks documents, embeds the chunks,
and returns the nearest neighbors at query time. That works well for
prompt-stuffing, but it treats the retrieved material as disposable context.

OpenArchive treats ingested material as a long-lived archive:

- Raw source payloads are preserved for audit and reprocessing.
- Canonical archive rows are separated from object storage.
- Derived objects (summaries, memories, entities, relationships) are typed,
  evidence-linked, and individually addressable.
- Retrieval is shaped around artifacts, context packs, and archive objects —
  not only chunk similarity.
- MCP clients can write back into the archive, creating new memories,
  entities, and links that become part of the knowledge base.

Embedding-backed search is available as one retrieval path, but it is a
component of the system rather than the whole story.

## V1 Status

OpenArchive V1 is a working local-first MVP.

What exists today:

- Postgres as the mainline relational backend
- Local filesystem object storage by default, with S3-compatible storage as an
  optional provider
- Import handlers for ChatGPT, Claude, Grok, and Gemini export JSON
- Durable asynchronous enrichment with structural link, extract, reconcile,
  and optional embedding jobs
- Archive retrieval through MCP
- MCP writeback for memories, entities, links, and object status updates
- Derived-object search with lexical ranking and optional embedding-backed
  semantic ranking

What is still missing or intentionally early:

- more import types beyond the current export handlers
- a polished human-facing UI
- review, pruning, merge, and correction workflows for bad or stale data
- stronger ranking and retrieval quality
- easier first-run setup and product-grade operator ergonomics

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Rust and Cargo if you want to run the MCP server locally outside Docker

### 1. Start the local stack

Copy the example environment and start the stack:

```bash
cp .env.example .env
make up
```

The checked-in `.env.example` is set up for a local smoke-test path:

- `OA_RELATIONAL_STORE=postgres`
- `OA_OBJECT_STORE=local_fs`
- `OA_MODEL_PROVIDER=stub`
- `OA_INFERENCE_MODE=direct`
- `OA_EMBEDDING_PROVIDER=disabled`

That path is useful for validating imports, storage, MCP wiring, and the job
pipeline. It is not intended to represent real enrichment quality.

Sanity-check the running service:

```bash
curl http://localhost:3000/artifacts
```

Useful operator commands:

```bash
make logs
make down
```

### 2. Switch to a real inference provider

For real enrichment, edit `.env` and set a hosted provider plus credentials.
Supported inference providers today:

- `openai`
- `gemini`
- `anthropic`
- `grok`
- `oci`
- `stub`

Example:

```bash
OA_MODEL_PROVIDER=gemini
OA_INFERENCE_MODE=direct
OA_HEAVY_MODEL=gemini-3-flash-preview
OA_FAST_MODEL=gemini-2.5-flash-lite
OA_GEMINI_API_KEY=...
```

Recommended starting point for real imports:

- primary provider: `gemini`
- heavy model: `gemini-3-flash-preview`
- fast model: `gemini-2.5-flash-lite`
- reconciliation defaults to the fast model unless explicitly overridden in code
- embeddings: `gemini-embedding-001` at `3072` dimensions

Current working guidance from import probes:

- `gemini` is the current best extraction default after the redesign probes.
  The strongest pair so far is a two-stage extraction flow:
  `gemini-3-flash-preview` for Stage 1 judgment and `gemini-2.5-flash-lite`
  for Stage 2 schema formatting.
- `openai` remains supported, but it is no longer the recommended default for
  extraction in this pipeline.
- `grok` is the strongest low-cost alternative, but still trails `openai` on
  final output shape.
- `anthropic` is supported, but is not the current recommended default because
  cost, verbosity, and rate limits were not justified by a clear quality win in
  this pipeline.

If you are importing a lot of data and do not care about interactive latency,
prefer batch execution where the provider supports it. Batch mode is the right
tradeoff for imports because throughput and cost matter more than immediate
responses.

OpenArchive now exposes one common inference model surface:

- `OA_MODEL_PROVIDER`
- `OA_HEAVY_MODEL`
- `OA_FAST_MODEL`

The code decides where to use the heavy or fast model internally. Today the
heavy model is used for the judgment-heavy inference paths, while the fast
model is used for formatting and similar latency-sensitive follow-up work.

If you want embedding-backed object search, also configure embeddings:

```bash
OA_EMBEDDING_PROVIDER=gemini
OA_EMBEDDING_MODEL=gemini-embedding-001
OA_EMBEDDING_DIMENSIONS=3072
OA_GEMINI_API_KEY=...
```

Gemini embeddings use the native Gemini API. OpenAI embeddings still support
OpenAI-compatible endpoints through `OA_OPENAI_BASE_URL`.

Embedding dimensions are effectively schema-level configuration. The first
database setup fixes the vector column width. Changing
`OA_EMBEDDING_DIMENSIONS` later requires a schema migration and re-embedding
existing derived objects. Startup now fails fast if the configured dimensions
do not match the Postgres schema.

### Choosing extraction and reconcile models

If you want to try other provider/model combinations, evaluate them against the
final stored archive, not just one raw extraction response.

For extraction models, favor:

- strong artifact summaries that match the document or conversation's real
  purpose
- durable memories and entities with low metadata or scaffolding noise
- reliable handling of long conversations and dense technical documents
- compact structured output without splitting one conclusion into many adjacent
  micro-facts

For reconcile models, favor:

- strict JSON/schema compliance
- conservative merge behavior
- correct target typing for memories, entities, and relationships
- willingness to create new objects rather than forcing weak merges

Use a representative evaluation set, not just one artifact type. A good sample
should include:

- long conversations
- short practical conversations
- technical reference documents
- procedural/setup documents
- dashboards or hub notes
- short definition notes

When comparing models, rank them by archive quality first:

1. Did the final stored objects retain the source knowledge?
2. Did the archive avoid noisy, duplicated, or weakly grounded objects?
3. Did relationships and related-object retrieval become more useful?
4. Only after that should you compare latency and cost.

## Import Data

Imports happen over HTTP. The current import surface is intentionally small:

- `POST /imports/chatgpt`
- `POST /imports/claude`
- `POST /imports/grok`
- `POST /imports/gemini`
- `GET /artifacts`

Example import commands:

```bash
curl -X POST http://localhost:3000/imports/chatgpt \
  -H "Content-Type: application/json" \
  -d @path/to/conversations.json
```

```bash
curl -X POST http://localhost:3000/imports/claude \
  -H "Content-Type: application/json" \
  -d @path/to/claude-export.json
```

```bash
curl -X POST http://localhost:3000/imports/grok \
  -H "Content-Type: application/json" \
  -d @path/to/prod-grok-backend.json
```

```bash
curl -X POST http://localhost:3000/imports/gemini \
  -H "Content-Type: application/json" \
  -d @path/to/MyActivity.json
```

Imports persist canonical rows immediately and enqueue enrichment work.
Enrichment completes asynchronously in the background.

## Use Through MCP

MCP is the primary external interface for day-to-day use.

### Build the MCP server

```bash
cargo build --bin mcp
```

This produces a local stdio server at:

```text
target/debug/mcp
```

The MCP binary loads `.env` relative to the project root, so a compiled binary
under `target/debug/` can usually reuse the repo's `.env` file without extra
wrapper scripts.

### Connect Claude Desktop

Add an MCP server entry that points Claude Desktop at the compiled binary:

```json
{
  "mcpServers": {
    "openarchive": {
      "command": "/absolute/path/to/open_archive/target/debug/mcp"
    }
  }
}
```

After saving the config, restart Claude Desktop. The server will expose the
OpenArchive tool set over stdio.

### MCP Tools

The current MCP surface includes:

- `search_archive`: ranked archive search across artifact titles, derived
  objects, and segment excerpts
- `get_artifact`: artifact detail plus bounded segment windows
- `get_context_pack`: compact artifact context pack for downstream agents,
  including imported note metadata when present
- `get_note_metadata`: imported note metadata and note-link retrieval for
  Obsidian and markdown-style artifacts
- `search_objects`: derived-object search with optional embedding-backed
  semantic ranking
- `list_artifacts`: filtered artifact browsing
- `get_timeline`: chronological artifact browsing with optional filters
- `get_related`: one-hop related-object traversal
- `store_memory`: persist a user- or agent-authored memory
- `store_entity`: persist a user- or agent-authored entity
- `link_objects`: create a relationship between derived objects
- `update_object`: mark an object as superseded or rejected

## Core Docs

The active documentation set is intentionally small:

- [docs/architecture.md](docs/architecture.md)
- [docs/domain-model.md](docs/domain-model.md)
- [docs/engineering-rules.md](docs/engineering-rules.md)
- [docs/roadmap.md](docs/roadmap.md)

## Contributing

The most useful work after V1 is productization rather than proving the basic
system shape:

- broader import coverage
- better retrieval and ranking quality
- human review and correction workflows
- better product UX and setup ergonomics

Start with [docs/architecture.md](docs/architecture.md) for system shape and
[docs/engineering-rules.md](docs/engineering-rules.md) for implementation
constraints.
