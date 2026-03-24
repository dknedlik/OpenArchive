# OpenArchive

OpenArchive is a local-first archive and memory layer for personal data in the
AI era. It ingests source material — conversation exports, text documents, and
other personal artifacts — preserves the raw payloads, normalizes them into a
canonical model, runs asynchronous enrichment, and exposes the result through a
machine-first interface.

The goal is not just to make old files searchable. It is to build a grounded
personal archive that accumulates durable knowledge over time: summaries,
classifications, entities, relationships, memories, evidence links, and
user- or agent-authored writebacks. Source material goes in; structured,
retrievable understanding comes out and grows with every import.

## What OpenArchive Does

- Import source material through format-specific adapters (conversation
  exports today, with text documents and other formats following).
- Preserve raw payload bytes outside the relational database.
- Normalize source material into a canonical artifact and segment model.
- Run a durable enrichment pipeline that extracts, reconciles, and embeds
  structured knowledge from stored artifacts.
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
- Durable asynchronous enrichment with extract, retrieve-context, reconcile,
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
- `OA_INFERENCE_PROVIDER=stub`
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
- `stub`

Example:

```bash
OA_INFERENCE_PROVIDER=openai
OA_INFERENCE_MODE=direct
OA_OPENAI_API_KEY=...
```

If you want embedding-backed object search, also configure embeddings:

```bash
OA_EMBEDDING_PROVIDER=openai
OA_OPENAI_API_KEY=...
OA_OPENAI_EMBEDDING_MODEL=text-embedding-3-small
OA_OPENAI_EMBEDDING_DIMENSIONS=1536
```

The OpenAI-shaped provider can also target OpenAI-compatible endpoints through
`OA_OPENAI_BASE_URL`.

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
- `get_context_pack`: compact artifact context pack for downstream agents
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
