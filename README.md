# OpenArchive

A local-first archive and memory layer for anything you want to preserve,
enrich, and reuse. OpenArchive ingests AI conversations, documents, notes,
markdown, and other source material — enriches them with structured metadata —
and makes everything searchable and retrievable through a machine-first
interface.

## What It Does

**Import** — bring in AI chat exports, documents, markdown files, notes, or
any source material worth preserving. OpenArchive normalizes everything into a
canonical model while keeping the original source verbatim.

**Enrich** — a durable, staged pipeline extracts summaries, classifications,
memories, entities, and relationships from your archived artifacts. Enrichment
runs asynchronously and survives restarts.

**Search** — query your archive by title, transcript content, or derived
metadata. Results carry match context and ranking so downstream consumers know
what matched and why.

**Retrieve** — pull artifact detail or assembled context packs. A context pack
is a compact, structured bundle of the most relevant knowledge about an
artifact — summaries, classifications, memories, relationships, and the
evidence that supports them — shaped for machine consumption.

**MCP** — all retrieval is exposed through local MCP tools (`search_archive`,
`get_artifact`, `get_context_pack`), so any MCP-aware agent or editor can use
your archive directly.

## Quick Start

```bash
cp .env.example .env
make up
```

This starts Postgres and the OpenArchive container. The app runs migrations on
startup and serves on `http://localhost:3000`.

Import a ChatGPT export:

```bash
curl -X POST http://localhost:3000/imports/chatgpt \
  -H "Content-Type: application/json" \
  -d @path/to/conversations.json
```

The enrichment worker picks up imported artifacts automatically. Once
enrichment completes, search and retrieve through MCP or the HTTP surface.

Other useful commands:

```bash
make logs              # tail container logs
make down              # stop the stack
make up-ollama         # add local Ollama inference
```

## How It Works

```text
ingest → normalize → store → preprocess → extract → retrieve-context → reconcile → retrieve
```

1. **Import** accepts a source payload, parses it, copies the raw payload into
   object storage, writes canonical rows (artifacts, segments, participants),
   and enqueues enrichment
2. **Preprocess** inspects the artifact and decides extraction shape — whole,
   windowed, or topic-threaded
3. **Extract** runs semantic derivation and persists structured outputs
4. **Retrieve-context** queries the archive for related prior knowledge using
   extraction-produced intents
5. **Reconcile** merges extraction with retrieved context, writes final derived
   objects with evidence links
6. **Retrieval services** read the persisted state to serve search, artifact
   detail, and context-pack assembly

## Architecture

OpenArchive is synchronous Rust with explicit provider boundaries.

- **Transport-agnostic core** — application use cases in `src/app/` are
  shared by MCP, HTTP, and future transports
- **Provider seams** — relational storage, object storage, and inference each
  have trait boundaries with swappable implementations
- **Database-backed job queue** — durable, inspectable with plain SQL, no
  separate message broker required
- **Local-first defaults** — Postgres, filesystem object storage, optional
  Ollama. No cloud accounts needed to run

Current providers:

| Concern            | Default          | Alternatives                        |
|--------------------|------------------|-------------------------------------|
| Relational storage | Postgres         | Oracle (legacy)                     |
| Object storage     | Local filesystem | S3-compatible (planned)             |
| Inference          | Gemini, OpenAI, Anthropic, Grok | Ollama via OpenAI-compatible endpoint |

## Project Status

**Working end to end:**

- ChatGPT export import with raw payload preservation
- Four-stage enrichment pipeline (preprocess → extract → retrieve-context → reconcile)
- Archive search, artifact detail, and context-pack retrieval
- Local MCP tool surface over all retrieval use cases

**In progress:**

- Search ranking quality and full-text search
- Broader import coverage (Claude, Codex, documents, markdown, plain text)
- Remote MCP deployment
- First-run setup flow to replace manual `.env` configuration

## Documentation

| Doc | What it covers |
|-----|----------------|
| [Product Overview](docs/01-product-overview.md) | Vision, principles, target users |
| [Architecture](docs/02-architecture.md) | System shape, providers, execution model |
| [Roadmap](docs/05-roadmap.md) | Sequencing and exit criteria |
| [Brain Overview](docs/06-brain-overview.md) | Enrichment pipeline design |
| [Artifact Model](docs/07-artifact-model.md) | Canonical artifact structure |
| [Provenance Model](docs/08-provenance-model.md) | Source fidelity and traceability |
| [Derived Metadata](docs/09-derived-metadata-model.md) | Summaries, memories, classifications |
| [Context Packs](docs/10-context-pack-model.md) | Working-memory assembly for consumers |

## Contributing

OpenArchive is early-stage and open to contributors. The most interesting
problems right now:

- search ranking over heterogeneous content (titles, transcripts, derived metadata)
- import adapters for more source types (AI tools, documents, notes, markdown)
- context-pack assembly strategies
- local and remote deployment ergonomics

If you want to dig in: the codebase is Rust, the storage boundary is
trait-based, and `make up` gets a working stack running locally. Start with
the [Architecture doc](docs/02-architecture.md) for orientation.
