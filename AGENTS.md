# OpenArchive Agent Guide

## Purpose

OpenArchive is a local-first archive and memory layer for AI-era personal
data. The current V1 shape is:

- direct local startup on host
- SQLite as the default relational backend
- Qdrant as the default vector store, managed as a native sidecar
- filesystem-backed object storage by default
- MCP as the primary external interface
- asynchronous enrichment with a database-backed job queue

Legacy Oracle-first implementation pieces still exist. Treat current docs and
code together as the source of truth for shipped behavior, and preserve the
architectural constraints described below.

## Repo Priorities

- Preserve clean boundaries between transport, application logic, relational
  persistence, object storage, and inference.
- Prefer explicit provider wiring over generic plugin systems.
- Keep raw payload bytes out of the relational layer.
- Keep MCP as a transport adapter over application use cases, not the shape of
  the core.

## Composition Rules

- The composition root is [`src/main.rs`](src/main.rs) plus
  [`src/bootstrap.rs`](src/bootstrap.rs).
- Do not hardcode concrete providers in `main`.
- Add providers by updating config parsing, implementing the relevant traits,
  and extending the bootstrap factory.
- Keep provider-specific branching out of domain and request-handling code.

## Runtime And Concurrency

- Default to synchronous Rust.
- Do not introduce async runtimes or async-first libraries without a concrete,
  demonstrated need.
- Treat any future async requirement as a thin transport-edge concern, not a
  reason to reshape the application core.
- Keep request handlers short and bounded.
- Enrichment runs out of band through bounded worker threads and durable job
  state in the database.

## Error Handling

- Library code returns typed errors from [`src/error.rs`](src/error.rs).
- `anyhow` belongs at binary and composition boundaries, not inside library
  modules.
- Preserve source errors when wrapping failures.
- Do not collapse structured error variants into generic strings.

## Documentation And Comments

- Add comments only where they explain a boundary, invariant, or non-obvious
  behavior.
- Prefer rustdoc on public traits, modules, and types at architecture seams.
- Do not add comments that only restate the code.

## Verification

- For Rust code changes, run focused tests first, then broader ones if the
  touched area compiles cleanly.
- Prefer `cargo test --lib` and targeted binary or integration tests over
  speculative refactors without verification.
- For docs-only changes, do not run tests unless the docs describe changed
  commands or code paths.

## Important Files

- [`README.md`](README.md): project pitch, quick start, import flow, MCP usage
- [`docs/architecture.md`](docs/architecture.md): system shape and boundaries
- [`docs/domain-model.md`](docs/domain-model.md): canonical archive and derived
  object model
- [`docs/engineering-rules.md`](docs/engineering-rules.md): implementation
  constraints and coding rules
- [`src/bootstrap.rs`](src/bootstrap.rs): provider assembly
- [`src/config.rs`](src/config.rs): provider-shaped configuration
- [`src/storage/mod.rs`](src/storage/mod.rs): storage-facing traits and write
  path types
- [`src/import_service.rs`](src/import_service.rs): import pipeline entry point
- [`src/enrichment_worker.rs`](src/enrichment_worker.rs): durable worker loop
