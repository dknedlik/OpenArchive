# OpenArchive Engineering Rules

## Runtime And Concurrency

- Default to synchronous Rust.
- Do not introduce an async runtime or async-first architecture without a
  concrete demonstrated need.
- If async is needed later, keep it at the transport edge rather than
  reshaping the application core.
- Keep request handlers short and bounded.
- Run enrichment out of band using bounded worker threads and durable job
  state in the database.

## Composition And Providers

- The composition root is `src/main.rs` plus `src/bootstrap.rs`.
- Do not hardcode concrete providers in `main`.
- Add providers through config parsing, trait implementations, and bootstrap
  wiring.
- Keep provider-specific branching out of domain logic and request handlers.
- Prefer explicit provider seams over generic plugin machinery.

## Storage Discipline

- Keep raw payload bytes out of the relational layer.
- Store large payloads behind the object-store boundary.
- Keep relational state focused on canonical metadata, provenance, jobs, and
  retrieval-facing records.
- Treat MCP and HTTP as adapters over application services, not as reasons to
  distort the storage model.

## Errors

- Library modules return typed errors from `src/error.rs`.
- `anyhow` belongs at binaries and composition boundaries, not inside library
  modules.
- Preserve source errors when wrapping failures.
- Do not collapse structured error variants into opaque strings.
- Include stable identifiers like artifact IDs, import IDs, or config keys when
  they improve diagnosis.

## Logging

- Use logs for operator visibility, not as a substitute for structured error
  handling.
- Prefer concise lifecycle logs around startup, claims, retries, failures, and
  shutdown.
- Avoid noisy per-record logging unless it is directly useful for debugging.
- Do not leak secrets or raw credentials into logs.

## Documentation And Comments

- Add comments only where they explain a boundary, invariant, or non-obvious
  behavior.
- Prefer rustdoc on public traits, modules, and architecture seams.
- Do not add comments that only restate the code.
- Keep the active docs set small and authoritative. If a document is no longer
  constraining implementation or explaining the current system, delete it or
  merge it.

## Verification

- For Rust changes, run focused tests first and broader tests second.
- Prefer `cargo test --lib` and targeted tests over speculative large refactors.
- Before calling Rust changes ready, run `make verify`. Keep that target aligned
  with `.github/workflows/ci.yml` so local verification matches CI.
- The local repo installs a `pre-commit` hook through
  `core.hooksPath=.githooks` so commits fail fast on `make verify`. Keep the
  hook thin and keep the real checks in `make verify`.
- For docs-only changes, do not run tests unless the docs changed commands or
  runtime assumptions that should be verified.

## Probes And Experiments

- Use probes and experiments to drive and inspect production behavior, not to
  create parallel implementations of core logic.
- Shared extraction, reconciliation, ranking, and routing behavior must live in
  production modules first. Probes should call those modules rather than
  reimplementing them.
- Probe-specific code may add visibility, sampling, reporting, fixtures, or
  ergonomic entrypoints, but it must not become the only place where important
  business logic exists.
- If an experimental rule or policy looks useful, move it into the shared
  runtime path before relying on probe results from it.
- Avoid benchmark-driven overfitting. Small regression sets are guardrails for
  catching breakage, not targets to optimize against in isolation.
- Prefer one production path with observable knobs over separate "test mode" and
  "real mode" behaviors that must be kept in sync.

## Product Pressure That Should Not Distort The Core

The following are real product needs, but they should be built on top of the
existing architectural shape rather than forcing a rewrite of the core:

- broader import coverage
- human-facing UI
- review and pruning workflows
- correction, supersession, and merge workflows
- improved retrieval quality
