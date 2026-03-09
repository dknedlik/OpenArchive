# Error Handling Strategy

`open_archive` uses typed errors in the library and `anyhow` only at executable boundaries.

## Rules

1. Library modules return typed errors.
   - Use `thiserror` enums from [`src/error.rs`](/Users/david/src/open_archive/src/error.rs).
   - Avoid `anyhow::Result` in `src/lib.rs` modules and their submodules.
2. Binaries are the `anyhow` boundary.
   - `src/main.rs` and any bin target may return `anyhow::Result`.
   - Add top-level context there when it improves operator-facing output.
3. Inner layers add structure, not logs.
   - Return typed variants with enough identifying fields to explain what failed.
   - Do not stringify and discard the original source error.
4. Outer layers decide presentation.
   - CLI code may format errors for humans.
   - Later service/API code can map typed errors to status codes or retry policy.

## Current Taxonomy

- `ConfigError`: environment and configuration loading failures.
- `DbError`: Oracle pool and connection failures plus DB-related env parsing.
- `MigrationsError`: migration discovery, validation, and schema update failures.
- `StorageError`: write-path failures for imports, artifacts, segments, jobs, commit, and rollback.
- `OpenArchiveError`: top-level library error enum that wraps the layer-specific enums.

## Practical Guidance

- Prefer a specific variant over a generic message string.
- Include stable identifiers such as `import_id`, `artifact_id`, or env key names in variants.
- Keep secrets out of error messages.
- `panic!`, `unwrap()`, and `expect()` are acceptable in tests and throwaway probes, not in library code.
- If a layer can recover locally, do so there. If it cannot, return a typed error and let the boundary decide how to report it.

## Why This Pattern

- The library stays explicit about failure modes.
- The CLI remains ergonomic.
- Future HTTP or worker entrypoints can reuse the same library errors without reverse-engineering opaque `anyhow` chains.
