# SQLite / Qdrant Probe Notes

These probe results informed the current local-first default runtime:
`sqlite` for relational state, `qdrant` for vectors, and `local_fs` for raw
payload bytes.

Probe examples below assume a local Qdrant instance is already running. The
current `serve` and `mcp` binaries now install or launch a managed native
Qdrant sidecar automatically; the older Docker-based probe command is no longer
the mainline setup path.

These probes are meant to answer the two main local-profile risks before
building the full provider layer:

- can SQLite handle the multi-process job-claim pattern credibly enough for a
  local-first runtime
- can Qdrant handle filtered semantic search on the current archive corpus

## Binaries

- `cargo run --bin probe_sqlite_queue -- run ...`
- `cargo run --bin probe_qdrant_search -- ...`

## SQLite Queue Probe

The SQLite probe creates a temporary database, enables WAL mode, seeds a probe
job table, then spawns multiple worker **processes**. Each worker uses
`BEGIN IMMEDIATE` to:

1. claim one eligible job
2. commit immediately
3. simulate processing
4. complete the job in another short transaction

This is intentionally testing the multi-process shape that matters for
`serve` + `mcp`, not only multi-thread behavior.

Example:

```bash
cargo run --bin probe_sqlite_queue -- run --jobs 2000 --workers 4 --process-ms 5
```

Observed local result on 2026-04-13:

- `jobs_seeded`: `2000`
- `workers`: `4`
- `elapsed_ms`: `16385`
- `completed_jobs`: `2000`
- `total_attempt_count`: `2000`
- `total_busy_retries`: `0`
- `claim_latency_ms`: `avg 0.60 | p50 0.56 | p95 1.02 | max 7.50`
- `complete_latency_ms`: `avg 0.24 | p50 0.20 | p95 0.39 | max 5.20`

Interpretation:

- the short-transaction claim pattern looks viable
- the probe did not show obvious write-lock thrash at this worker count
- this does **not** prove parity with Postgres under heavy ingest, but it does
  validate the basic local-profile queue approach

## Qdrant Search Probe

The Qdrant probe can run with synthetic vectors or load the current live
embedding corpus from Postgres. For the local-profile decision, the Postgres
snapshot mode is the interesting one because it uses the real current object
distribution and vector dimensionality.

Example:

```bash
cargo run --bin probe_qdrant_search -- \
  --qdrant-url http://127.0.0.1:6333 \
  --postgres-url postgres://openarchive:openarchive@127.0.0.1:5432/openarchive \
  --query-sample 25
```

The probe:

1. recreates a disposable collection
2. loads the current active embeddable objects
3. runs filtered semantic search by `object_status` and `derived_object_type`
4. runs cross-artifact filtered search by excluding the query artifact id
5. reports latency and hit behavior

Observed local result on 2026-04-13:

- `points`: `5713`
- `dimensions`: `3072`
- `load_elapsed_ms`: `14831`
- `filtered_self_hit_rate`: `0.880`
- `filtered_nonempty_rate`: `1.000`
- `cross_artifact_nonempty_rate`: `1.000`
- `filtered_latency_ms`: `p50 9.05 | p95 11.99 | max 12.02`
- `cross_artifact_latency_ms`: `p50 9.46 | p95 13.30 | max 37.55`

Exact mode produced roughly the same latency on this corpus:

```bash
cargo run --bin probe_qdrant_search -- \
  --qdrant-url http://127.0.0.1:6333 \
  --postgres-url postgres://openarchive:openarchive@127.0.0.1:5432/openarchive \
  --query-sample 25 \
  --exact
```

Observed local result:

- `filtered_self_hit_rate`: `0.880`
- `filtered_latency_ms`: `p50 9.09 | p95 10.45 | max 10.48`
- `cross_artifact_latency_ms`: `p50 9.60 | p95 11.53 | max 11.72`

Interpretation:

- latency on the current real corpus is comfortably in the "good local app"
  range
- filtered vector search behavior is working
- the `0.880` self-hit rate should **not** be read as an ANN failure by itself;
  the current archive has many duplicate entity embeddings

Relevant duplicate check from the current local database:

- duplicate `(content_text_hash, derived_object_type)` groups: `260`
- all duplicate groups are `entity`
- largest duplicate group size: `52`

That means identity-query top-1 is not guaranteed to be unique even when search
behavior is correct.

## Local Readout

Based on these probes, the local-profile direction looks viable:

- SQLite looks credible for archive state + durable local queue semantics
- Qdrant looks credible for filtered semantic search on the current corpus size
- neither probe surfaced an immediate blocker for the SQLite/Qdrant split
