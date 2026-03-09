# First Vertical Slice Plan

## Summary

Build one end-to-end brain slice that ingests a `ChatGPT export`, persists it
in `Oracle ADB`, enriches it through the first three brain layers, and returns
a `conversation_resume` context pack through a small HTTP API.

This slice should prove the core architecture, not the whole product. It should
validate:

- the artifact model for a real source
- segment-level provenance and derivation lineage
- persisted machine-readable metadata
- task-shaped context assembly for AI consumption

The implementation should use thin seams around storage and inference from day
one, but only one concrete backend in slice one:

- storage: Oracle ADB
- inference: one concrete extraction/summarization path
- enrichment execution: persistent jobs with in-process bounded workers

## Implementation Changes

### Slice behavior

- Support exactly one source type: `ChatGPT export`
- Treat imported conversations as `conversation` artifacts
- Limit slice one parsing to text-oriented conversation content
- Normalize one export into:
  - artifact record
  - participant records
  - message segments
  - raw payload reference
  - import/derivation lineage
- Persist a minimal but real metadata layer:
  - `Classification`
  - `Memory`
  - summary
- Treat `Entity`, `EntityMention`, and `Relationship` as deferred unless a
  later design pass explicitly brings them back into slice one
- Produce exactly one context-pack type: `conversation_resume`

### Public interfaces

- Add a small HTTP API with two endpoints:
  - `POST /imports/chatgpt`
    - accepts one ChatGPT export payload or file upload
    - persists raw/canonical data, creates enrichment jobs, and returns import
      id, artifact ids created, import status, and enrichment status
  - `GET /artifacts`
    - returns a minimal list of imported conversation artifacts with id, title,
      source type, created/captured timestamps, and enrichment status
  - `GET /context-packs/conversation-resume`
    - input by conversation/artifact id
    - returns one machine-readable `conversation_resume` pack
- Keep API shapes compact and machine-first
- Return provenance-bearing identifiers rather than large raw source dumps by
  default

### Storage and service boundaries

- Define thin internal seams only:
  - repository/storage boundary for persisted brain objects
  - inference/extraction boundary for summary/entity/memory extraction
  - context-pack assembly boundary
  - enrichment job store/dispatcher/executor boundary
- Implement only one concrete storage path now:
  - Oracle ADB via the proven local wallet + Instant Client path
- Implement only one concrete inference path now
- Use a persistent job model from the start so in-process workers can evolve
  later into MQ-backed or out-of-process execution without redesigning the job
  contract
- Do not build provider selection, plugin registration, or multi-backend
  execution yet

### Execution and concurrency guardrails

- Slice one may run in a single application process, but internal execution
  should still follow explicit pipeline-stage boundaries
- Prefer synchronous Rust for request handling and storage unless a specific
  stage proves a concrete need for async execution
- Treat blocking Oracle calls as acceptable only within bounded worker and
  connection pools
- Do not spawn ad hoc threads per request or per job; use fixed-size worker
  pools
- Keep long-running enrichment and future hydration work out of request
  handlers
- Use the database as the durable source of truth for stage transitions, job
  status, and retries
- In-process channels or dispatch loops are allowed, but they must not become
  the only place where outstanding work exists
- Preserve job contracts and stage interfaces so moving from in-process workers
  to MQ-backed or distributed execution does not require redesign

Recommended slice-one worker classes:

- request workers for short HTTP handling only
- import/normalization workers for parse and canonical write work
- enrichment workers for model-facing derivation jobs

These classes may share one binary at first, but their concurrency limits
should remain independently configurable.

### Brain-layer scope for slice one

- Artifact layer:
  - implement `conversation` only
  - support source type `chatgpt_export`
  - model ordered message segments and participants
- Provenance layer:
  - implement `Artifact`, `Segment`, `DerivationRun`, `DerivedObject`, and
    `EvidenceLink`
  - require segment-level evidence for all persisted derived metadata
- Metadata layer:
  - `Classification`: at least one conversation-level classification
  - `Memory`: small durable memory set grounded in evidence
  - `Summary`: one conversation summary
- Context-pack layer:
  - implement `conversation_resume` only
  - include request frame, evidence set, distilled semantic context, provenance
    envelope, and policy/budget fields
  - optimize output for machine consumption, not human browsing

### Ingestion pipeline

- Build one ingestion path for ChatGPT export only
- Treat the supported input as the real ChatGPT export structure, including
  multi-conversation exports
- Ingestion flow:
  - parse export payload
  - identify conversations from the export structure
  - flatten conversation trees into ordered message sequences
  - extract text-oriented message content only
  - skip unsupported non-text/tool/image content with explicit status or
    logging
  - create conversation artifact records
  - create participant/message segments
  - preserve raw source payload reference
  - compute per-conversation content hash for idempotency
  - create enrichment jobs
- Handle partial import safely:
  - import status tracked explicitly
  - failed conversation normalization should not corrupt successful imports
  - re-import of the same payload should be idempotent per conversation by
    source identity and/or content hash

### Large-conversation handling

- Do not assume one conversation always fits into one model context window
- Treat message segments/windows as the basic unit of enrichment when needed
- Support two enrichment paths in slice one:
  - direct conversation-level enrichment when the conversation fits within a
    configured token budget
  - chunk-and-reduce enrichment when the conversation exceeds that budget
- In the chunk-and-reduce path:
  - run enrichment on ordered message windows
  - persist derivation runs and evidence for window-level outputs
  - reduce window-level outputs into conversation-level summary and memory
    outputs
- Context packs should remain selective regardless of source conversation size
- Exact chunk sizing policy can be implementation-tuned, but the architecture
  must support both direct and hierarchical enrichment from the start

### Raw payload storage

- Store raw payload data in Oracle ADB for slice one
- Use a simple ADB-backed raw payload reference strategy first rather than
  introducing object storage now
- Revisit object storage only after the brain slice is proven and raw payload
  size or cost becomes a real problem

### Enrichment execution model

- Enrichment is asynchronous
- `POST /imports/chatgpt` should not wait for AI enrichment to complete
- Import success means raw/canonical artifact persistence succeeded
- Enrichment success is tracked separately per conversation artifact
- Use a persistent enrichment job model with these responsibilities:
  - durable job payload
  - claim/update/complete/fail lifecycle
  - retryable execution
  - explicit job and enrichment statuses
- Use a bounded in-process worker pool in slice one
- Design the job contract so the same executor can later run from:
  - in-process worker threads
  - MQ-backed workers
  - other out-of-process execution models
- Separate these concerns explicitly:
  - job definition
  - job persistence/dispatch
  - job execution
- Treat the database as the source of truth for job state
- Do not perform remote model calls inline in the request path
- Keep worker concurrency conservative by default on small OCI instances and
  expand only with measurement

### Enrichment failure handling

- Allow partial enrichment success per conversation artifact
- Persist successful summary/classification/memory outputs even if a later
  enrichment step fails
- Track enrichment status separately from import status
- Expose enrichment state through `GET /artifacts`
- `GET /context-packs/conversation-resume` should either:
  - return the best available pack when minimum required enrichment exists, or
  - return a not-ready response when required enrichment has not completed yet

### Retrieval and context assembly

- `conversation_resume` assembly should return:
  - task/request frame
  - concise conversation summary
  - key extracted memories
  - lightweight classification output
  - open questions or unresolved threads only as best-effort optional output
  - supporting message excerpts
  - provenance and selection reasons
- Context pack should be rebuildable
- Persisting the context pack itself is optional; the default should be
  generate-on-demand unless caching is clearly needed during implementation

### Context-pack response shape

- `conversation_resume` should serialize in a predictable machine-readable
  shape, roughly:
  - `context_pack_id`
  - `pack_type`
  - `request_frame`
  - `summary`
  - `memories`
  - `classifications`
  - `supporting_evidence`
  - `uncertainties`
  - `provenance`
  - `policy`
- `supporting_evidence` should reference message segments and include short
  excerpts rather than full transcript dumps
- `provenance` should explain selection and derivation sources in structured
  form

## Test Plan

- Import one valid ChatGPT export with one conversation and verify:
  - import accepted without waiting for enrichment completion
  - artifact created
  - participants created
  - message segments created in order
  - raw payload reference stored
  - per-conversation content hash stored
- Import one valid multi-conversation ChatGPT export and verify:
  - each conversation becomes a separate artifact
  - partial failure of one conversation does not corrupt successful ones
- Re-import the same export and verify idempotent behavior
- Import malformed or partial export input and verify:
  - failure is explicit
  - successful prior data is not corrupted
- Verify derived metadata persistence:
  - summary exists
  - at least one classification exists
  - memories have evidence links
- Verify provenance:
  - every persisted derived object has a derivation run
  - every persisted derived object has segment-level evidence
- Verify async enrichment:
  - jobs are created durably
  - worker execution updates job state correctly
  - partial enrichment failure is visible and recoverable
- Verify `conversation_resume` response:
  - returns compact machine-readable structure
  - includes evidence-backed excerpts
  - includes provenance envelope
  - excludes full raw transcript unless explicitly requested
  - reflects not-ready or partial-enrichment states correctly
- Verify local ADB-backed development path works with current wallet/client
  setup

## Assumptions

- Local Oracle ADB development is a valid target for slice one and already
  proven feasible in this environment.
- Slice one is intentionally brain-layer focused; polished mobile UX, broader
  auth design, and richer access-surface design remain out of scope.
- Thin seams for storage and inference are required now, but full pluggable
  provider architecture is deferred.
- Thin seams are also required around enrichment jobs so the executor can move
  from in-process threads to MQ-backed or other out-of-process execution later.
- The first slice should validate all four brain layers in one narrow path
  rather than implementing broad source coverage.
- `ChatGPT export` is the canonical first source and `conversation_resume` is
  the canonical first context-pack type.
- Slice one favors async enrichment and explicit status over synchronous
  end-to-end completion in a single request.
- Slice one favors bounded thread and connection counts over maximizing
  concurrency density within one process.
