# First Slice Schema Design

## Status

This document defines the first-pass storage schema for the first vertical
slice in [docs/11-first-vertical-slice-plan.md](/Users/david/src/open_archive/docs/11-first-vertical-slice-plan.md).

It is implementation-oriented. The goal is to make the first Oracle ADB schema
decision-complete enough that service code can be written against it without
inventing new concepts.

This schema is intentionally scoped to slice one:

- one source type: `chatgpt_export`
- one artifact class: `conversation`
- one context-pack type: `conversation_resume`
- minimal derived metadata: `summary`, `classification`, `memory`
- async enrichment with persistent jobs

## Design Principles

- Preserve raw source data exactly
- Keep canonical rows stable and idempotent
- Model provenance explicitly
- Separate import status from enrichment status
- Store machine-readable semantic outputs, not UI-shaped blobs
- Prefer direct foreign keys over duplicate reference encodings when slice-one
  storage is single-path
- Keep schema broad enough for later growth, but only implement slice-one
  object types now

## Storage Scope

For slice one, all storage should live in Oracle ADB.

That includes:

- raw import payloads
- canonical artifacts and segments
- provenance records
- derived metadata
- enrichment jobs

Object storage is deferred.

## Identifier Strategy

Use application-assigned UUID-style identifiers for all top-level records.

Recommended pattern:

- primary keys stored as `VARCHAR2(36 CHAR)` or similar string id type
- natural keys and hashes stored separately with unique constraints where needed

Reason:

- keeps IDs stable across application boundaries
- avoids Oracle identity coupling in the application core
- simplifies job payload and API identifiers

## Table Set

The first slice should use these tables:

1. `oa_import`
2. `oa_import_payload`
3. `oa_artifact`
4. `oa_conversation_participant`
5. `oa_segment`
6. `oa_derivation_run`
7. `oa_derived_object`
8. `oa_evidence_link`
9. `oa_enrichment_job`
10. `oa_context_pack_cache` (optional cache table, may be deferred)

## 1. oa_import

Represents one uploaded ChatGPT export import attempt.

### Purpose

- track the lifecycle of one import request
- separate import-level status from per-conversation artifact status
- preserve source-level metadata for multi-conversation exports

### Columns

- `import_id` PK
- `source_type`
- `import_status`
- `payload_id` FK to `oa_import_payload`
- `source_filename`
- `source_content_hash`
- `conversation_count_detected`
- `conversation_count_imported`
- `conversation_count_failed`
- `created_at`
- `completed_at`
- `error_message`

### Allowed values

`source_type`

- `chatgpt_export`

`import_status`

- `pending`
- `parsing`
- `completed`
- `completed_with_errors`
- `failed`

### Constraints

- `source_type` required
- `payload_id` required
- `source_content_hash` indexed

## 2. oa_import_payload

Stores the raw uploaded export payload.

### Purpose

- preserve raw source exactly
- keep import and artifact records referring to a stable raw payload root

### Columns

- `payload_id` PK
- `payload_format`
- `payload_mime_type`
- `payload_bytes` BLOB
- `payload_size_bytes`
- `payload_sha256`
- `created_at`

### Allowed values

`payload_format`

- `chatgpt_export_zip`
- `chatgpt_export_json`
- `chatgpt_export_canonical_json`

### Constraints

- `payload_sha256` unique

### Notes

- Slice one may accept either a raw uploaded export file or a JSON payload
- If the API later narrows to zip-only intake, `chatgpt_export_json` and
  `chatgpt_export_canonical_json` can be removed before DDL

## 3. oa_artifact

Represents one canonical conversation artifact derived from the import.

### Purpose

- root row for slice-one brain artifacts
- support idempotent per-conversation imports from multi-conversation exports

### Columns

- `artifact_id` PK
- `import_id` FK to `oa_import`
- `artifact_class`
- `source_type`
- `artifact_status`
- `enrichment_status`
- `source_conversation_key`
- `source_conversation_hash`
- `title`
- `created_at_source`
- `captured_at`
- `started_at`
- `ended_at`
- `primary_language`
- `content_hash_version`
- `content_facets_json`
- `normalization_version`
- `error_message`

### Allowed values

`artifact_class`

- `conversation`

`source_type`

- `chatgpt_export`

`artifact_status`

- `captured`
- `normalized`
- `failed`

`enrichment_status`

- `pending`
- `running`
- `completed`
- `partial`
- `failed`

### Constraints

- unique on (`source_type`, `content_hash_version`, `source_conversation_hash`)
- index on `import_id`
- index on `enrichment_status`

### Notes

- `source_conversation_key` is the source-native identifier if present
- `source_conversation_hash` is the application-computed per-conversation hash
  used for idempotency
- `content_hash_version` identifies the hash algorithm and normalization basis
  used for `source_conversation_hash`
- `content_facets_json` should at least store `["messages","text","participants","timestamps"]`
- raw payload linkage should resolve through `oa_import.payload_id` in slice one
  rather than duplicating a second payload reference on every artifact row

## 4. oa_conversation_participant

Represents one participant in a conversation artifact.

### Purpose

- preserve speaker identity at the canonical layer
- support user/assistant/system roles without overcomplicating slice one

### Columns

- `participant_id` PK
- `artifact_id` FK to `oa_artifact`
- `participant_role`
- `display_name`
- `provider_name`
- `model_name`
- `source_participant_key`
- `sequence_no`
- `created_at`

### Allowed values

`participant_role`

- `user`
- `assistant`
- `system`
- `tool`
- `unknown`

### Constraints

- index on `artifact_id`

## 5. oa_segment

Represents one addressable message segment inside a conversation artifact.

### Purpose

- canonical ordered message storage
- smallest useful provenance anchor for slice one
- basis for chunk/window enrichment later

### Columns

- `segment_id` PK
- `artifact_id` FK to `oa_artifact`
- `participant_id` FK to `oa_conversation_participant`, nullable for some
  system/internal cases
- `segment_type`
- `source_segment_key`
- `parent_segment_id` nullable self-FK
- `sequence_no`
- `created_at_source`
- `text_content` CLOB
- `text_content_hash`
- `locator_json`
- `visibility_status`
- `unsupported_content_json`
- `created_at`

### Allowed values

`segment_type`

- `message`
- `message_window`

`visibility_status`

- `visible`
- `hidden`
- `skipped_unsupported`

### Constraints

- unique on (`artifact_id`, `sequence_no`, `segment_type`)
- index on `artifact_id`
- index on `participant_id`

### Notes

- In slice one, imported ChatGPT messages become `message` segments
- `message_window` rows are optional and only needed if chunk-and-reduce is
  materialized as persisted window segments rather than transient windows
- `unsupported_content_json` records skipped non-text/image/tool content for
  transparency

## 6. oa_derivation_run

Represents one execution of a derivation step.

### Purpose

- record lineage for summary/classification/memory extraction
- support reprocessing and debugging

### Columns

- `derivation_run_id` PK
- `artifact_id` FK to `oa_artifact`
- `job_id` FK to `oa_enrichment_job`, nullable for future non-job derivations
- `run_type`
- `pipeline_name`
- `pipeline_version`
- `provider_name`
- `model_name`
- `prompt_version`
- `run_status`
- `input_scope_type`
- `input_scope_json`
- `started_at`
- `completed_at`
- `error_message`

### Allowed values

`run_type`

- `summary_extraction`
- `classification_extraction`
- `memory_extraction`
- `summary_reduction`
- `memory_reduction`
- `context_pack_assembly`

`run_status`

- `running`
- `completed`
- `failed`

`input_scope_type`

- `artifact`
- `segment_window`
- `artifact_reduce`

### Constraints

- index on `artifact_id`
- index on `job_id`

## 7. oa_derived_object

Stores persisted derived outputs for slice one.

### Purpose

- one flexible table for the initial metadata layer
- avoid premature table explosion while still preserving typed objects

### Columns

- `derived_object_id` PK
- `artifact_id` FK to `oa_artifact`
- `derivation_run_id` FK to `oa_derivation_run`
- `derived_object_type`
- `origin_kind`
- `object_status`
- `confidence_score`
- `confidence_label`
- `scope_type`
- `scope_id`
- `title`
- `body_text` CLOB
- `object_json` CLOB
- `supersedes_derived_object_id` nullable self-FK
- `created_at`

### Allowed values

`derived_object_type`

- `summary`
- `classification`
- `memory`

`origin_kind`

- `explicit`
- `deterministic`
- `inferred`
- `user_confirmed`

`object_status`

- `active`
- `superseded`
- `failed`

`scope_type`

- `artifact`
- `segment`

### Usage by type

`summary`

- `title`: optional summary label
- `body_text`: normalized summary text
- `object_json`: optional structured summary fields

`classification`

- `title`: classification type
- `body_text`: optional human-readable explanation
- `object_json`: machine-readable classification payload

`memory`

- `title`: memory type
- `body_text`: memory statement
- `object_json`: machine-readable memory payload

### Constraints

- index on `artifact_id`
- index on `derivation_run_id`
- index on `derived_object_type`

### Notes

- Slice one uses a single typed derived-object table for speed
- Slice-one memories should be artifact-scoped or segment-scoped only
- If needed later, `summary`, `classification`, and `memory` can be split into
  dedicated tables without changing the conceptual model

## 8. oa_evidence_link

Links derived objects back to supporting segments.

### Purpose

- enforce segment-level provenance for all slice-one derived outputs

### Columns

- `evidence_link_id` PK
- `derived_object_id` FK to `oa_derived_object`
- `segment_id` FK to `oa_segment`
- `evidence_role`
- `evidence_rank`
- `support_strength`
- `created_at`

### Allowed values

`evidence_role`

- `primary_support`
- `secondary_support`
- `reduction_input`

`support_strength`

- `strong`
- `medium`
- `weak`

### Constraints

- index on `derived_object_id`
- index on `segment_id`

## 9. oa_enrichment_job

Represents one asynchronous enrichment job for a conversation artifact.

### Purpose

- durable source of truth for async enrichment
- basis for future MQ or out-of-process execution

### Columns

- `job_id` PK
- `artifact_id` FK to `oa_artifact`
- `job_type`
- `job_status`
- `attempt_count`
- `max_attempts`
- `priority_no`
- `claimed_by`
- `claimed_at`
- `available_at`
- `payload_json`
- `last_error_message`
- `created_at`
- `completed_at`

### Allowed values

`job_type`

- `conversation_enrichment`

`job_status`

- `pending`
- `running`
- `completed`
- `partial`
- `failed`
- `retryable`

### Constraints

- index on `job_status`
- index on `artifact_id`
- index on `available_at`

### Notes

- `payload_json` should be self-contained enough for future MQ execution
- `claimed_by` identifies the worker instance/thread logically, not necessarily
  a durable machine identity

## 10. oa_context_pack_cache

Optional cache for built context packs.

### Purpose

- allow caching if pack assembly proves expensive
- remain optional for slice one

### Columns

- `context_pack_id` PK
- `artifact_id` FK to `oa_artifact`
- `pack_type`
- `pack_status`
- `request_hash`
- `pack_json` CLOB
- `derivation_run_id` FK to `oa_derivation_run`
- `created_at`
- `expires_at`

### Allowed values

`pack_type`

- `conversation_resume`

`pack_status`

- `active`
- `stale`

### Notes

- This table can be deferred entirely if packs are generated on demand in slice one

## Status Model

The first slice needs three distinct status layers:

1. import status
2. artifact status
3. enrichment/job status

They should not be collapsed into one field.

### Import success

Means:

- raw export payload stored
- conversations parsed as far as possible
- canonical artifact rows written for successful conversations

It does not mean:

- enrichment completed

### Enrichment success

Means:

- summary/classification/memory derivations finished for a conversation

It does not mean:

- every conversation in the import succeeded

## Idempotency Rules

Slice one should use two levels of hashing:

- payload-level hash on the full uploaded export
- conversation-level hash on each normalized conversation

### Payload hash

Used to deduplicate exact repeated uploads.

### Conversation hash

Used to prevent duplicate artifacts when:

- the same export is imported twice
- a later export contains conversations already ingested

The conversation hash should be computed from the normalized conversation
content actually used in slice one:

- ordered text message content
- participant roles
- relevant timestamps

Unsupported skipped content should not make idempotency unstable.

### Hash version rule

`content_hash_version` must be treated as part of the idempotency contract.

- If normalization logic changes in a way that changes hash inputs,
  `content_hash_version` must also change
- Re-import deduplication should only assume equivalence within the same
  `content_hash_version`
- A normalization rewrite should create a deliberate migration/reprocessing plan
  rather than silently colliding with older hashes

## ChatGPT Export Mapping

The schema assumes this slice-one mapping:

- each ChatGPT conversation becomes one `oa_artifact` row
- each logical speaker becomes one `oa_conversation_participant` row
- each flattened text-bearing message becomes one `oa_segment` row

Slice-one parsing policy:

- flatten tree structure to linear ordered messages
- support text-oriented content only
- skip unsupported tool/image/non-text payloads
- record skipped content in `unsupported_content_json`
- keep one stable hash basis for the normalized text-oriented representation
  used in slice one

## Derived Metadata Shape

Slice one should persist exactly these derived object types:

- `summary`
- `classification`
- `memory`

### Summary object_json

Should support fields like:

- `summary_kind`
- `summary_version`

### Classification object_json

Should support fields like:

- `classification_type`
- `classification_value`

### Memory object_json

Should support fields like:

- `memory_type`
- `memory_scope`
- `memory_scope_value`

This keeps the table flexible while still enforcing typed usage in code.

For slice one, `memory_scope` should stay within the persisted scope model:

- `artifact`
- `segment`

## Context-Pack Assembly Inputs

`conversation_resume` should be built from:

- one `oa_artifact`
- its `oa_segment` rows
- active `summary` derived object
- active `classification` derived objects
- active `memory` derived objects
- evidence links for those derived objects

The pack should be generated on demand by default.

## Minimal API Query Support

The schema must support these query patterns efficiently:

### `POST /imports/chatgpt`

- write payload
- create import
- create artifacts
- create participants
- create segments
- create jobs

### `GET /artifacts`

- list artifact id
- title
- source type
- created/captured timestamps
- enrichment status

### `GET /context-packs/conversation-resume`

- load one artifact
- load ordered segments
- load active summaries/classifications/memories
- load evidence links and supporting segments

## Oracle Notes

- prefer `CLOB` for text and JSON bodies in slice one
- store JSON in text columns initially unless there is a strong Oracle JSON
  reason to enforce JSON-native types immediately
- add check constraints for enumerated statuses where practical
- define explicit nullability and defaults during DDL authoring rather than
  leaving column behavior implicit
- add indexes only for known slice-one query paths
- avoid overusing Oracle-specific advanced features until the basic slice is
  proven

## Deferred From Slice One

These are intentionally not part of this first schema:

- general non-conversation artifact classes
- entity/mention/relationship tables
- object storage references outside ADB
- auth/user/account tables
- full retrieval feature or embedding storage
- provider-pluggable inference metadata beyond basic provider/model fields

## Recommended Build Order

1. create `oa_import` and `oa_import_payload`
2. create `oa_artifact`, `oa_conversation_participant`, and `oa_segment`
3. create `oa_enrichment_job`
4. create `oa_derivation_run`, `oa_derived_object`, and `oa_evidence_link`
5. add `oa_context_pack_cache` only if needed

This order matches the actual slice flow:

- ingest first
- enrich second
- cache later if justified
