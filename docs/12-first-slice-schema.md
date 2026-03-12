# First Slice Schema Contract

## Status

This document defines the provider-neutral relational schema contract for the
first vertical slice in [docs/11-first-vertical-slice-plan.md](/Users/david/src/open_archive/docs/11-first-vertical-slice-plan.md).

It is the master schema description for the application model. Oracle and
Postgres should each implement this contract through provider-specific
migrations.

## Scope

This contract is intentionally scoped to slice one:

- one source type: `chatgpt_export`
- one artifact class: `conversation`
- one context-pack type: `conversation_resume`
- minimal derived metadata: `summary`, `classification`, `memory`
- asynchronous enrichment with persistent jobs

## Design Rules

- Preserve raw source data exactly, but store large payload bytes outside the
  relational database.
- Keep canonical rows stable and idempotent.
- Model provenance explicitly.
- Separate import status from enrichment status.
- Keep the relational contract provider-neutral even when SQL implementations
  differ.

## Storage Split

The slice-one storage model has two parts:

- relational store
  - imports
  - object references
  - artifacts
  - participants
  - segments
  - derivation runs
  - derived objects
  - evidence links
  - enrichment jobs
  - optional context-pack cache
- object store
  - raw import payload bytes
  - later other large binary or text objects

The relational schema never stores host filesystem paths directly. It stores
opaque managed object references.

## Identifiers

All top-level records use application-assigned string identifiers.

This keeps identifiers stable across:

- relational providers
- object-store providers
- job payloads
- MCP and future transport surfaces

## Table Set

The first slice relational contract consists of:

1. `oa_object_ref`
2. `oa_import`
3. `oa_artifact`
4. `oa_conversation_participant`
5. `oa_segment`
6. `oa_enrichment_job`
7. `oa_derivation_run`
8. `oa_derived_object`
9. `oa_evidence_link`
10. `oa_context_pack_cache` (optional)

## 1. oa_object_ref

Represents an object stored outside the relational database.

Purpose:

- preserve durable references to copied raw payloads
- support future object-store providers without changing the relational model

Required fields:

- `object_id` primary key
- `object_kind`
- `storage_provider`
- `storage_key`
- `mime_type`
- `size_bytes`
- `sha256`
- `created_at`

Slice-one object kinds:

- `import_payload`
- `canonical_source_copy` (optional, future-ready)

Rules:

- `sha256` is unique for payload-level deduplication where the application uses
  it that way
- `storage_key` is provider-managed and opaque to callers

## 2. oa_import

Represents one import attempt.

Required fields:

- `import_id` primary key
- `source_type`
- `import_status`
- `payload_object_id` foreign key to `oa_object_ref`
- `source_filename`
- `source_content_hash`
- `conversation_count_detected`
- `conversation_count_imported`
- `conversation_count_failed`
- `created_at`
- `completed_at`
- `error_message`

Rules:

- import counts must never be negative
- imported plus failed conversations must not exceed detected conversations
- `source_filename` exists to preserve original source names for file-like
  importers even though the current ChatGPT slice does not populate it

## 3. oa_artifact

Represents one canonical conversation artifact derived from an import.

Required fields:

- `artifact_id` primary key
- `import_id` foreign key
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

Rules:

- unique on (`source_type`, `content_hash_version`, `source_conversation_hash`)
- time range must be consistent when both endpoints exist

Future revision note:

- later document/note importers should create new artifact rows for edited
  uploads rather than mutating prior artifacts in place
- a later schema extension should add explicit artifact-level lineage or
  supersession for those revision chains
- slice one does not need artifact-level supersession yet because the current
  import path is chat-oriented rather than file-revision-oriented

## 4. oa_conversation_participant

Represents one participant within a conversation artifact.

Required fields:

- `participant_id` primary key
- `artifact_id` foreign key
- `participant_role`
- `display_name`
- `provider_name`
- `model_name`
- `source_participant_key`
- `sequence_no`
- `created_at`

Rules:

- unique on (`artifact_id`, `sequence_no`)

## 5. oa_segment

Represents one ordered segment within an artifact.

Required fields:

- `segment_id` primary key
- `artifact_id` foreign key
- `participant_id` foreign key, nullable
- `segment_type`
- `source_segment_key`
- `parent_segment_id`
- `sequence_no`
- `created_at_source`
- `text_content`
- `text_content_hash`
- `locator_json`
- `visibility_status`
- `unsupported_content_json`
- `created_at`

Rules:

- unique on (`artifact_id`, `sequence_no`, `segment_type`)
- message segments require `text_content`

## 6. oa_enrichment_job

Represents one durable enrichment job for an artifact.

Required fields:

- `job_id` primary key
- `artifact_id` foreign key
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

Rules:

- claim bookkeeping must stay paired
- provider SQL must support safe concurrent claims
- the application-level lifecycle remains:
  - pending or retryable -> running -> completed
  - pending or retryable -> running -> failed
  - pending or retryable -> running -> retryable

## 7. oa_derivation_run

Represents one execution of a derivation pipeline.

Required fields:

- `derivation_run_id` primary key
- `artifact_id` foreign key
- `job_id` foreign key, nullable
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

## 8. oa_derived_object

Represents a persisted semantic output.

Required fields:

- `derived_object_id` primary key
- `artifact_id` foreign key
- `derivation_run_id` foreign key
- `derived_object_type`
- `origin_kind`
- `object_status`
- `confidence_score`
- `confidence_label`
- `scope_type`
- `scope_id`
- `title`
- `body_text`
- `object_json`
- `supersedes_derived_object_id`
- `created_at`

## 9. oa_evidence_link

Connects a derived object to the segment evidence that supports it.

Required fields:

- `evidence_link_id` primary key
- `derived_object_id` foreign key
- `segment_id` foreign key
- `evidence_role`
- `evidence_rank`
- `support_strength`
- `created_at`

Rules:

- unique on (`derived_object_id`, `evidence_rank`)
- unique on (`derived_object_id`, `segment_id`)

## 10. oa_context_pack_cache

Optional cache table for built context packs.

This table may be deferred, but when present it should use:

- `context_pack_id` primary key
- `artifact_id` foreign key
- `pack_type`
- `pack_status`
- `request_hash`
- `pack_json`
- `derivation_run_id` foreign key
- `created_at`
- `expires_at`

## Provider Responsibilities

Provider-specific migrations may differ in:

- SQL syntax
- string and text types
- JSON storage types or validation
- timestamp defaults
- index DDL
- migration bookkeeping tables
- locking/query details for job claims

Provider-specific migrations must not change:

- table meanings
- column meanings
- uniqueness and idempotency semantics
- job lifecycle semantics
- provenance relationships

## Current Providers

- Oracle: secondary provider, resettable during the migration
- Postgres: target default relational provider

Both providers should converge on this same contract.
