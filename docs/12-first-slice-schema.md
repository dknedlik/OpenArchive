# First Slice Schema Design

## Status

This document defines the first-pass storage shape for the first vertical slice
in [docs/11-first-vertical-slice-plan.md](/Users/david/src/open_archive/docs/11-first-vertical-slice-plan.md).

It is implementation-oriented.

The goal is to make the first slice storage decisions complete enough that
service code can be written without inventing new concepts.

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
- Keep large artifact bytes outside the relational database
- Keep schema broad enough for later growth, but only implement slice-one
  object types now

## Storage Scope

For slice one, storage is split across two concerns:

- relational storage in Postgres
- object storage on a local filesystem-backed Docker volume

The relational schema owns:

- imports
- artifacts
- participants
- segments
- derivation runs
- derived objects
- evidence links
- enrichment jobs
- object references

The object store owns:

- raw import payload bytes
- other large binary or text objects that do not belong inline in relational
  rows

## Identifier Strategy

Use application-assigned UUID-style identifiers for top-level records and
cross-store references.

Reason:

- keeps IDs stable across application boundaries
- avoids database-generated identity coupling in the application core
- simplifies job payloads and cross-store references

## Table Set

The first slice should use these tables:

1. `oa_import`
2. `oa_object_ref`
3. `oa_artifact`
4. `oa_conversation_participant`
5. `oa_segment`
6. `oa_derivation_run`
7. `oa_derived_object`
8. `oa_evidence_link`
9. `oa_enrichment_job`
10. `oa_context_pack_cache` (optional, may be deferred)

## Object Reference Model

`oa_object_ref` should represent objects stored outside the relational
database.

Suggested fields:

- `object_id` PK
- `object_kind`
- `storage_provider`
- `storage_key`
- `mime_type`
- `size_bytes`
- `sha256`
- `created_at`

Likely slice-one object kinds:

- `import_payload`
- `canonical_source_copy`
- later larger derived artifacts if needed

The important rule is:

- rows refer to objects by opaque managed references, not host filesystem paths

## Notes

- Raw payload bytes should not be stored as Postgres large objects or inline
  byte columns for slice one
- The object-store boundary should make a future S3-compatible provider
  straightforward
- Vector storage is intentionally out of scope for this slice-one schema
