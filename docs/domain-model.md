# OpenArchive Domain Model

## Overview

OpenArchive models an archive first and a retrieval product second.

The core domain splits into four layers:

- canonical archive records
- provenance and evidence
- derived knowledge
- retrieval-facing projections

## Canonical Archive

The canonical archive is the stable stored record of imported material.

Key objects:

- `Import`: one import operation and its source payload
- `Artifact`: the top-level archived unit
- `Participant`: named or role-based participants attached to an artifact
- `Segment`: ordered source passages used for provenance and enrichment

Supported source types today:

- `chatgpt_export`
- `claude_export`
- `grok_export`
- `gemini_takeout`

The canonical model is intentionally broader than chat transcripts even though
the current import coverage is still chat-export-heavy.

## Raw Payload Preservation

OpenArchive preserves source payload bytes separately from the relational
model.

That gives the system:

- auditability
- reprocessing ability
- clean separation between large objects and queryable metadata

The relational layer should reference stored objects, not inline raw payload
content.

## Derived Knowledge

The enrichment pipeline writes typed derived objects rather than a single blob
of model output.

Current first-class derived object types:

- `summary`
- `classification`
- `memory`
- `entity`
- `relationship`

These objects are durable archive state. They are not temporary prompt
fragments.

Derived objects carry the kinds of fields needed for retrieval and curation,
including:

- title and body text
- type-specific fields such as classification type, memory type, or entity type
- candidate keys for deduplication and reconciliation
- confidence signals
- object status

## Provenance And Evidence

Derived knowledge is expected to remain grounded in source material.

Important provenance records include:

- derivation runs
- artifact-level provenance from derived objects back to their source artifact
- origin metadata describing whether an object came from enrichment or
  writeback

This is what keeps the archive from collapsing into an ungrounded pile of
assertions.

## Writeback And Curation

The archive is not write-only from the enrichment pipeline.

MCP clients can currently:

- store memories
- store entities
- create links between objects
- mark objects as superseded or rejected

That means the domain already includes user- and agent-authored archive state,
not only model-authored outputs.

This is an important product direction. The archive is meant to support
continuous refinement over time, even though a full human review UI does not
exist yet.

## Retrieval-Facing Projections

The stored archive is surfaced through a few retrieval contracts:

- archive search
- artifact detail
- artifact context packs
- derived-object search
- artifact timelines
- object graph traversal

Context packs are compact, machine-facing summaries of the most relevant
grounded context for an artifact. They are not a dump of the whole archive and
they are not meant to replace the canonical stored model.

## What The Model Is Optimizing For

The domain model is optimized for:

- durable storage
- grounded retrieval
- machine consumption
- later correction and curation

It is not optimized for:

- pretending every source is the same kind of flat document
- reducing the system to embeddings plus chunk retrieval
- shaping the core around one transport protocol or one provider
