# OpenArchive Brain Overview

## Status

This document captures the current working understanding of the "brain" layer
for OpenArchive. It is a jumping-off point, not a finished architecture spec.

## Core Thesis

OpenArchive should be built around a durable brain layer first, with access
layers added on top as the surrounding AI ecosystem matures.

The brain is the stable asset.

Access methods are expected to change over time:

- web UI
- HTTPS API
- remote MCP
- ChatGPT actions or connectors
- Claude integrations
- future vendor-specific integrations

The system should not depend on any one access surface being permanent.

## What The Brain Is For

The brain exists to make user knowledge reusable across time, tools, and AI
systems.

That includes:

- preserving source material
- making it retrievable
- extracting useful structure from messy inputs
- assembling compact context for future work
- supporting analysis and durable memory

The immediate use case is AI chats, because that is where a large amount of
active brainstorming already happens. But the brain should be broader than chat
archives alone.

Potential source types include:

- AI chat conversations
- notes
- pasted text snippets
- links and bookmarked pages
- documents and exports
- screenshots and OCR-derived text
- later, other personal/work artifacts if useful

## High-Level Brain Responsibilities

The brain layer should eventually cover:

- ingestion
- raw artifact preservation
- canonical structure
- provenance and evidence tracking
- metadata extraction
- identity and linking across related artifacts
- search and retrieval
- context-pack generation
- summaries and analysis
- extracted durable memory
- versioning of derived artifacts
- trust/confidence tracking
- privacy and permission boundaries
- observability and auditability
- export and portability

## Core Product Principle

Capture first, structure later.

Ingestion must be extremely easy. The user should not need to decide how to
file something before saving it. The system should accept messy input quickly,
preserve it faithfully, and then enrich it afterward.

That implies a three-layer flow:

1. raw capture
2. canonical normalization
3. derived enrichment

## Raw Versus Derived

One of the most important design boundaries is the difference between source
truth and AI-derived interpretation.

The system must preserve the raw artifact exactly.

AI can then help generate:

- titles
- summaries
- classifications
- tags
- inferred entities
- inferred relationships
- candidate memories
- relevance scores

But those outputs should be treated as derived artifacts, not as the original
record.

That keeps the system reprocessable and reduces the damage from model mistakes.

## Where AI Helps

AI should do a large share of the semantic organization work.

Examples:

- classifying content type
- detecting projects, people, topics, and decisions
- clustering related items
- extracting possible durable memories
- producing compact summaries
- building context packs for future prompts or agent calls

The deterministic system should still own:

- raw preservation
- identifiers
- timestamps
- source attribution
- provenance
- version history

## Why This Is More Than Notes Plus Search

If the system only stores text and supports keyword search, it is not strong
enough to justify itself.

The value has to come from context reuse, not just storage.

That means the brain should eventually help answer questions like:

- what past conversations are relevant to this current task
- what stable preferences or facts about the user have already been expressed
- what decisions have already been made on this topic
- what prior brainstorming should be brought into this session
- what compact context packet should be handed to an AI tool right now

## Mobile And Usability Reality

Mobile use is a hard requirement for this project.

The system will not be good enough if it only works well through desktop
scripts, local agents, or developer workflows. The user does a large amount of
brainstorming and idea capture on the go.

That means the brain must be designed so that:

- ingestion is mobile-friendly
- retrieval is mobile-friendly
- context can be reused from phone-based AI workflows

The ecosystem may not support universal deep integration with every consumer AI
app. Because of that, the brain should be designed so the core remains useful
even while access layers are uneven.

## Working Architectural Shape

The current working shape is:

- brain layer first
- access layers second

Brain layer:

- source ingestion pipeline
- durable storage for raw and canonical artifacts
- derived enrichment pipeline
- retrieval and context assembly services

Access layers:

- direct web UI
- authenticated HTTPS API
- remote MCP
- vendor-specific integrations where available

## Storage Direction

No final storage design is locked in yet.

The strongest current direction is:

- structured and queryable archive data in Oracle ADB
- large raw source payloads potentially in object storage
- references, hashes, provenance, and derived metadata in ADB

This remains an open design area, but the principle is clear: the storage model
should optimize for durable preservation and useful retrieval, not just put
everything into one bucket because it is convenient.

## Core Brain Layers

The current working brain design is now split into four main layers:

1. artifact model
2. provenance model
3. derived metadata model
4. context-pack model

These layers are documented in:

- [docs/07-artifact-model.md](/Users/david/src/open_archive/docs/07-artifact-model.md)
- [docs/08-provenance-model.md](/Users/david/src/open_archive/docs/08-provenance-model.md)
- [docs/09-derived-metadata-model.md](/Users/david/src/open_archive/docs/09-derived-metadata-model.md)
- [docs/10-context-pack-model.md](/Users/david/src/open_archive/docs/10-context-pack-model.md)

Together they define whether the brain is flexible enough to support multiple
source types and multiple future access layers.

## What Still Needs Design

The core concepts are now sketched, but several follow-on areas remain open:

- cross-document consistency and terminology refinement
- first-pass schema design
- ingestion pipeline design
- access-surface design
- storage layout and retention rules

## Current Summary

The current direction is not "build a chat archive."

It is:

- build a user-owned brain layer
- start with AI chats as the primary source
- support broader artifact types over time
- preserve raw data and provenance
- use AI to enrich and organize the data
- retrieve and assemble machine-usable context for future AI and human
  workflows
- expose access through multiple interfaces as the ecosystem allows
