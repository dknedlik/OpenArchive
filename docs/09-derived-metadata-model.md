# OpenArchive Derived Metadata Model

## Status

This is the first-pass draft of the derived metadata model for the OpenArchive
brain layer. It describes how the system should represent meaning, relatedness,
and durable context on top of raw artifacts and provenance.

## Purpose

The artifact model defines what the system stores.

The provenance model defines how stored and derived knowledge stays grounded.

The derived metadata model defines the semantic layer that makes the brain
useful for:

- finding related artifacts
- identifying people, projects, books, and concepts
- extracting durable memories
- assembling context for future AI and human workflows

This is the layer where the brain becomes more than storage plus search.

## Representation Bias

This layer should be designed as canonical semantic infrastructure for machine
consumption first.

That means the metadata model should prefer:

- typed objects
- typed relationships
- explicit confidence and status
- provenance-backed claims
- reusable machine-readable structure

It should not be optimized primarily around:

- presentation-specific blobs
- loose tag piles
- dashboard-friendly prose as the canonical record
- UI-shaped summaries replacing semantic structure

Human-readable views still matter, but they should usually be projections built
on top of the canonical metadata rather than the canonical metadata itself.

## Core Principle

Derived metadata should be:

- typed
- evidence-backed
- confidence-aware
- reprocessable
- selective rather than indiscriminate

The system should not generate a giant undifferentiated pile of tags and vague
"related" links.

## Metadata Zones

The current recommended design has four main zones:

1. basic enrichment
2. knowledge graph
3. durable memory
4. retrieval support

## First-Class Metadata Objects

The recommended starting set is:

- `Classification`
- `Entity`
- `EntityMention`
- `Relationship`
- `Memory`
- `Summary`
- `RetrievalFeature`

These are intended to be broad enough for multiple source types while still
being small enough to model clearly.

They should be treated as semantic building blocks rather than UI-oriented data
structures.

## 1. Classification

Classification is lightweight labeling applied to an artifact or segment.

Examples:

- `brainstorm`
- `decision`
- `question`
- `reading-discussion`
- `project-note`
- `reference-material`
- `personal-preference`

Classifications help with:

- filtering
- browsing
- ranking
- deciding what extraction workflows to run

Suggested fields:

- `classification_id`
- `target_type`
- `target_id`
- `classification_type`
- `classification_value`
- `origin_kind`
- `confidence_score`
- `created_at`
- `status`

Examples:

- artifact classified as `brainstorm`
- segment classified as `decision_statement`

## 2. Entity

An entity is a canonical thing in the world or in the user's mental landscape.

Examples:

- person
- project
- company
- tool
- book
- author
- character
- theme
- location
- concept

Entities are nodes in the brain's knowledge graph.

Suggested fields:

- `entity_id`
- `entity_type`
- `canonical_name`
- `normalized_name`
- `description`
- `status`
- `created_at`

Examples:

- `Book`: "The Lies of Locke Lamora"
- `Character`: "Locke Lamora"
- `Project`: "OpenArchive"
- `Tool`: "ChatGPT"

## 3. EntityMention

Entity mentions ground canonical entities in real artifact segments.

This is the bridge between source evidence and higher-order context.

Examples:

- a chat message mentions a specific book
- a note mentions a specific project
- a pasted article mentions a person or company

Suggested fields:

- `entity_mention_id`
- `entity_id`
- `segment_id`
- `mention_text`
- `mention_role`
- `origin_kind`
- `confidence_score`
- `created_at`

`mention_role` could later distinguish:

- primary subject
- secondary mention
- comparison target
- author
- participant

## 4. Relationship

Relationships are typed edges between meaningful objects.

This is the heart of context.

Relationships should be explicit and typed, not vague.

Bad example:

- `related_to`

Better examples:

- `artifact_mentions_entity`
- `artifact_about_entity`
- `artifact_follow_up_to_artifact`
- `artifact_compares_entity_to_entity`
- `entity_appears_in_entity`
- `entity_written_by_entity`
- `memory_supported_by_artifact`
- `entity_similar_to_entity`
- `artifact_same_project_as_artifact`
- `artifact_contradicts_artifact`

Suggested fields:

- `relationship_id`
- `subject_type`
- `subject_id`
- `relationship_type`
- `object_type`
- `object_id`
- `origin_kind`
- `confidence_score`
- `created_at`
- `status`

Relationships should often be backed by `EvidenceLink` objects from the
provenance model.

## 5. Memory

Memory is durable distilled knowledge worth reusing across future workflows.

Memories should be higher-value than ordinary labels.

Examples:

- user preference
- stable fact
- recurring goal
- unresolved question
- important decision
- taste pattern

Examples from book-oriented use:

- user likes political intrigue
- user dislikes slow pacing
- user is interested in comparing character archetypes across books

Suggested fields:

- `memory_id`
- `memory_type`
- `memory_text`
- `scope_type`
- `scope_id`
- `origin_kind`
- `confidence_score`
- `created_at`
- `status`
- `supersedes_memory_id`

`scope_type` might later include:

- user
- project
- entity
- topic

Design rule:

Not every observation should become a memory.

Memories should be selective, durable, and evidence-backed.

## 6. Summary

Summary is a narrative or structured condensation of one or more artifacts.

Examples:

- conversation summary
- topic summary
- reading-note summary
- project-state summary

Suggested fields:

- `summary_id`
- `summary_type`
- `target_type`
- `target_id`
- `summary_text`
- `origin_kind`
- `confidence_score`
- `created_at`
- `status`

Summaries can be useful for browsing and context assembly, but they should
remain derived objects rather than replacing raw source material.

They also should not become a substitute for underlying machine-readable
objects such as entities, relationships, and memories.

## 7. RetrievalFeature

Retrieval features are metadata that primarily exist to improve later search and
ranking.

Examples:

- embedding vector reference
- salience score
- importance score
- recency weight
- keyword features
- user-pinned score

Suggested fields:

- `retrieval_feature_id`
- `target_type`
- `target_id`
- `feature_type`
- `feature_value_ref`
- `created_at`
- `status`

Some retrieval features may be persisted permanently. Others may be refreshable
or cache-like depending on cost and value.

## Relationship Families

The relationship model should support multiple families of relatedness.

### Source relationships

Examples:

- same import batch
- same source system
- same originating thread or export

### Temporal relationships

Examples:

- near in time
- before or after a decision
- same reading period

### Entity relationships

Examples:

- person works on project
- character appears in book
- book written by author
- artifact mentions entity

### Project and topic relationships

Examples:

- artifact belongs to project
- artifact about topic
- artifact extends prior brainstorming

### Semantic relationships

Examples:

- artifact semantically similar to artifact
- entity similar to entity
- artifact compares entity to entity
- artifact contradicts artifact

### Memory relationships

Examples:

- artifact supports memory
- memory updated by artifact
- memory related to entity

### User-curated relationships

Examples:

- manually linked
- pinned together
- grouped into same collection

## Trust Levels

Derived metadata should not all carry the same trust level.

The metadata layer should distinguish:

- deterministic metadata
- hybrid or low-risk extracted metadata
- AI-inferred metadata
- user-confirmed metadata

### Mostly deterministic

Examples:

- source type
- timestamps
- participants when explicit
- message boundaries
- URL extraction
- import batch membership

### Hybrid or low-risk extracted

Examples:

- title candidates
- language detection
- OCR and transcription
- basic named entity extraction
- keyword extraction

### Mostly AI-inferred

Examples:

- topic classification
- theme extraction
- preference inference
- relationship inference
- decision extraction
- summary generation
- memory candidate generation

### User-confirmed

Examples:

- approved memory
- corrected entity identity
- curated project assignment
- manually confirmed relationship

## Storage Discipline

The metadata layer can grow very quickly if not controlled.

The goal is not to avoid volume entirely. The goal is to avoid storing
low-value interpretation with the same permanence as high-value knowledge.

Recommended discipline:

- persist strong, reusable metadata
- store weak candidates selectively or temporarily
- avoid persisting every intermediate model output
- avoid vague all-to-all similarity edges
- prefer top-N strong relationships over dense graph explosion
- store semantic objects as the canonical layer and generate human-facing
  narratives as projections on top

### Good candidates for durable storage

- canonical entities
- grounded entity mentions
- important classifications
- strong typed relationships
- durable memories
- important summaries

### Better as cache or rebuildable data

- ad hoc context-pack rankings
- low-confidence similarity edges
- one-off temporary clustering results
- intermediate extraction drafts

## Why This Model Fits The Project

This model supports the broader OpenArchive goal of a user-owned brain because
it allows the system to represent:

- what an artifact is
- what it mentions
- what it is about
- what it means in relation to other artifacts
- what durable memory should be carried forward
- what context should be retrieved later

It is also flexible enough for mixed source types rather than only AI chats.

It also supports a split future where:

- humans inspect readable projections
- machines consume compact semantic structure

without forcing one representation to serve both equally well.

## Example: Book-Oriented Context

Imagine the user has multiple chats and notes about books.

Possible metadata:

- `Entity`
  - book: "The Name of the Wind"
- `Entity`
  - character: "Kvothe"
- `Entity`
  - theme: "gifted protagonist"
- `Classification`
  - artifact classified as `reading-discussion`
- `Relationship`
  - artifact `about` book
- `Relationship`
  - character `appears_in` book
- `Relationship`
  - artifact `compares` one book to another
- `Memory`
  - user likes intricate worldbuilding
- `Memory`
  - user dislikes slow pacing

This is the kind of structure that later enables:

- book recommendation context
- cross-book character comparison
- retrieval of prior reading reactions
- synthesis of the user's taste profile

## Design Rules

- keep metadata typed and explicit
- ground metadata in artifacts and segments via provenance
- distinguish canonical entities from local mentions
- store relationships as typed edges, not vague similarity blobs
- be selective about what becomes durable memory
- separate trust levels clearly
- control metadata growth through retention and confidence discipline
- keep the canonical metadata layer machine-first and build human views on top

## Open Follow-On Questions

The next metadata design pass should answer:

1. what entity types are needed in the first version
2. what relationship types are strong enough to support now
3. what qualifies as durable memory instead of just a classification
4. which retrieval features should be persisted versus recomputed
