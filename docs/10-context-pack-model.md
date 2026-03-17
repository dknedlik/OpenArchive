# OpenArchive Context-Pack Model

## Status

This is the first-pass draft of the context-pack model for the OpenArchive
brain layer. It describes how the system should assemble usable working context
from artifacts, provenance, and derived metadata for a specific task.

The slice-one term `conversation_resume` appears in older planning documents.
For the MVP product phase, the preferred framing is `artifact_context_pack`
for the first concrete retrieval contract, with narrower pack types layered on
later if needed.

The current MVP implementation is narrower than the broader design space in
this document. The live artifact-context-pack contract is already implemented
and returned by the app layer and local MCP surface.

## Purpose

The artifact model defines what exists.

The provenance model defines where it came from and how it is grounded.

The metadata model defines what it means and how it relates to other things.

The context-pack model defines what the brain hands back to a consumer right
now.

This is the working-memory layer of the system.

## Representation Bias

This layer should be strongly machine-first.

The most common consumer of context packs is expected to be another AI system,
agent, tool, or API client rather than a human browsing a screen.

That means context packs should prefer:

- compact machine-readable structure
- explicit task framing
- typed semantic content
- supporting evidence
- confidence and uncertainty signals
- predictable shape

Human-readable renderings may exist later, but they should usually be
projections built on top of the pack rather than the primary representation.

## Core Principle

A context pack is not the archive.

A context pack is a task-shaped bundle of the most relevant grounded context for
one current use.

The pack should not be:

- a blind top-k dump of chunks
- a giant copy of source artifacts
- a vague summary with no evidence

The pack should be:

- scoped
- selective
- structured
- explainable
- rebuildable

## Mental Model

The broader brain can be understood as:

- episodic memory
  - artifacts and segments
- semantic memory
  - entities, relationships, classifications, memories
- working memory
  - context packs

Context packs are the system's working-memory output for the current task.

## What A Context Pack Must Do

A context pack should answer:

- what is the current task
- what prior knowledge is relevant
- what evidence supports that knowledge
- what uncertainty or contradiction exists
- what compact structure should be handed to the consumer

## Pack Structure

The recommended first-pass model has six parts:

1. request frame
2. retrieved evidence set
3. distilled semantic context
4. consumer-oriented structure
5. provenance envelope
6. policy and budget

## Current MVP Pack Shape

The current implemented `artifact_context_pack` is intentionally compact and
artifact-scoped.

It returns:

- artifact identity and title
- source type
- readiness:
  - `not_ready`
  - `partial`
  - `ready`
- grouped derived entries:
  - summaries
  - classifications
  - memories
  - relationships
- evidence excerpts for each derived entry
- pack provenance:
  - total segments
  - referenced segments
  - whether enrichment is still incomplete
  - omission counts for segments with no evidence links

Readiness is currently derived from the artifact's persisted
`enrichment_status` plus whether the minimum useful derived output set exists,
not from evidence coverage ratio alone.

In other words:

- `referenced_segments < total_segments` is a provenance observation
- `not_ready` / `partial` / `ready` is a lifecycle-and-output judgment

## 1. Request Frame

The request frame captures the reason the pack exists.

Suggested fields:

- `context_pack_id`
- `pack_type`
- `request_intent`
- `request_text`
- `consumer_type`
- `scope_constraints`
- `created_at`
- `status`

Examples of `pack_type`:

- `artifact_context_pack`
- `decision_context`
- `project_context`
- `entity_context`
- `recommendation_context`
- `preference_context`
- `research_context`

Examples of `consumer_type`:

- `llm`
- `agent`
- `mcp_client`
- `api_client`
- `human_review`

`scope_constraints` might later include:

- time range
- source types
- artifact classes
- project or entity scope
- trust threshold
- token budget

## 2. Retrieved Evidence Set

This is the grounded source material selected for the task.

It should usually contain:

- artifact references
- segment references
- selected excerpts
- supporting summaries
- relevant memories
- relevant entities and relationships

The evidence set should be selective. It should not default to full raw
artifacts unless that is specifically required.

Suggested fields or sub-objects:

- `evidence_items`
- `artifact_ids`
- `segment_ids`
- `selected_excerpt`
- `selection_reason`
- `rank_score`

## 3. Distilled Semantic Context

This is the most useful part of the pack for machine consumers.

It should contain compact structured context such as:

- relevant entities
- relevant relationships
- relevant memories
- key classifications
- important summaries
- decisions
- open questions
- contradictions
- confidence or uncertainty markers

This is what turns retrieval into reusable context rather than just document
search.

Suggested sections:

- `facts`
- `memories`
- `entities`
- `relationships`
- `decisions`
- `open_questions`
- `uncertainties`

## 4. Consumer-Oriented Structure

The pack should have a predictable layout so consumers can rely on it.

An example high-level structure might be:

- `task`
- `relevant_background`
- `memories`
- `entities`
- `relationships`
- `supporting_evidence`
- `contradictions`
- `uncertainties`
- `provenance`

The exact shape may vary by `pack_type`, but the system should avoid producing
completely ad hoc payloads for each use case.

## 5. Provenance Envelope

The pack should preserve why included items are present.

This should capture:

- what was selected
- why it was selected
- what evidence anchors support it
- what ranking or filtering logic was used
- what summarization or trimming occurred

This is important both for trust and for debugging retrieval behavior.

Suggested provenance fields:

- `selection_strategy`
- `selection_reasons`
- `evidence_links`
- `retrieval_features_used`
- `omitted_due_to_budget`

## 6. Policy And Budget

Context assembly is constrained.

The system should track the rules and limits used for pack creation.

Examples:

- maximum token budget
- maximum evidence items
- recency bias enabled
- inferred memories allowed or disallowed
- contradiction inclusion required or optional
- trust threshold for inferred metadata

Suggested fields:

- `policy_name`
- `policy_version`
- `token_budget`
- `item_budget`
- `trust_threshold`
- `include_inferred`
- `include_contradictions`

## Pack Types

Different tasks should produce different kinds of context packs.

The current recommended starting set is:

- `artifact_context_pack`
- `decision_context`
- `project_context`
- `entity_context`
- `recommendation_context`
- `preference_context`
- `research_context`

### `artifact_context_pack`

Used to continue prior work around one artifact in a current interaction.

Likely contents:

- the current artifact
- recent relevant related artifacts
- key summaries
- open questions
- unresolved decisions
- supporting excerpts

Legacy note:

- `conversation_resume` was the slice-one planning name for this first pack
  shape and should be treated as historical terminology.

### `decision_context`

Used when evaluating or revisiting a decision.

Likely contents:

- prior decision statements
- rationale
- alternatives considered
- new contradictory evidence

### `project_context`

Used to resume work on a project or initiative.

Likely contents:

- current goals
- recent artifacts
- active memories
- pending questions
- linked entities and decisions

### `entity_context`

Used when the task is centered around a person, book, project, company, or
other named thing.

Likely contents:

- canonical entity record
- important mentions
- related entities
- relevant summaries
- related memories

### `recommendation_context`

Used when the system needs to recommend something based on prior knowledge.

Likely contents:

- durable preferences
- relevant prior experiences
- negative preferences
- related entities
- evidence excerpts showing why a preference exists

### `preference_context`

Used when a task depends heavily on user taste, habits, or stable preferences.

Likely contents:

- current preferences
- confidence levels
- supporting evidence
- contradictions or recent updates

### `research_context`

Used when comparing sources, ideas, or questions over time.

Likely contents:

- relevant artifacts
- topic summaries
- contradictions
- unresolved questions
- linked entities

## Lessons From Retrieval Systems

This layer should incorporate several lessons from RAG and adjacent retrieval
systems.

### What to do

- use hybrid retrieval rather than relying only on vector similarity
- use metadata filters before ranking when possible
- retrieve semantic memory and episodic evidence together
- keep evidence attached to higher-level claims
- shape the output to the task rather than dumping generic search results

### What to avoid

- naive top-k chunk stuffing
- pure vector search with no structured filters
- giant undifferentiated text bundles
- unsupported summaries with no evidence
- over-retrieval that dilutes the signal

## Context Pack Lifecycle

Context packs should usually be treated as generated artifacts rather than
canonical storage records.

Design guidance:

- most packs should be rebuildable
- some packs may be cached temporarily
- especially expensive or important packs may be persisted
- packs should not replace artifacts or semantic metadata as the source of
  truth

## Example: Book Recommendation Context

Imagine the user asks for a recommendation for a new book.

A `recommendation_context` pack might include:

- durable memories about taste
  - likes political intrigue
  - dislikes slow pacing
- entities
  - books and authors previously discussed
- summaries
  - brief summaries of prior reading discussions
- relationships
  - book compared to another book
  - character similar to another character
- evidence
  - excerpts from chat messages showing the user's reactions
- uncertainty
  - conflicting evidence if the user's preferences changed over time

That is far more useful than returning a handful of semantically similar chat
chunks with no structure.

## Design Rules

- treat context packs as task-specific working memory
- optimize for machine consumption first
- include semantic context and supporting evidence together
- expose uncertainty and contradiction explicitly
- make pack structure predictable
- keep packs selective and compact
- prefer rebuildability over permanent storage unless there is a strong reason
  to persist

## Open Follow-On Questions

The next context-pack design pass should answer:

1. which pack types matter in the first MVP
2. what the canonical JSON shape should be for machine consumers
3. how pack assembly should balance semantic memory and episodic evidence
4. which packs should be cached or persisted versus generated on demand
