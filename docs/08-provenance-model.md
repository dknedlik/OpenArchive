# OpenArchive Provenance Model

## Status

This is the first-pass draft of the provenance model for the OpenArchive brain
layer. It defines how the system should keep stored and derived knowledge
grounded, explainable, and reprocessable over time.

## Purpose

The provenance model answers questions like:

- where did this come from
- what exact source supports it
- how was it transformed
- what process created it
- how trustworthy is it
- what changed later

Without provenance, the system becomes a pile of AI-generated assertions with
weak grounding. With provenance, the system can preserve trust while still
using AI heavily for enrichment and analysis.

## Representation Bias

This layer should be machine-usable first and human-auditable second.

That means provenance should be stored as explicit structured references rather
than primarily as explanatory prose.

In practice, that means:

- addressable segment identifiers
- typed evidence links
- structured origin kinds
- explicit derivation run records
- machine-readable lineage between source and derived objects

Human inspection still matters, but it should usually be a rendering of this
structured provenance rather than the provenance model itself.

## Core Principle

Every derived object should be traceable back to preserved source material.

That means:

- summaries
- classifications
- extracted entities
- extracted relationships
- memories
- recommendations
- context packs

should all be explainable in terms of source artifacts and the transformations
applied to them.

## Provenance Layers

The current model should be thought of as five linked layers:

1. source artifact
2. source segment
3. derivation run
4. derived object
5. evidence link

### 1. Source artifact

This is the top-level stored object from the artifact model.

Examples:

- a chat export
- a note
- a screenshot
- a saved web page

This is the root of provenance.

### 2. Source segment

This is an addressable sub-part of an artifact.

Examples:

- a message in a conversation
- a paragraph in a note
- a chunk in a document
- a page span in a PDF
- an OCR text span in an image
- a timestamped transcript segment in audio

Segment-level provenance is important because artifact-level provenance is often
too coarse to be useful.

It is also important because machine consumers need explicit, addressable
anchors rather than broad human-style citations.

### 3. Derivation run

This records how some derived output was created.

Examples:

- summary generation run
- entity extraction run
- relationship extraction run
- memory extraction run
- retrieval assembly run

It should record:

- what inputs were used
- what pipeline or process produced the output
- what version of that process was used
- what model and prompt were used if AI was involved
- when it ran

This information should be structured enough for later machine reasoning,
reprocessing, and debugging.

### 4. Derived object

A derived object is any non-raw artifact created by the system.

Examples:

- summary
- classification
- entity mention
- relationship
- memory candidate
- user preference inference
- context pack

Derived objects should be first-class records, not just transient outputs.

### 5. Evidence link

This connects a derived object back to one or more supporting source segments.

This is where the system says:

- this summary sentence came from these chunks
- this inferred preference came from these chat messages
- this relationship was inferred from these two artifacts

Evidence links are what make the whole system auditable.

They are also what make the system machine-groundable rather than merely
human-explainable.

## Root Rule

Raw source is the root of truth.

The system may derive many interpretations, but it should not lose the original
artifact or confuse an interpretation with a source fact.

This is the strongest guardrail in the model.

It is also what prevents downstream machine consumers from treating derived
objects as unsupported truth.

## Segment-Level Grounding

The system should support provenance below the artifact level wherever
possible.

Recommended segment types:

- message
- paragraph
- section
- page span
- chunk
- OCR span
- transcript span

The exact segmentation method can vary by artifact class, but the idea stays
the same: derived outputs should point to the smallest useful evidence anchor.

This should be optimized for machine addressability as much as for human review.

## Explicit Versus Inferred

Not all knowledge in the brain should be treated equally.

The provenance model should distinguish at least these origin kinds:

- `explicit`
- `deterministic`
- `inferred`
- `user_confirmed`

### `explicit`

Directly stated in source material.

Example:

- "I loved the political intrigue in this book."

### `deterministic`

Produced by a non-interpretive transformation or extraction rule.

Examples:

- parsing a timestamp
- extracting a URL
- identifying a message boundary

### `inferred`

Generated by AI or another probabilistic process.

Examples:

- inferring that the user likes morally complex fantasy
- inferring that two characters are similar
- inferring a likely project topic

### `user_confirmed`

A user has reviewed and accepted the item or inference.

This should carry more trust than a purely inferred result.

## Trust And Confidence

Provenance is not just about lineage. It is also about trust.

Derived objects should carry fields such as:

- `origin_kind`
- `confidence_score`
- `confidence_label`
- `evidence_count`
- `created_at`
- `supersedes_derived_object_id`

This supports reasoning like:

- explicitly stated facts rank above inferred preferences
- user-confirmed memories rank above unreviewed memory candidates
- relationships with multiple evidence links are stronger than ones with only a
  single weak source

## Change Over Time

The provenance model should support revision and supersession.

Later evidence may:

- strengthen an earlier inference
- weaken it
- contradict it
- replace it entirely

That means derived data should not usually be overwritten silently.

Instead, the system should support:

- new derived object versions
- links to the objects they supersede
- retention of older outputs for audit or comparison

This is especially important for:

- preferences
- memories
- classifications
- summaries

## Retrieval Provenance

Provenance should also exist for context retrieval, not just stored metadata.

When the system creates a context pack, it should be possible to answer:

- which artifacts were selected
- which segments were selected
- why they were selected
- what ranking signals were used
- what trimming or summarization happened

This matters because retrieval errors can look like reasoning errors unless the
selection path is visible.

For a machine-first system, retrieval provenance is not optional debugging
metadata. It is part of the usable output contract.

## Recommended Provenance Objects

The current recommended object set is:

- `Artifact`
- `Segment`
- `DerivationRun`
- `DerivedObject`
- `EvidenceLink`

### `Artifact`

Top-level source or normalized stored object from the artifact model.

### `Segment`

Addressable unit inside an artifact.

Suggested fields:

- `segment_id`
- `artifact_id`
- `segment_type`
- `sequence_no`
- `locator`
- `text_content`
- `created_at`

`locator` should capture how to find the segment inside the artifact, such as:

- message index
- paragraph range
- page/chunk coordinates
- transcript timestamps

### `DerivationRun`

Represents one execution of a derivation pipeline.

Suggested fields:

- `derivation_run_id`
- `run_type`
- `pipeline_name`
- `pipeline_version`
- `model_name`
- `prompt_version`
- `started_at`
- `completed_at`
- `status`

Optional input references:

- source artifact ids
- source segment ids
- retrieval query or retrieval result set for context-pack assembly

### `DerivedObject`

Represents one first-class derived output.

Suggested fields:

- `derived_object_id`
- `derived_object_type`
- `artifact_id` if scoped to one artifact
- `derivation_run_id`
- `origin_kind`
- `confidence_score`
- `created_at`
- `supersedes_derived_object_id`
- `status`

### `EvidenceLink`

Connects a derived object to supporting segments.

Suggested fields:

- `evidence_link_id`
- `derived_object_id`
- `segment_id`
- `evidence_role`
- `evidence_rank`
- `support_strength`

`evidence_role` could later distinguish things like:

- primary support
- secondary support
- contrast
- contradiction

## Example

Imagine a chat artifact where a user says:

"I liked the political intrigue, but the pacing dragged."

Possible provenance chain:

- `Artifact`
  - conversation about a book
- `Segment`
  - one message containing that sentence
- `DerivationRun`
  - reading-preference extraction pipeline version 2
- `DerivedObject`
  - preference: likes political intrigue
- `DerivedObject`
  - preference: dislikes slow pacing
- `EvidenceLink`
  - both preference objects linked to the specific message segment

Later, a second artifact may add more evidence or contradict the earlier one.
That later derivation should create new derived objects or updated versions
rather than silently mutating the earlier record.

## Design Rules

- raw source remains the root of trust
- preserve provenance at segment level where possible
- treat AI outputs as derived, not canonical truth
- distinguish explicit, deterministic, inferred, and user-confirmed origins
- record derivation runs as first-class objects
- keep evidence links typed and inspectable
- avoid silent overwrites of important derived data
- capture retrieval provenance for context packs
- keep provenance machine-readable and addressable by default

## Why This Model Fits The Project

This model is important for OpenArchive because the project aims to support:

- multiple source types
- AI-assisted categorization and memory extraction
- cross-artifact relationships
- retrieval for future AI workflows

All of that becomes unreliable without strong grounding.

The provenance model is what makes the brain explainable enough to trust.

## Open Follow-On Questions

The next provenance design pass should answer:

1. what segment types are required for each artifact class
2. which derived object types deserve first-class tables or records
3. how contradictions and invalidations should be represented
4. how much provenance should be stored for context-pack generation
