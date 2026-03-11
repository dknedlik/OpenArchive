# OpenArchive Artifact Model

## Status

This is the first-pass draft of the artifact model for the OpenArchive brain
layer. It is intended to be stable enough to guide later schema design, but it
should still be treated as a working draft.

## Purpose

The artifact model defines the top-level objects the brain stores.

It should be:

- broad enough to support more than AI chats
- small enough to stay stable as new source types are added
- structured enough to support retrieval, analysis, and context reuse

The design goal is to avoid both extremes:

- too little structure, such as only `text` and `binary`
- too much structure, such as a giant type hierarchy for every input source

## Representation Bias

This layer should stay simpler than the metadata and context layers, but it
should still lean toward machine-usable structure rather than human file-cabinet
metaphors.

That means artifact classes should help answer questions like:

- how should this be segmented
- what extraction workflows apply
- what retrieval strategies make sense
- what kinds of provenance anchors are available

This layer should not be optimized primarily around how an artifact "looks" in
a human UI.

## Modeling Approach

The recommended approach has three layers:

1. artifact class
2. source type
3. content facets

### Artifact class

This is the broad behavioral category for the stored object.

Artifact class should answer:

"What kind of thing is this in the brain?"

### Source type

This identifies where the artifact came from or how it entered the system.

Source type should answer:

"What external system or capture path produced this?"

### Content facets

These describe the kinds of usable content the artifact contains.

Content facets should answer:

"What can the brain do with this content?"

This is intentionally a machine-oriented question.

## Top-Level Artifact Classes

The current recommended set is:

- `conversation`
- `document`
- `note`
- `capture`
- `media`
- `collection`

These should be treated as broad, durable categories rather than narrow source
types.

### `conversation`

Used for turn-based or message-oriented artifacts.

Examples:

- ChatGPT export
- Claude thread
- Codex thread
- pasted chat transcript
- voice transcript of a dialogue later

Behavioral properties:

- contains ordered turns or messages
- usually has participants
- often benefits from message-level provenance
- often produces summaries, decisions, and extracted memories

### `document`

Used for authored or imported long-form content.

Examples:

- PDF
- markdown note file
- exported report
- saved article or web page
- design document

Behavioral properties:

- primarily read as a continuous artifact
- may be chunked for retrieval
- may have sections, headings, or pages
- often treated as reference material
- should preserve enough internal structure for machine segmentation and
  extraction

### `note`

Used for user-authored freeform content that is usually smaller and more direct
than a formal document.

Examples:

- brainstorm
- quick outline
- project notes
- personal reminder with context

Behavioral properties:

- usually text-first
- often created directly in the system
- may later be promoted into a document or linked to other artifacts
- should remain easy to capture while still being segmentable later

### `capture`

Used for raw intake items that arrive quickly and may not yet be well
understood.

Examples:

- pasted snippet
- shared URL
- clipboard dump
- screenshot with no further classification yet
- imported file or raw payload from a basic intake flow

Behavioral properties:

- inbox-oriented
- may be incomplete or messy
- should preserve raw content quickly
- can later be normalized or promoted into another class
- is intentionally biased toward fast intake first and machine interpretation
  later

### `media`

Used when image, audio, or video is the primary object.

Examples:

- screenshot
- voice memo
- video clip
- photo of a whiteboard

Behavioral properties:

- binary-first artifact
- may have derived text from OCR or transcription
- often needs a media-specific preview and extraction pipeline
- should retain machine-usable derived text or transcript sidecars where
  possible

### `collection`

Used for grouping or container objects.

Examples:

- import batch
- project bundle
- source archive
- logical grouping of related artifacts

Behavioral properties:

- contains or references other artifacts
- supports organization and navigation
- may be user-defined or system-generated

## Source Type

`source_type` should be more specific than `artifact_class`.

Examples:

- `chatgpt_export`
- `claude_thread`
- `codex_thread`
- `manual_paste`
- `quick_capture`
- `shared_url`
- `pdf_upload`
- `screenshot`
- `voice_memo`
- `ios_share_sheet`

Design rule:

New source types should usually not require a new artifact class.

This helps keep the artifact model stable while letting the machine-oriented
processing layers evolve more freely.

## Content Facets

Facets describe the usable content forms present in the artifact.

Examples:

- `text`
- `messages`
- `image`
- `audio`
- `video`
- `ocr_text`
- `transcript`
- `attachments`
- `links`
- `participants`
- `timestamps`
- `structured_fields`

Artifacts may have multiple facets.

Examples:

- a chat export may have `messages`, `text`, `participants`, and `timestamps`
- a screenshot may have `image` and `ocr_text`
- a saved web page may have `text`, `links`, and `structured_fields`

## Universal Artifact Fields

Every artifact should have a common set of fields, regardless of class.

Suggested universal fields:

- `artifact_id`
- `artifact_class`
- `source_type`
- `title`
- `created_at`
- `captured_at`
- `raw_payload_ref`
- `content_hash`
- `language`
- `content_facets`
- `status`
- `normalization_version`

### Field notes

- `artifact_id`
  - stable internal identifier
- `artifact_class`
  - broad type such as `conversation` or `document`
- `source_type`
  - ingestion origin such as `chatgpt_export` or `shared_url`
- `title`
  - user-provided, source-provided, or AI-suggested
- `created_at`
  - source creation time if known
- `captured_at`
  - time it entered OpenArchive
- `raw_payload_ref`
  - pointer to the preserved source payload
- `content_hash`
  - supports deduplication and idempotent imports
- `language`
  - primary language if detectable
- `content_facets`
  - list of usable content modes
- `status`
  - lifecycle status such as `captured`, `normalized`, `enriched`, `failed`
- `normalization_version`
  - version of the canonicalization logic used

## Promotion And Normalization Rules

Not every incoming item needs to be classified perfectly at the moment of
capture.

Recommended rule:

When uncertain, ingest as `capture` first.

Then later:

- normalize it
- infer stronger classification
- promote it to another artifact class if justified

Examples:

- a raw pasted transcript starts as `capture`, then becomes `conversation`
- a shared web page starts as `capture`, then becomes `document`
- a screenshot starts as `media`, with a derived OCR text sidecar

Promotion should not destroy the original raw record.

## Child Structures

Top-level artifact class is not enough on its own. Some classes require
class-specific internal structure.

### Conversation child structures

Likely needs:

- participants
- messages or turns
- optional thread/parent relationships
- message-level timestamps
- message-level evidence anchors

### Document child structures

Likely needs:

- chunks or sections
- page/heading boundaries when available
- extracted references and links

### Note child structures

Likely needs:

- body text
- optional checklist/outline structure later

### Media child structures

Likely needs:

- binary object reference
- extracted OCR text and/or transcript
- media metadata such as duration or dimensions

### Collection child structures

Likely needs:

- artifact membership table
- ordering or grouping metadata

## Design Rules

- prefer a small number of stable artifact classes
- add new `source_type` values more freely than new artifact classes
- keep content facets composable
- preserve raw payloads even when normalized structures exist
- allow imperfect intake classification
- use promotion and reprocessing rather than forcing perfect up-front filing
- prefer structures that support later segmentation, extraction, and retrieval
  cleanly

## Why This Model Fits The Project

This model works for the broader "brain" direction because it can handle:

- AI chats as a primary early source
- quick mobile captures
- notes and documents
- screenshots and media
- later expansion into other personal knowledge artifacts

It also keeps the system from being overfit to a single source like chat
exports.

## Open Follow-On Questions

The next artifact-model design pass should answer:

1. what exact fields belong on top-level `Artifact`
2. what child tables or substructures each artifact class needs
3. how promotion history should be represented
4. how collections relate to projects, topics, or user-defined organization
