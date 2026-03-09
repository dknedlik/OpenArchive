# OpenArchive Brainstorming Roadmap

This is a sequencing sketch, not a committed delivery plan.

## Phase 0: Framing

- document the problem and the non-goals
- decide whether the memory-substrate framing still holds up
- choose candidate canonical model boundaries
- keep stack choices provisional while de-risking major constraints

## Phase 1: Canonical Archive MVP Candidate

- create schema for artifacts, segments, provenance, and core derived metadata
- implement one ingestion path from a concrete source format
- persist raw source reference plus normalized rows
- expose a basic read API for artifact retrieval and context-oriented queries

Exit criteria:

- one real export can be imported end to end
- imported artifact data can be fetched from the API
- import is idempotent by source identity or content hash

## Phase 2: Search and Summary Candidate

- add metadata-aware and full-text retrieval
- generate stored summaries and useful semantic enrichments
- support filtering by source, date range, artifact type, and core metadata

Exit criteria:

- stored artifacts are retrievable without manually browsing raw source material

## Phase 3: Extracted Memory Candidate

- define memory types and evidence linkage
- extract reusable memories from imported artifacts
- retrieve memory items with provenance

Exit criteria:

- a new agent workflow can pull prior memory with source grounding

## Phase 4: Integration Surfaces Candidate

- remote MCP
- more ingestion adapters
- automation hooks

Exit criteria:

- OpenArchive can participate as shared memory across more than one tool

## Better Immediate Next Steps

1. capture the brainstorm as explicit hypotheses and open questions
2. choose one concrete import source to design around first
3. turn the brain-layer docs into a first-pass schema and ingestion pipeline
4. only then decide whether to start with schema, service code, or both
