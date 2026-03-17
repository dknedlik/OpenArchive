# OpenArchive Product Overview

## Status

The architecture proof phase is complete. OpenArchive is now in the MVP build
phase, focused on making retrieval and MCP genuinely usable instead of only
proving the storage and enrichment foundations.

## Working Thesis

AI work is increasingly fragmented across tools. Valuable knowledge ends up
trapped inside chat sessions, export formats, browser tabs, and agent-specific
memory systems.

OpenArchive should preserve and reuse that knowledge without requiring a new
chat client.

The strongest idea is to build a machine-first archive and memory substrate
beside existing tools, not a replacement for them.

## Candidate Job To Be Done

When a user has important AI conversations and related artifacts spread across
multiple tools, they need one place to ingest, preserve, enrich, query, and
reuse those artifacts across future tools and agents.

## Product Direction

OpenArchive should:

- ingest chat exports or captured transcripts from multiple AI tools
- normalize them into a canonical internal model
- retain raw source fidelity for audit and reprocessing
- extract structured metadata and reusable memories
- provide machine-oriented retrieval, search, and context packs through
  MCP-first interfaces
- support both local and remote deployment shapes

OpenArchive probably should not initially:

- become a full end-user chat client
- require direct replacement of existing AI tools
- depend on one model provider, one database, or one hosting target

## Working Principles

- Raw artifact preservation is essential
- Derived metadata should be versioned and reproducible
- Tool-specific ingestion should stay separate from the canonical archive model
- The application core should be transport-agnostic
- MCP can be the primary external interface without making the core
  MCP-protocol-shaped
- Enrichment should be asynchronous and durable
- Memory extraction is only trustworthy if provenance remains attached

## Likely Initial Users

- a single power user archiving their own AI work locally
- later, a small team with shared archives and permissions
- contributors who want an open source memory/archive system that runs with
  modest local setup

## Near-Term Product Outcomes

- make archive search, artifact detail, and artifact-context retrieval usable
  as real product surfaces
- search the archive without dumping every artifact to the client
- return compact, useful context packs for downstream agents
- make local MCP the practical way to use the archive day to day
- keep the same application-layer retrieval use cases usable through local MCP,
  remote MCP, and a thin HTTP surface
- broaden import coverage beyond the initial ChatGPT-export path
- support optional local inference for users who want GPU-backed enrichment
