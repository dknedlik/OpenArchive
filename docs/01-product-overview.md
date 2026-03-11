# OpenArchive Product Overview

## Status

This document captures the current product hypothesis, not a settled product
definition.

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

## Candidate Scope

OpenArchive may eventually:

- ingest chat exports or captured transcripts from multiple AI tools
- normalize them into a canonical internal model
- retain raw source fidelity for audit and reprocessing
- extract structured metadata and reusable memories
- provide machine-oriented retrieval through MCP-first interfaces
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

## Near-Term Outcomes To Validate

- import a ChatGPT export into canonical storage
- preserve raw payloads outside the relational database
- query archived artifacts through a local MCP server
- run enrichment asynchronously with a database-backed job queue
- support optional local inference for users who want GPU-backed enrichment
