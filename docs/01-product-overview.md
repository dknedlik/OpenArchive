# OpenArchive Product Overview

## Status

This document captures the current product hypothesis, not a settled product
definition.

## Working Thesis

AI work is increasingly fragmented across tools. Valuable knowledge ends up
trapped inside chat sessions, export formats, browser tabs, and agent-specific
memory systems.

OpenArchive may be a good vehicle for preserving and reusing that knowledge
without requiring a new chat client.

The strongest idea so far is to build a memory substrate beside existing tools,
not a replacement for them.

## Candidate Job To Be Done

When a user has important AI conversations spread across multiple tools, they
need one place to archive, search, summarize, and reuse those conversations
across future tools and agents.

## Candidate Scope

OpenArchive may eventually:

- ingest chat exports or captured transcripts from multiple AI tools
- normalize them into a canonical internal model
- retain raw source fidelity for audit and reprocessing
- extract structured metadata and reusable memories
- provide search and retrieval through an API
- support later integrations like remote MCP

OpenArchive probably should not initially:

- become a full end-user chat client
- require direct replacement of existing AI tools
- depend on one model provider or one agent runtime

## Working Principles

- Raw transcript preservation seems essential
- Derived metadata should be versioned and reproducible
- Tool-specific ingestion should stay separate from the canonical archive model
- Search likely matters before advanced agent integration exists
- Memory extraction is only trustworthy if provenance remains attached

## Likely Initial Users

- a single power user archiving their own AI work
- later, a small team with shared archives and permissions

## Near-Term Outcomes To Validate

- import a chat export into canonical storage
- browse or query archived threads by source, date, tag, and full text
- generate concise summaries and extracted memory with source grounding
- retrieve relevant prior threads for a new agent workflow
