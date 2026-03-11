# OpenArchive Brain Overview

## Status

This document captures the current working understanding of the brain layer for
OpenArchive. It is a jumping-off point, not a finished architecture spec.

## Core Thesis

OpenArchive should be built around a durable brain layer first, with access
layers added on top.

The brain is the stable asset.

Access methods will change over time:

- local MCP
- remote MCP
- CLI
- later HTTP or web surfaces if useful

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
active brainstorming already happens.

## Working Architectural Shape

The current working shape is:

- brain layer first
- transport adapters second

Brain layer:

- source ingestion pipeline
- durable relational storage for canonical records and provenance
- durable object storage for raw payloads and other large objects
- derived enrichment pipeline
- retrieval and context assembly services

Transport adapters:

- local MCP
- remote MCP
- CLI
- later HTTP if it earns its keep

## Storage Direction

The working storage direction is now:

- Postgres for structured and queryable archive data
- local filesystem-backed object storage for raw payloads in slice one
- object-store abstraction from day one so S3-compatible storage can follow
  cleanly

The key principle is simple:

- relational state and large object storage are separate concerns

## Enrichment Direction

Enrichment should be asynchronous, durable, and provider-driven.

That means:

- database-backed job coordination
- worker processes that can run locally or elsewhere
- optional local inference through Ollama
- provider-specific model details kept below the application boundary

## Current Summary

The direction is not "build a chat archive."

It is:

- build a user-owned brain layer
- start with AI chats as the primary source
- preserve raw data and provenance
- enrich and organize the data asynchronously
- retrieve and assemble machine-usable context
- expose that capability through MCP-first interfaces
