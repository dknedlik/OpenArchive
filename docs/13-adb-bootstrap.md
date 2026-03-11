# Historical ADB Bootstrap

## Status

This document is historical reference only.

OpenArchive is no longer using Oracle ADB as the default bootstrap path for
slice one. The mainline direction is Docker Compose plus Postgres and a local
filesystem-backed object store.

## Why Keep This Document

The Oracle/OCI bootstrap work may still be useful later for:

- a personal OCI deployment
- a remote MCP deployment variant
- migration experiments outside the open source default path

Until that work is resumed, do not treat this document as current setup
guidance.
