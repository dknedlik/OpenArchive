# Historical Oracle ADB Notes

## Status

This document is historical reference only.

OpenArchive is no longer treating Oracle ADB as the default slice-one storage
path. The current working direction is local-first with Postgres plus a
filesystem-backed object store.

## Why Keep This Document

These notes may still matter later for a personal OCI deployment or a remote
MCP variant of the system.

The earlier spike established that:

- Rust can talk to Oracle ADB through `rust-oracle`
- the practical connection path used Oracle Instant Client plus wallet plus
  `TNS_ADMIN`
- local development against ADB was viable, but setup-heavy

## Current Interpretation

The Oracle/OCI work is no longer the mainline contributor path.

Treat it as:

- useful prior research
- relevant for future personal deployment work
- not the default architecture for the open source slice-one system
