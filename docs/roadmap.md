# OpenArchive Roadmap

## V1 Completed

OpenArchive V1 proved the core local-first system shape and made it usable:

- import and preserve real source payloads
- persist a canonical archive in Postgres
- keep raw payloads in object storage
- run a durable enrichment pipeline
- expose retrieval through MCP
- support archive writeback through MCP
- support embedding-backed search for derived objects when configured

The mainline question is no longer "can this architecture work?" The answer is
yes.

## Next Priorities

### Broader Import Coverage

The current import surface is still narrow.

Priority areas:

- more AI export types
- non-chat artifacts such as notes, markdown, and documents
- cleaner normalization rules across heterogeneous sources

### Human Interface And Curation

The archive can already be written to and corrected through MCP, but it lacks a
strong human-facing workflow.

Priority areas:

- browse and inspect archive state
- review low-quality or conflicting derived objects
- prune, merge, reject, and supersede bad data
- edit user-authored or agent-authored archive state safely

### Retrieval Quality

The system now has the right basic surfaces, but quality still needs work.

Priority areas:

- better ranking across titles, segments, and derived objects
- better context-pack assembly
- stronger reconciliation against prior archive state
- more disciplined evaluation and regression checks

### Product And Operator Ergonomics

The current setup works, but it still feels like an engineering system rather
than a polished product.

Priority areas:

- first-run setup that is easier than hand-editing `.env`
- clearer local and remote deployment paths
- better observability for import and enrichment state
- cleaner client integration guidance for MCP consumers

## Non-Goals For The Next Phase

The next phase should not throw away the core architecture in pursuit of
surface polish.

Keep:

- synchronous Rust core
- explicit provider seams
- durable job-based enrichment
- separation of relational state and object storage
- MCP as an adapter over application services
