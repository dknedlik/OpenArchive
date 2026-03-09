# OpenArchive

OpenArchive is the working name for a new project exploring a cloud-hosted
brain and memory/archive layer for AI-era personal data.

This repo exists because the idea outgrew the fitness tracker. It should be
treated as a brainstorming and incubation space, not as a project with fixed
product or technical decisions yet.

It is also intended to be an AI learning lab. The product and the learning work
are coupled on purpose: OpenArchive is both a possible "second brain" system
and a place to practice production-grade AI architecture, retrieval, memory,
evaluation, and interoperability patterns.

## Current Working Idea

- Build a user-owned brain layer rather than a new chat client
- Start with AI chats as the primary early source, but support broader artifact
  types over time
- Preserve raw source material while extracting structured, machine-usable
  meaning
- Support retrieval, context assembly, summarization, and durable memory reuse
  across tools and agents
- Likely start with a web/API layer and add MCP and other interfaces later

## Current Hypotheses, Not Decisions

- Rust is a plausible service language
- OCI is a plausible hosting target
- Reusing the existing Oracle Autonomous Database via a new schema looks like a
  strong default
- If Rust talks to ADB, the credible path appears to be `rust-oracle` with
  Oracle Instant Client, wallet, and `TNS_ADMIN`

The only thing that is materially de-risked so far is the ADB connectivity
question from the earlier spike.

## Initial Repo Contents

- [docs/01-product-overview.md](/Users/david/src/open_archive/docs/01-product-overview.md)
- [docs/02-architecture.md](/Users/david/src/open_archive/docs/02-architecture.md)
- [docs/03-data-model.md](/Users/david/src/open_archive/docs/03-data-model.md)
- [docs/04-adb-integration.md](/Users/david/src/open_archive/docs/04-adb-integration.md)
- [docs/05-roadmap.md](/Users/david/src/open_archive/docs/05-roadmap.md)
- [docs/06-brain-overview.md](/Users/david/src/open_archive/docs/06-brain-overview.md)
- [docs/07-artifact-model.md](/Users/david/src/open_archive/docs/07-artifact-model.md)
- [docs/08-provenance-model.md](/Users/david/src/open_archive/docs/08-provenance-model.md)
- [docs/09-derived-metadata-model.md](/Users/david/src/open_archive/docs/09-derived-metadata-model.md)
- [docs/10-context-pack-model.md](/Users/david/src/open_archive/docs/10-context-pack-model.md)
- [docs/15-AI-SYLLABUS.md](/Users/david/src/open_archive/docs/15-AI-SYLLABUS.md)
- [src/main.rs](/Users/david/src/open_archive/src/main.rs)

## Notes

The minimal Rust scaffold in this repo is just a placeholder so the project is
not empty. It is not a commitment to the final service structure.

The repository is also mirrored to OCI DevOps source control for backup:

- DevOps project: `oa-devops`
- hosted repository: `open-archive`
- HTTPS remote URL:
  `https://devops.scmservice.us-phoenix-1.oci.oraclecloud.com/namespaces/axrp48zlutag/projects/oa-devops/repositories/open-archive`

If you clone or reattach this workspace later, the expected backup remote is:

```bash
git remote add oci https://devops.scmservice.us-phoenix-1.oci.oraclecloud.com/namespaces/axrp48zlutag/projects/oa-devops/repositories/open-archive
git push -u oci main
```

Set the Oracle-related environment explicitly only if and when you revisit ADB
connectivity:

- `WALLET_DIR`
- `TNS_ALIAS`
- `DB_USERNAME`
- `DB_PASSWORD`
- `TNS_ADMIN`
- `DYLD_LIBRARY_PATH` on macOS if Instant Client is outside a standard path

See [docs/04-adb-integration.md](/Users/david/src/open_archive/docs/04-adb-integration.md)
for the confirmed connectivity notes carried over from the spike.
