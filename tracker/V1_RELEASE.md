# V1 Release Roadmap

Goal: a locally installable product with good UX. No Docker, no cloud dependencies,
no curl. Mac ARM, Mac Intel, Linux x86, Windows x86.

---

## Phase 1 — Config & Secrets Foundation

- [x] **1. Config file schema and loader**
  Define `~/.open_archive/config.toml` schema covering all current env vars.
  Implement `AppConfig::load()` that reads file first, env vars override.
  Update all call sites from `AppConfig::from_env()` to `AppConfig::load()`.

- [x] **2. Keyring integration for secrets**
  Add `keyring` crate. Implement a `SecretStore` with two backends — Keychain/Credential
  Manager (Mac/Windows) and plain text `0600` fallback (Linux or when keyring unavailable).
  Stored secrets: provider API keys. Config file references secrets by name, never stores
  them directly.

- [x] **3. `init` command**
  Interactive first-run setup. Prompts for: data directory, enrichment provider choice,
  API key. Writes config file, stores key in keyring, creates `~/.open_archive/` directory
  structure, runs migrations, runs a mini doctor check, prints summary.
  Safe to re-run — updates existing config rather than clobbering.

---

## Phase 2 — Enrichment UX

- [x] **4. `enrich` command**
  `open_archive enrich [--workers N] [--limit N]`
  Starts enrichment pipeline, drains the job queue, exits when empty.
  Prints progress (`processing job 3 of 47`), summary on completion (`enriched=45 failed=2`).
  No HTTP server. This is the missing link between `import` and `search`.

- [x] **5. Startup validation**
  Before any command that needs enrichment config, validate API key is present and provider
  is configured. Clear, actionable error: `"enrichment provider not configured — run
  open_archive init"` rather than a cryptic panic downstream.

- [x] **6. `status` improvements**
  If pending jobs > 0, make it actionable:
  `pending_jobs=47  (run 'open_archive enrich' to process)`
  Currently just shows a number with no guidance.

---

## Phase 3 — Doctor & Qdrant Hardening

- [x] **7. `doctor` improvements**
  Add checks for: keyring availability, API key present in keyring, Qdrant binary installed
  (with fix hint: `run install-qdrant`), config file exists and has correct permissions.
  Currently doctor checks DB and stores but not the secrets layer.

- [ ] **8. Bundle Qdrant binary in installer**
  CI downloads the version-pinned Qdrant binary for each target platform at build time and
  packages it alongside `open_archive` in the installer artifact. No runtime download step.
  User installs open_archive, Qdrant is already there. Pure CI/packaging work — no app code
  changes required.

---

## Phase 4 — Release Packaging

- [ ] **9. CI cross-compilation pipeline**
  GitHub Actions matrix build:
  - `aarch64-apple-darwin` (Mac ARM)
  - `x86_64-apple-darwin` (Mac Intel)
  - `x86_64-unknown-linux-gnu` (Linux x86)
  - `x86_64-pc-windows-msvc` (Windows x86)
  Produces release binaries on tag push.

- [ ] **10. Mac installer**
  Homebrew tap (`brew install openarchive`) or signed `.pkg`.
  Homebrew is lower friction for the target user.

- [ ] **11. Linux install script**
  `curl -sSf https://... | sh` pattern. Detects arch, downloads correct binary,
  installs to `~/.local/bin` or `/usr/local/bin`.

- [ ] **12. Windows installer**
  MSI or winget package. Winget is lower effort and increasingly the standard.

---

---

## Pre-Release Test Plan

> App code is feature-complete. Test before packaging. No HTTP server required —
> the local product runs entirely via CLI + stdio MCP.

### Phase 1 — Fresh Start
1. Delete `~/.open_archive` entirely — clean slate
2. `open_archive doctor` — expect failures on db, migrations, object_store
3. `open_archive init` — walk through prompts
4. `open_archive doctor` — all green
5. `open_archive status` — zero artifacts, zero jobs

### Phase 2 — Import
6. Import a ChatGPT export ZIP
7. Import a Claude JSON export
8. Import a plain text file
9. Import a markdown file
10. Import an Obsidian vault ZIP
11. Re-import the ChatGPT ZIP — verify idempotency (`already_exists`, no duplicates)

### Phase 3 — Enrichment
12. `open_archive enrich` — watch jobs process
13. `open_archive status` — queue drains, artifacts move to enriched
14. `open_archive enrich` on empty queue — exits cleanly

### Phase 4 — Query
15. Query something that matches imported content — verify hits
16. Query something that won't match — empty results, no crash
17. Query a paraphrase (not exact wording) — tests semantic path

### Phase 5 — Edge Cases
18. Import a malformed/corrupt file — clean error, no partial state
19. Kill `enrich` mid-run, restart — verify jobs resume correctly
20. Wrong API key in keyring — `doctor` fail message is actionable

### Phase 6 — MCP
21. Point Claude Desktop at `open_archive mcp` (stdio)
22. Ask Claude to search the archive — verify it calls `search_archive`
23. Ask Claude to retrieve an artifact — verify `get_artifact` response

---

## Phase Summary

| Phase | Tasks | Outcome |
|-------|-------|---------|
| 1     | 1–3   | New user can run `init` and be configured |
| 2     | 4–6   | Import → enrich → search works end to end from CLI |
| 3     | 7–8   | Setup failures are recoverable, not cryptic |
| 4     | 9–12  | Downloadable binaries on all four platforms |
