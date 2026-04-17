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

- [ ] **7. `doctor` improvements**
  Add checks for: keyring availability, API key present in keyring, Qdrant binary installed
  (with fix hint: `run install-qdrant`), config file exists and has correct permissions.
  Currently doctor checks DB and stores but not the secrets layer.

- [ ] **8. Bundle Qdrant binary in installer**
  Ship Qdrant as part of the installer rather than downloading at runtime. Version-locked
  to the app. `init` verifies the bundled binary is present rather than downloading it.
  `install-qdrant` command kept for developer use but removed from user-facing docs.

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

## Phase Summary

| Phase | Tasks | Outcome |
|-------|-------|---------|
| 1     | 1–3   | New user can run `init` and be configured |
| 2     | 4–6   | Import → enrich → search works end to end from CLI |
| 3     | 7–8   | Setup failures are recoverable, not cryptic |
| 4     | 9–12  | Downloadable binaries on all four platforms |
