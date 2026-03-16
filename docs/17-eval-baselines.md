# Evaluation Baselines

This document records lightweight baseline results from real pipeline runs so
provider and prompt changes can be compared against something concrete.

## 2026-03-15 ChatGPT Export Baseline

- Corpus: small OpenAI/ChatGPT export used as the primary pipeline smoke-test
  corpus
- Import id: `import-189d26eb560e2204-1`
- Primary inference provider for this run: Gemini batch pipeline
- Audit command:
  `cargo run --quiet --bin audit_knowledge -- --worst 10`

### Pipeline Outcome

- `64/65` preprocess jobs completed
- `63/65` extract jobs completed
- `63/65` retrieval jobs completed
- `62/65` reconcile + derivation jobs completed at the time of audit
- 1 preprocess artifact failed due to invalid phase-one evidence refs
- A few jobs were later found stranded in `running` state during restart/fix
  cycles and had to be reset manually

### Knowledge Audit

- Overall score: `68.6 / 100`
- Audited artifacts: `63`
- Signature reported by audit: `deterministic |  |`
  - The audit is scoring persisted derived objects, not attributing quality
    directly back to the upstream model/provider.

#### Subscores

- Summary: `20.0 / 20.0`
- Evidence: `20.0 / 20.0`
- Memories: `12.3 / 15.0`
- Classifications: `5.0 / 10.0`
- Compression: `8.3 / 10.0`
- Importance: `3.0 / 10.0`
- Duplication penalty: `0.0`

#### Output Mix

- Summaries: `63`
- Classifications: `60`
- Memories: `66`
- Relationships: `38`
- Memory coverage: `79%`
- Classification coverage: `95%`
- Escalations: `0`

#### Top Audit Issues

- `missing_importance_score = 63`
- `summary_overcompressed = 26`
- `no_memories_on_rich_artifact = 13`
- `evidence_overcited = 1`

### Relationship / Reconciliation Notes

- Reconciliation decisions:
  - `66 create_new memory`
  - `38 create_new relationship`
  - `1 contradicts_existing relationship`
  - `10 insufficient_evidence artifact`
- `matched_object_id`: `0 / 115`
- Interpretation:
  - Relationship extraction exists and produced plausible relationship objects.
  - Actual linking/merging against prior brain state was effectively absent on
    this corpus.

### Spot-Check Notes

#### Low-memory artifacts

These looked acceptable without strong memory extraction:

- `Grill chicken method`
- `Healthy banana protein recipes`

These looked borderline but arguable:

- `Search Mounted Drives Script`
  - Could justify a reusable reference-style memory, but not obviously a strong
    durable personal memory.

Interpretation:

- This corpus appears sparse and skewed toward one-off Q&A, generic lookups,
  and utility interactions.
- Low memory yield on many artifacts is likely a corpus property as much as a
  model/prompt issue.

#### Memory-worthy artifacts

These looked genuinely good:

- `Planning API Gateway Setup`
  - Captured migration strategy, effort estimate, and replacement relationship.
- `Model selection workflows`
  - Captured memory-bank pattern, workflow consolidation, and tool/project
    relationship.

Interpretation:

- On clearly project- and workflow-oriented artifacts, the pipeline can produce
  useful durable memories and relationships.
- The main semantic weakness observed in this corpus is underproduction on
  borderline cases, not obvious hallucinated brain clutter.

### Overall Takeaways

- This ChatGPT export is a good pipeline smoke-test corpus:
  - small
  - quick to rerun
  - useful for import, queueing, batch polling, recovery, and persistence bugs
- It is not a strong benchmark for long-horizon brain quality.
- For future comparisons:
  - use this corpus to ring out provider behavior, prompts, and batch modes
  - use richer Gemini/Grok/Claude exports to judge actual brain quality
