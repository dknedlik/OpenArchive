# Phase 15: AI-in-SaaS Learning Syllabus

**Status:** 🔄 **ACTIVE**

---

## Overview

This document is the learning-first roadmap for AI work in OpenArchive.

The goal is not just to build a functional archive for AI chat history. The
goal is to use OpenArchive as a practical lab for learning how to build
production-worthy AI systems: retrieval, memory, grounded summarization,
structured outputs, evaluation, observability, human review, and interoperable
agent integration.

OCI-native implementation still matters because direct experience with Oracle
Cloud, Oracle ADB, and Oracle-specific AI/data capabilities is useful. But each
module should map to a portable skill that transfers across providers and
product environments.

### Why This Syllabus Exists

- Keep AI work learning-first rather than feature-first
- Practice production SaaS AI patterns, not just demo prompts
- Use OpenArchive as a realistic environment for memory, retrieval, and agent
  interoperability experiments
- Build OCI-native depth while preserving cross-provider architectural judgment

### How to Use This Document

- Treat each module as a learning unit, not just a feature ticket
- Complete modules in order unless there is a strong reason to skip ahead
- Update the status and notes for a module when its lab is finished
- Keep feature-specific implementation details in narrower docs when needed
- Use this file as the source of truth for AI learning progress

---

## Learning Sequence

### Module 1: AI Product Framing and Trust Boundaries

**Status:** ⏳ Pending

**Why this matters in production SaaS AI**

Every useful AI product starts by deciding what the model should do, what the
application must do deterministically, and where user trust can be lost. This
is foundational product and systems judgment.

**Learning objectives**

- Identify where AI adds value versus noise in an archive and memory product
- Separate facts, interpretation, and action-taking
- Define trust boundaries for summaries, memory extraction, and retrieval
- Decide when the system must fall back to deterministic behavior

**Repo lab**

- Write an AI feature contract for one read-only OpenArchive AI experience
- Classify each part of the flow as deterministic fact, model interpretation,
  or user-confirmed action
- Define failure behavior, fallback rules, and confidence boundaries

**OCI-specific angle**

- No deep OCI dependency yet; focus on architecture and product design choices
  before implementation

**Portable equivalent**

- Universal across OCI, OpenAI, Anthropic, AWS, and GCP-based SaaS products

**Completion criteria**

- One AI use case is documented with explicit trust boundaries
- Deterministic versus model-owned responsibilities are written down
- Failure and fallback behavior is defined

---

### Module 2: Context Engineering

**Status:** ⏳ Pending

**Why this matters in production SaaS AI**

Most real-world AI quality problems are context problems. Good SaaS AI depends
on assembling the right business data in a compact, auditable, repeatable
format.

**Learning objectives**

- Build compact, task-specific context from archived conversations and metadata
- Reduce token waste by shaping and summarizing context
- Version context structures for repeatability
- Understand why context quality matters more than prompt cleverness

**Repo lab**

- Build a context assembler for one OpenArchive task such as summarizing a
  conversation or retrieving prior relevant threads
- Produce a structured context payload rather than sending raw transcripts
- Version the context schema

**OCI-specific angle**

- Use OCI Generative AI as an inference target if and when inference is wired

**Portable equivalent**

- The same app-owned retrieval and context shaping pattern applies to any hosted
  model provider

**Completion criteria**

- One context builder exists for a real AI workflow
- The context is structured, compact, and versioned
- The context shape is documented and reusable

---

### Module 3: Structured Outputs

**Status:** ⏳ Pending

**Why this matters in production SaaS AI**

Raw prose is hard to validate and hard to integrate. Structured outputs are how
AI becomes a dependable subsystem instead of a novelty.

**Learning objectives**

- Design typed response schemas for archive and memory workflows
- Validate AI responses before using them
- Handle invalid output and degraded mode cleanly
- Separate machine-readable fields from presentation text

**Repo lab**

- Define a response schema for one OpenArchive answer flow such as summarization
  or memory extraction
- Validate model output server-side
- Implement fallback behavior for invalid or partial outputs

**OCI-specific angle**

- OCI inference remains the source of generation, but validation is owned by
  the app

**Portable equivalent**

- Structured output validation is a core production pattern across providers

**Completion criteria**

- One response schema is implemented and documented
- Parse failures are handled without breaking the user experience
- The system only uses validated output

---

### Module 4: Prompt Engineering as Application Engineering

**Status:** ⏳ Pending

**Why this matters in production SaaS AI**

Prompts are not one-off strings in production systems. They are versioned
application assets that encode scope, constraints, format, and task definition.

**Learning objectives**

- Write task-specific prompts with explicit constraints
- Version prompts like code
- Compare prompt strategies against the same task
- Understand how prompt design depends on context design

**Repo lab**

- Create versioned prompt templates for the first OpenArchive AI workflow
- Store prompt version with each AI response record or artifact
- Compare at least two prompt variants against the same evaluation set

**OCI-specific angle**

- Exercise OCI model selection and inference parameters while holding app
  behavior constant

**Portable equivalent**

- Prompt lifecycle management translates directly across providers

**Completion criteria**

- Prompt templates are versioned
- Prompt version is captured in logs or persisted metadata
- At least two prompt variants have been compared intentionally

---

### Module 5: Evaluation and Quality Measurement

**Status:** ⏳ Pending

**Why this matters in production SaaS AI**

Without evaluation, AI quality is anecdotal. Real products need a way to tell
whether a change improved correctness, usefulness, or trust.

**Learning objectives**

- Build a representative evaluation set
- Define quality dimensions for archive-oriented AI tasks
- Test prompt and model changes against expected outcomes
- Distinguish factual accuracy from tone and usefulness

**Repo lab**

- Create a small evaluation set from realistic OpenArchive tasks such as
  summary quality, memory extraction, or thread retrieval
- Define expected outputs or scoring criteria
- Run the same evaluation set before changing prompts or models

**OCI-specific angle**

- OCI is the runtime provider; evaluation remains application-owned

**Portable equivalent**

- Evaluation discipline is one of the most transferable AI product skills

**Completion criteria**

- A repeatable eval set exists
- Quality criteria are written down
- Prompt/model changes can be compared against a stable baseline

---

### Module 6: Caching, Cost, and Latency Management

**Status:** ⏳ Pending

**Why this matters in production SaaS AI**

Inference cost and latency shape product design. Good AI systems know when to
reuse work, when to pay for fresh computation, and how to express those
tradeoffs.

**Learning objectives**

- Decide when AI outputs should be cached
- Design context-aware cache keys
- Set freshness policies by use case
- Understand latency and cost tradeoffs in API behavior

**Repo lab**

- Add caching for one AI workflow using task, prompt version, and context hash
- Define invalidation rules
- Log cache hit and miss behavior

**OCI-specific angle**

- Use ADB for cache and audit storage where it makes the architecture simpler

**Portable equivalent**

- The same caching strategies apply with any paid model provider

**Completion criteria**

- One AI workflow has an explicit cache policy
- Cache keys include prompt and context versioning
- Cache behavior is observable in logs or metrics

---

### Module 7: Observability and Operations

**Status:** ⏳ Pending

**Why this matters in production SaaS AI**

AI systems fail in new ways: timeouts, parse errors, stale cache, bad prompts,
hallucinated memories, and user distrust. Production competence means seeing
those failures clearly.

**Learning objectives**

- Instrument the full AI request path
- Capture latency, failures, and parse issues
- Log useful metadata without leaking sensitive context
- Build operational visibility for prompts and models

**Repo lab**

- Add request IDs, model name, prompt version, latency, cache behavior, and
  failure type to AI logs
- Define what must not be logged in production
- Create a basic operational checklist for monitoring AI behavior

**OCI-specific angle**

- Use OCI Logging and Monitoring for operational visibility

**Portable equivalent**

- Observability requirements are universal even if the vendor-specific tooling
  changes

**Completion criteria**

- AI flows emit structured operational metadata
- Sensitive prompt/content logging rules are documented
- There is a basic troubleshooting path for failures

---

### Module 8: Human-in-the-Loop UX

**Status:** ⏳ Pending

**Why this matters in production SaaS AI**

Users trust AI more when it assists visibly and can be corrected. Drafts,
confirmations, and explanations are often more valuable than full automation.

**Learning objectives**

- Design review and correction into AI flows
- Communicate uncertainty without making the feature useless
- Build reversible or low-risk interaction patterns
- Prefer assistive behavior over silent decisions

**Repo lab**

- Design one reviewable AI flow such as approving an extracted memory or edited
  summary
- Show the proposed artifact before any write or promotion happens
- Capture whether the user accepts, edits, or rejects the draft

**OCI-specific angle**

- OCI inference remains the model layer; trust is created in application UX and
  control flow

**Portable equivalent**

- Human-in-the-loop design is central to safe AI product behavior across
  vendors

**Completion criteria**

- One AI interaction uses a visible review pattern
- The user can reject or edit the proposed result
- The system records acceptance or rejection for learning purposes

---

### Module 9: Tool Calling and Constrained Actions

**Status:** ⏳ Pending

**Why this matters in production SaaS AI**

The important skill is not "let the AI do things." It is turning natural
language into constrained, validated, auditable operations against
application-owned business logic.

**Learning objectives**

- Convert language into structured intent
- Route AI proposals into constrained backend actions
- Validate and confirm writes before execution
- Keep the model out of direct control of business writes

**Repo lab**

- Implement one AI action flow that produces a typed draft against an existing
  safe backend path such as tagging a thread or promoting a memory
- Validate the draft server-side
- Require explicit confirmation before execution

**OCI-specific angle**

- Use OCI inference for intent extraction or structured action drafting

**Portable equivalent**

- This is the basis for practical agents inside SaaS systems regardless of
  provider

**Completion criteria**

- One action-capable AI flow exists
- The action is validated against app rules before execution
- No write occurs without explicit user confirmation

---

### Module 10: Retrieval and Semantic Memory

**Status:** ⏳ Pending

**Why this matters in production SaaS AI**

Not all useful context is a clean SQL query. Retrieval teaches when semantic
similarity helps, when it hurts, and how to combine it with deterministic
filters.

**Learning objectives**

- Understand when embeddings are useful
- Compare semantic retrieval to exact metadata or SQL retrieval
- Design chunks, metadata, and retrieval boundaries
- Use retrieval as support for grounded generation

**Repo lab**

- Add a narrow semantic retrieval experiment over archived messages, summaries,
  or extracted memories
- Compare semantic retrieval to ordinary metadata or full-text retrieval for
  the same task
- Document when vector search helps and when it does not

**OCI-specific angle**

- Use Oracle 23ai vector capabilities as one retrieval experiment if they fit

**Portable equivalent**

- The same retrieval principles apply to Pinecone, pgvector, Elasticsearch,
  OpenSearch, and other vector systems

**Completion criteria**

- One vector-backed retrieval experiment is running
- Retrieval quality is compared to a non-vector baseline
- Usefulness boundaries are documented

---

### Module 11: NL-to-SQL and Select AI

**Status:** ⏳ Pending

**Why this matters in production SaaS AI**

Natural-language querying is powerful, but it can also produce confident wrong
answers or unsafe query behavior. This module is about bounded experimentation
and governance.

**Learning objectives**

- Understand the promise and risk of NL-to-SQL
- Restrict AI-generated querying to safe, read-only scopes
- Inspect generated behavior instead of blindly trusting it
- Compare open-ended query generation to app-owned retrieval paths

**Repo lab**

- Build a bounded read-only "ask the archive" experiment
- Restrict the scope to approved tables or views
- Compare its behavior against deterministic retrieval paths

**OCI-specific angle**

- Use Oracle Select AI and related 23ai capabilities for the primary experiment

**Portable equivalent**

- The general skill is NL-to-query governance, even though Select AI is
  Oracle-specific

**Completion criteria**

- One read-only NL-to-SQL experiment exists
- Query scope is intentionally constrained
- Results are compared against a safer baseline

---

### Module 12: Provider Abstraction and Portability

**Status:** ⏳ Pending

**Why this matters in production SaaS AI**

Provider lock-in is sometimes acceptable, but accidental lock-in is not. Mature
systems separate provider-specific capabilities from portable application
logic.

**Learning objectives**

- Decide which layers should be provider-neutral
- Build a minimal abstraction around inference calls
- Keep prompts, context builders, and validators portable
- Preserve OCI-specific advantages without coupling the whole app to OCI

**Repo lab**

- Define a minimal inference abstraction for OpenArchive
- Keep the surrounding AI workflow portable
- Document OCI-specific pieces and their likely alternatives elsewhere

**OCI-specific angle**

- OCI remains the primary implementation target for this project unless a better
  reason emerges

**Portable equivalent**

- The abstraction layer should make the architecture understandable outside
  Oracle-specific contexts

**Completion criteria**

- The inference boundary is explicitly defined
- Portable versus OCI-specific responsibilities are documented
- The surrounding workflow is not hardwired to one vendor-specific API shape

---

## Progress Tracking

**Current module:** Module 1 - AI Product Framing and Trust Boundaries

**Completed modules**

- None yet

**Current notes**

- The first OpenArchive slice should optimize for learning production SaaS AI
  patterns, not just proving import/export mechanics
- The product and the lab are intentionally the same project
- OCI-native depth matters, but the architecture should stay understandable in
  cross-provider terms
- Early implementation should likely start with read-only AI and grounded
  outputs before action-taking flows

**Design changes discovered while implementing**

- Add notes here as modules lead to architecture or product changes in the app

---

## Update Rules

When a module is finished:

1. Change its status from `⏳ Pending` to `✅ Complete`
2. Add a short summary of what was built
3. Record any design changes or lessons learned in the progress section
4. Link to narrower implementation docs if the module produced them

When a module changes shape:

- Update the module in this file directly
- Do not create scattered side notes when the syllabus itself is what changed
- Keep the module title focused on the skill being learned, not the surface
  feature

When OCI-specific learning maps to broader industry patterns:

- Add the mapping here so the learning remains portable
