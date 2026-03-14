# AI Learning Syllabus

## Status

This document is a learning-first roadmap for AI work in OpenArchive.

The goal is not just to build a functional archive for AI chat history. The
goal is to use OpenArchive as a practical lab for learning how to build
production-worthy AI systems: retrieval, memory, grounded summarization,
structured outputs, evaluation, observability, human review, and interoperable
agent integration.

The current architecture should stay portable across local inference,
self-hosted deployment, and later remote providers.

## Why This Syllabus Exists

- Keep AI work learning-first rather than feature-first
- Practice production AI patterns, not just demo prompts
- Use OpenArchive as a realistic environment for memory, retrieval, and agent
  interoperability experiments
- Build portable judgment that still maps cleanly onto concrete providers

## Current Learning Sequence

1. AI product framing and trust boundaries
2. Context engineering and context-pack design
3. Grounded summarization and memory extraction
4. Structured outputs and validation
5. Retrieval and ranking
6. Evaluation and regression testing
7. Human review and confidence handling
8. Interoperability through MCP-oriented workflows

## Future Brain-Aware Enrichment

Early enrichment should stay grounded in the current artifact and produce
validated structured outputs before it becomes graph-aware.

Later iterations should learn how to enrich with access to existing brain
state, including prior entities, memories, and relationships.

The intended pattern is:

- first-pass artifact extraction
- targeted lookups into existing brain state
- reconciliation of new outputs against existing nodes and edges

This is different from letting the model process every artifact in isolation,
but it is also different from unconstrained tool use. The learning goal is to
build disciplined brain-aware reconciliation, not a free-roaming enrichment
agent.

## Human Review And Confidence Handling

The learning plan should also include a real human review surface for model
outputs that are too uncertain to trust silently.

Important future review categories include:

- low-confidence memories
- weak or ambiguous entity matches
- uncertain relationship creation
- contradictory facts
- merge and supersession suggestions
- artifacts flagged for premium or frontier reruns

The goal is to learn how to keep the brain clean over time, not just how to
generate structured outputs once.

## Provider Stance

OpenArchive should learn from concrete providers without letting the learning
plan force the architecture into one provider's shape.

Likely practical targets over time:

- local Ollama-backed inference
- stub or test providers
- later hosted inference providers where useful

The learning objective is transferable application design, not platform lock-in.
