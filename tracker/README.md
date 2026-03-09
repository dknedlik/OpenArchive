# Tracker Usage Guide

OpenArchive Tracker
===================

Purpose
- Keep implementation execution separate from product and architecture docs.
- Track milestone-scoped tasks with concrete acceptance criteria.
- Preserve high-level design in `docs/` while using `tracker/` for current work.

Structure
- Index: `tracker/index.yaml`
- Tasks: `tracker/tasks/<milestone>/<ID>_<slug>.yaml`
- Completed tasks: `tracker/tasks/<milestone>/completed/<ID>_<slug>.yaml`

Conventions
- `id`: zero-padded string matching the filename prefix.
- `status`: one of `todo`, `in_progress`, `blocked`, `done`
- `type`: one of `feature`, `bug`, `chore`, `test`
- `priority`: one of `low`, `medium`, `high`
- `files_touched`: repo-relative paths

Recommended fields
- `planned_order`: advisory execution order within a milestone
- `labels`: free-form tags like `api`, `storage`, `oracle`, `ingestion`
- `depends_on`: list of task ids that should land first
- `implementation_plan`
- `test_plan`
- `risks`
- `notes`

Completed workflow
- Each milestone directory contains a `completed/` subfolder.
- When a task is done, move it into `completed/` and keep the filename unchanged.
- Re-opened tasks move back out of `completed/` with an updated `status`.

Notes
- This tracker is intentionally file-based and lightweight.
- There is no validator yet. Keep the format disciplined so a validator can be added later without churn.
- The milestone names should stay aligned with the major planning docs in `docs/`.
