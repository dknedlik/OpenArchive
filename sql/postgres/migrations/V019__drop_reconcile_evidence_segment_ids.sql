-- Reconciliation no longer stores segment-level evidence ids.
-- Provenance for new pipeline output is artifact-level.

ALTER TABLE oa_reconciliation_decision
    DROP COLUMN IF EXISTS evidence_segment_ids_json;
