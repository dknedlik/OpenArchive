-- Add enrichment tier, job lineage, and model capability routing to oa_enrichment_job.
--
-- enrichment_tier: controls which worker pool claims this job and which model
--   tier handles it. 'standard' covers summarization, classification, and entity
--   extraction. 'quality' covers memory extraction and requires a frontier model.
--   Standard tier jobs are created at import time. Quality tier jobs are spawned
--   by the standard tier worker on completion when the output warrants it.
--
-- spawned_by_job_id: lineage reference from a quality job back to the standard job
--   that produced it. Nullable — standard jobs have no parent. Allows tracing the
--   full enrichment history for an artifact without scanning derivation_run rows.
--
-- required_capabilities: JSON array of model capability tags this job requires.
--   Worker pools declare their supported capabilities and only claim jobs whose
--   required set is a subset of what they support. Slice-one jobs all require
--   ["text"]. Future source types will add "image", "audio", etc. so that vision
--   or audio models are only invoked when the content actually needs them.
--
--   Example values:
--     ["text"]           -- text-only conversation or document
--     ["text", "image"]  -- document with embedded images, or image artifact
--     ["text", "audio"]  -- audio transcript artifact

ALTER TABLE oa_enrichment_job
    ADD COLUMN enrichment_tier TEXT NOT NULL DEFAULT 'standard'
        CONSTRAINT ck_oa_job_tier CHECK (enrichment_tier IN ('standard', 'quality'));

ALTER TABLE oa_enrichment_job
    ADD COLUMN spawned_by_job_id TEXT
        CONSTRAINT fk_oa_job_spawned_by REFERENCES oa_enrichment_job (job_id);

ALTER TABLE oa_enrichment_job
    ADD COLUMN required_capabilities JSONB NOT NULL DEFAULT '["text"]';

-- Composite index for tier-filtered claiming. The claim query filters on
-- job_status and available_at within a specific tier, so this covers that
-- pattern efficiently without a full table scan.
CREATE INDEX ix_oa_enrichment_job_tier_status
    ON oa_enrichment_job (enrichment_tier, job_status, available_at);

-- Sparse index on spawned_by_job_id for lineage lookups. Only quality jobs
-- carry this value so the partial index stays small.
CREATE INDEX ix_oa_enrichment_job_spawned_by
    ON oa_enrichment_job (spawned_by_job_id)
    WHERE spawned_by_job_id IS NOT NULL;
