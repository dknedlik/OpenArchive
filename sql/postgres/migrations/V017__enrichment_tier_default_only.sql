-- Collapse enrichment job routing to a single runtime tier.
--
-- The redesign no longer distinguishes between standard and quality job tiers.
-- Existing rows are normalized to 'default', and future rows are constrained to
-- that single value.

ALTER TABLE oa_enrichment_job
    DROP CONSTRAINT IF EXISTS ck_oa_job_tier;

UPDATE oa_enrichment_job
SET enrichment_tier = 'default'
WHERE enrichment_tier IN ('standard', 'quality');

ALTER TABLE oa_enrichment_job
    ALTER COLUMN enrichment_tier SET DEFAULT 'default';

ALTER TABLE oa_enrichment_job
    ADD CONSTRAINT ck_oa_job_tier CHECK (enrichment_tier = 'default');
