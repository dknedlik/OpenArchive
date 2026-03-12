-- Add enrichment tier, job lineage, and model capability routing to oa_enrichment_job.
--
-- See Postgres migration V002 for full design rationale.
--
-- Oracle notes:
--   ADD (col TYPE DEFAULT val NOT NULL) applies the DEFAULT to existing rows
--   when the column is added, satisfying the NOT NULL constraint without a
--   separate UPDATE pass. Requires Oracle 11g+.
--
--   required_capabilities is a CLOB with IS JSON validation. Oracle does not
--   support JSONB natively; JSON integrity is enforced via CHECK constraint.
--   Claim queries use JSON_EXISTS or JSON_TABLE to filter by capability tag.
--
--   Oracle does not support partial indexes, so ix_oa_enrichment_job_spawned_by
--   covers all rows including NULLs. Oracle skips NULL-only index entries by
--   default for single-column B-tree indexes, so the effective size is still
--   proportional to quality job volume only.

ALTER TABLE oa_enrichment_job ADD (
    enrichment_tier       VARCHAR2(16 CHAR)  DEFAULT 'standard' NOT NULL,
    spawned_by_job_id     VARCHAR2(36 CHAR),
    required_capabilities CLOB               DEFAULT '["text"]' NOT NULL
);

ALTER TABLE oa_enrichment_job ADD CONSTRAINT ck_oa_job_tier CHECK (
    enrichment_tier IN ('standard', 'quality')
);

ALTER TABLE oa_enrichment_job ADD CONSTRAINT fk_oa_job_spawned_by FOREIGN KEY (spawned_by_job_id)
    REFERENCES oa_enrichment_job (job_id);

ALTER TABLE oa_enrichment_job ADD CONSTRAINT ck_oa_job_capabilities_json CHECK (
    required_capabilities IS JSON
);

-- Composite index for tier-filtered claiming.
CREATE INDEX ix_oa_enrichment_job_tier_status
    ON oa_enrichment_job (enrichment_tier, job_status, available_at);

-- Lineage index. Oracle omits NULL-only leaf entries from single-column
-- B-tree indexes, so this index naturally stays small.
CREATE INDEX ix_oa_enrichment_job_spawned_by
    ON oa_enrichment_job (spawned_by_job_id);
