ALTER TABLE oa_enrichment_job DROP CONSTRAINT ck_oa_job_type;

ALTER TABLE oa_enrichment_job ADD CONSTRAINT ck_oa_job_type CHECK (
    job_type IN ('artifact_preprocess', 'artifact_enrichment')
);
