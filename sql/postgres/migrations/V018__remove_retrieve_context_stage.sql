-- Remove the retrieve-context stage from the runtime job graph.
--
-- Existing pending/retryable retrieve-context jobs are rewritten to reconcile
-- jobs with a payload that points directly at the extraction result. Completed
-- historical rows may remain for auditability, but the runtime no longer
-- accepts or schedules this stage.

ALTER TABLE oa_enrichment_job
    DROP CONSTRAINT IF EXISTS ck_oa_job_type;

UPDATE oa_enrichment_job
SET job_type = 'artifact_reconcile',
    payload_json = jsonb_build_object(
        'version', 'v1',
        'artifact_id', artifact_id,
        'import_id', payload_json->>'import_id',
        'source_type', payload_json->>'source_type',
        'extraction_result_id', payload_json->>'extraction_result_id'
    )
WHERE job_type = 'artifact_retrieve_context';

ALTER TABLE oa_enrichment_job
    ADD CONSTRAINT ck_oa_job_type CHECK (
        job_type IN (
            'artifact_preprocess',
            'artifact_extract',
            'artifact_reconcile',
            'derived_object_embed'
        )
    );

ALTER TABLE oa_reconciliation_decision
    DROP COLUMN IF EXISTS retrieval_result_set_id;

DROP TABLE IF EXISTS oa_retrieval_result_set;
