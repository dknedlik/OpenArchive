-- Remove the retrieve-context stage from the runtime job graph.

BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE oa_enrichment_job DROP CONSTRAINT ck_oa_job_type';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -2443 THEN
            RAISE;
        END IF;
END;
/

UPDATE oa_enrichment_job
SET job_type = 'artifact_reconcile',
    payload_json = JSON_OBJECT(
        'version' VALUE 'v1',
        'artifact_id' VALUE artifact_id,
        'import_id' VALUE JSON_VALUE(payload_json, '$.import_id'),
        'source_type' VALUE JSON_VALUE(payload_json, '$.source_type'),
        'extraction_result_id' VALUE JSON_VALUE(payload_json, '$.extraction_result_id')
        RETURNING CLOB
    )
WHERE job_type = 'artifact_retrieve_context';

ALTER TABLE oa_enrichment_job ADD CONSTRAINT ck_oa_job_type CHECK (
    job_type IN (
        'artifact_preprocess',
        'artifact_extract',
        'artifact_reconcile'
    )
);

ALTER TABLE oa_reconciliation_decision DROP COLUMN retrieval_result_set_id;

DROP TABLE oa_retrieval_result_set CASCADE CONSTRAINTS;
