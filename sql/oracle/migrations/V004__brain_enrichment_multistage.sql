ALTER TABLE oa_enrichment_job DROP CONSTRAINT ck_oa_job_type;

UPDATE oa_enrichment_job
SET job_type = 'artifact_reconcile'
WHERE job_type = 'artifact_enrichment';

ALTER TABLE oa_enrichment_job ADD CONSTRAINT ck_oa_job_type CHECK (
    job_type IN (
        'artifact_preprocess',
        'artifact_extract',
        'artifact_retrieve_context',
        'artifact_reconcile'
    )
);

ALTER TABLE oa_derivation_run DROP CONSTRAINT ck_oa_derivation_run_type;

UPDATE oa_derivation_run
SET run_type = 'artifact_reconciliation'
WHERE run_type IN (
    'summary_extraction',
    'classification_extraction',
    'memory_extraction',
    'summary_reduction',
    'memory_reduction'
);

ALTER TABLE oa_derivation_run ADD CONSTRAINT ck_oa_derivation_run_type CHECK (
    run_type IN (
        'artifact_extraction',
        'archive_retrieval',
        'artifact_reconciliation',
        'context_pack_assembly'
    )
);

ALTER TABLE oa_derived_object DROP CONSTRAINT ck_oa_derived_object_type;

ALTER TABLE oa_derived_object ADD CONSTRAINT ck_oa_derived_object_type CHECK (
    derived_object_type IN ('summary', 'classification', 'memory', 'relationship')
);

CREATE TABLE oa_artifact_extraction_result (
    extraction_result_id VARCHAR2(255 CHAR) PRIMARY KEY,
    artifact_id VARCHAR2(255 CHAR) NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    job_id VARCHAR2(255 CHAR) NOT NULL REFERENCES oa_enrichment_job (job_id) ON DELETE CASCADE,
    pipeline_name VARCHAR2(255 CHAR) NOT NULL,
    pipeline_version VARCHAR2(255 CHAR) NOT NULL,
    result_json CLOB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE INDEX ix_oa_artifact_extract_artifact
    ON oa_artifact_extraction_result (artifact_id);

CREATE INDEX ix_oa_artifact_extract_job
    ON oa_artifact_extraction_result (job_id);

CREATE TABLE oa_retrieval_result_set (
    retrieval_result_set_id VARCHAR2(255 CHAR) PRIMARY KEY,
    artifact_id VARCHAR2(255 CHAR) NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    job_id VARCHAR2(255 CHAR) NOT NULL REFERENCES oa_enrichment_job (job_id) ON DELETE CASCADE,
    extraction_result_id VARCHAR2(255 CHAR) NOT NULL REFERENCES oa_artifact_extraction_result (extraction_result_id) ON DELETE CASCADE,
    pipeline_name VARCHAR2(255 CHAR) NOT NULL,
    pipeline_version VARCHAR2(255 CHAR) NOT NULL,
    result_json CLOB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE INDEX ix_oa_retrieval_result_artifact
    ON oa_retrieval_result_set (artifact_id);

CREATE INDEX ix_oa_retrieval_result_extract
    ON oa_retrieval_result_set (extraction_result_id);

CREATE TABLE oa_reconciliation_decision (
    reconciliation_decision_id VARCHAR2(255 CHAR) PRIMARY KEY,
    artifact_id VARCHAR2(255 CHAR) NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    job_id VARCHAR2(255 CHAR) NOT NULL REFERENCES oa_enrichment_job (job_id) ON DELETE CASCADE,
    extraction_result_id VARCHAR2(255 CHAR) NOT NULL REFERENCES oa_artifact_extraction_result (extraction_result_id) ON DELETE CASCADE,
    retrieval_result_set_id VARCHAR2(255 CHAR) NOT NULL REFERENCES oa_retrieval_result_set (retrieval_result_set_id) ON DELETE CASCADE,
    pipeline_name VARCHAR2(255 CHAR) NOT NULL,
    pipeline_version VARCHAR2(255 CHAR) NOT NULL,
    decision_kind CLOB NOT NULL,
    target_kind VARCHAR2(255 CHAR) NOT NULL,
    target_key VARCHAR2(255 CHAR) NOT NULL,
    matched_object_id VARCHAR2(255 CHAR),
    rationale CLOB NOT NULL,
    evidence_segment_ids_json CLOB NOT NULL,
    decision_json CLOB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE INDEX ix_oa_reconcile_decision_artifact
    ON oa_reconciliation_decision (artifact_id);

CREATE INDEX ix_oa_reconcile_decision_result
    ON oa_reconciliation_decision (retrieval_result_set_id);
