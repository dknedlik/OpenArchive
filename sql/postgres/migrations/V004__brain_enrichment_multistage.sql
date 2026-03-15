DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT c.conname
        FROM pg_constraint c
        JOIN pg_class r ON r.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = 'public'
          AND r.relname = 'oa_enrichment_job'
          AND c.contype = 'c'
          AND pg_get_constraintdef(c.oid) LIKE '%job_type%'
    LOOP
        EXECUTE format('ALTER TABLE oa_enrichment_job DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END
$$;

UPDATE oa_enrichment_job
SET job_type = 'artifact_reconcile'
WHERE job_type = 'artifact_enrichment';

ALTER TABLE oa_enrichment_job
    ADD CONSTRAINT ck_oa_job_type CHECK (
        job_type IN (
            'artifact_preprocess',
            'artifact_extract',
            'artifact_retrieve_context',
            'artifact_reconcile'
        )
    );

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT c.conname
        FROM pg_constraint c
        JOIN pg_class r ON r.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = 'public'
          AND r.relname = 'oa_derivation_run'
          AND c.contype = 'c'
          AND pg_get_constraintdef(c.oid) LIKE '%run_type%'
    LOOP
        EXECUTE format('ALTER TABLE oa_derivation_run DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END
$$;

UPDATE oa_derivation_run
SET run_type = 'artifact_reconciliation'
WHERE run_type IN (
    'summary_extraction',
    'classification_extraction',
    'memory_extraction',
    'summary_reduction',
    'memory_reduction'
);

ALTER TABLE oa_derivation_run
    ADD CONSTRAINT ck_oa_derivation_run_type CHECK (
        run_type IN (
            'artifact_extraction',
            'archive_retrieval',
            'artifact_reconciliation',
            'context_pack_assembly'
        )
    );

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT c.conname
        FROM pg_constraint c
        JOIN pg_class r ON r.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = 'public'
          AND r.relname = 'oa_derived_object'
          AND c.contype = 'c'
          AND pg_get_constraintdef(c.oid) LIKE '%derived_object_type%'
    LOOP
        EXECUTE format('ALTER TABLE oa_derived_object DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END
$$;

ALTER TABLE oa_derived_object
    ADD CONSTRAINT ck_oa_derived_object_type CHECK (
        derived_object_type IN ('summary', 'classification', 'memory', 'relationship')
    );

CREATE TABLE oa_artifact_extraction_result (
    extraction_result_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    job_id TEXT NOT NULL REFERENCES oa_enrichment_job (job_id) ON DELETE CASCADE,
    pipeline_name TEXT NOT NULL,
    pipeline_version TEXT NOT NULL,
    result_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX ix_oa_artifact_extraction_result_artifact_id
    ON oa_artifact_extraction_result (artifact_id);

CREATE INDEX ix_oa_artifact_extraction_result_job_id
    ON oa_artifact_extraction_result (job_id);

CREATE TABLE oa_retrieval_result_set (
    retrieval_result_set_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    job_id TEXT NOT NULL REFERENCES oa_enrichment_job (job_id) ON DELETE CASCADE,
    extraction_result_id TEXT NOT NULL REFERENCES oa_artifact_extraction_result (extraction_result_id) ON DELETE CASCADE,
    pipeline_name TEXT NOT NULL,
    pipeline_version TEXT NOT NULL,
    result_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX ix_oa_retrieval_result_set_artifact_id
    ON oa_retrieval_result_set (artifact_id);

CREATE INDEX ix_oa_retrieval_result_set_extraction_result_id
    ON oa_retrieval_result_set (extraction_result_id);

CREATE TABLE oa_reconciliation_decision (
    reconciliation_decision_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    job_id TEXT NOT NULL REFERENCES oa_enrichment_job (job_id) ON DELETE CASCADE,
    extraction_result_id TEXT NOT NULL REFERENCES oa_artifact_extraction_result (extraction_result_id) ON DELETE CASCADE,
    retrieval_result_set_id TEXT NOT NULL REFERENCES oa_retrieval_result_set (retrieval_result_set_id) ON DELETE CASCADE,
    pipeline_name TEXT NOT NULL,
    pipeline_version TEXT NOT NULL,
    decision_kind JSONB NOT NULL,
    target_kind TEXT NOT NULL,
    target_key TEXT NOT NULL,
    matched_object_id TEXT,
    rationale TEXT NOT NULL,
    evidence_segment_ids_json JSONB NOT NULL,
    decision_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX ix_oa_reconciliation_decision_artifact_id
    ON oa_reconciliation_decision (artifact_id);

CREATE INDEX ix_oa_reconciliation_decision_retrieval_result_set_id
    ON oa_reconciliation_decision (retrieval_result_set_id);
