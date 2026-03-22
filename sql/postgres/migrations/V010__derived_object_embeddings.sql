CREATE EXTENSION IF NOT EXISTS vector;

ALTER TABLE oa_enrichment_job
    DROP CONSTRAINT IF EXISTS ck_oa_job_type;

ALTER TABLE oa_enrichment_job
    ADD CONSTRAINT ck_oa_job_type CHECK (
        job_type IN (
            'artifact_preprocess',
            'artifact_extract',
            'artifact_retrieve_context',
            'artifact_reconcile',
            'derived_object_embed'
        )
    );

CREATE TABLE oa_derived_object_embedding (
    derived_object_id   TEXT PRIMARY KEY REFERENCES oa_derived_object (derived_object_id) ON DELETE CASCADE,
    artifact_id         TEXT NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    derived_object_type TEXT NOT NULL CHECK (derived_object_type IN ('summary', 'classification', 'memory', 'relationship')),
    embedding_provider  TEXT NOT NULL,
    embedding_model     TEXT NOT NULL,
    content_text_hash   TEXT NOT NULL,
    embedding           VECTOR(1536) NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX ix_oa_derived_object_embedding_artifact_id
    ON oa_derived_object_embedding (artifact_id);

CREATE INDEX ix_oa_derived_object_embedding_type
    ON oa_derived_object_embedding (derived_object_type);
