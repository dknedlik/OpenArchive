CREATE TABLE oa_enrichment_batch (
    provider_batch_id TEXT PRIMARY KEY,
    provider_name TEXT NOT NULL,
    stage_name TEXT NOT NULL,
    phase_name TEXT NOT NULL,
    owner_worker_id TEXT NOT NULL,
    batch_status TEXT NOT NULL DEFAULT 'running',
    context_json JSONB,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    last_error_message TEXT,
    CONSTRAINT ck_oa_enrichment_batch_status CHECK (
        batch_status IN ('running', 'completed', 'failed')
    )
);

CREATE INDEX ix_oa_enrichment_batch_stage_status
    ON oa_enrichment_batch (stage_name, batch_status, submitted_at);

CREATE TABLE oa_enrichment_batch_job (
    provider_batch_id TEXT NOT NULL REFERENCES oa_enrichment_batch (provider_batch_id) ON DELETE CASCADE,
    job_id TEXT NOT NULL REFERENCES oa_enrichment_job (job_id) ON DELETE CASCADE,
    job_order INTEGER NOT NULL,
    PRIMARY KEY (provider_batch_id, job_id),
    CONSTRAINT uq_oa_enrichment_batch_job_order UNIQUE (provider_batch_id, job_order)
);

CREATE INDEX ix_oa_enrichment_batch_job_job_id
    ON oa_enrichment_batch_job (job_id);
