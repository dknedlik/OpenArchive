CREATE TABLE oa_enrichment_batch (
    provider_batch_id VARCHAR2(255 CHAR) NOT NULL,
    provider_name VARCHAR2(255 CHAR) NOT NULL,
    stage_name VARCHAR2(255 CHAR) NOT NULL,
    phase_name VARCHAR2(255 CHAR) NOT NULL,
    owner_worker_id VARCHAR2(255 CHAR) NOT NULL,
    batch_status VARCHAR2(32 CHAR) DEFAULT 'running' NOT NULL,
    context_json CLOB,
    submitted_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    last_error_message CLOB,
    CONSTRAINT pk_oa_enrichment_batch PRIMARY KEY (provider_batch_id),
    CONSTRAINT ck_oa_enrichment_batch_status CHECK (
        batch_status IN ('running', 'completed', 'failed')
    ),
    CONSTRAINT ck_oa_enrichment_batch_context_json CHECK (
        context_json IS NULL OR context_json IS JSON
    )
);

CREATE INDEX ix_oa_enrichment_batch_stage_status
    ON oa_enrichment_batch (stage_name, batch_status, submitted_at);

CREATE TABLE oa_enrichment_batch_job (
    provider_batch_id VARCHAR2(255 CHAR) NOT NULL,
    job_id VARCHAR2(255 CHAR) NOT NULL,
    job_order NUMBER(10) NOT NULL,
    CONSTRAINT pk_oa_enrichment_batch_job PRIMARY KEY (provider_batch_id, job_id),
    CONSTRAINT fk_oa_batch_job_batch FOREIGN KEY (provider_batch_id)
        REFERENCES oa_enrichment_batch (provider_batch_id) ON DELETE CASCADE,
    CONSTRAINT fk_oa_batch_job_job FOREIGN KEY (job_id)
        REFERENCES oa_enrichment_job (job_id) ON DELETE CASCADE,
    CONSTRAINT uq_oa_enrichment_batch_job_order UNIQUE (provider_batch_id, job_order)
);

CREATE INDEX ix_oa_enrichment_batch_job_job_id
    ON oa_enrichment_batch_job (job_id);
