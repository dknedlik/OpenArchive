CREATE TABLE oa_object_ref (
    object_id TEXT PRIMARY KEY,
    object_kind TEXT NOT NULL CHECK (object_kind IN ('import_payload', 'canonical_source_copy')),
    storage_provider TEXT NOT NULL,
    storage_key TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    sha256 TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE oa_import (
    import_id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL CHECK (source_type IN ('chatgpt_export')),
    import_status TEXT NOT NULL CHECK (
        import_status IN ('pending', 'parsing', 'completed', 'completed_with_errors', 'failed')
    ),
    payload_object_id TEXT NOT NULL REFERENCES oa_object_ref (object_id),
    source_filename TEXT,
    source_content_hash TEXT NOT NULL,
    conversation_count_detected INTEGER NOT NULL DEFAULT 0 CHECK (conversation_count_detected >= 0),
    conversation_count_imported INTEGER NOT NULL DEFAULT 0 CHECK (conversation_count_imported >= 0),
    conversation_count_failed INTEGER NOT NULL DEFAULT 0 CHECK (conversation_count_failed >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    CONSTRAINT ck_oa_import_counts_total
        CHECK (conversation_count_imported + conversation_count_failed <= conversation_count_detected)
);

CREATE INDEX ix_oa_import_source_content_hash
    ON oa_import (source_content_hash);

CREATE TABLE oa_artifact (
    artifact_id TEXT PRIMARY KEY,
    import_id TEXT NOT NULL REFERENCES oa_import (import_id),
    artifact_class TEXT NOT NULL CHECK (artifact_class IN ('conversation')),
    source_type TEXT NOT NULL CHECK (source_type IN ('chatgpt_export')),
    artifact_status TEXT NOT NULL CHECK (artifact_status IN ('captured', 'normalized', 'failed')),
    enrichment_status TEXT NOT NULL CHECK (
        enrichment_status IN ('pending', 'running', 'completed', 'partial', 'failed')
    ),
    source_conversation_key TEXT,
    source_conversation_hash TEXT NOT NULL,
    title TEXT,
    created_at_source TIMESTAMPTZ,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    primary_language TEXT,
    content_hash_version TEXT NOT NULL,
    content_facets_json JSONB NOT NULL,
    normalization_version TEXT NOT NULL,
    error_message TEXT,
    CONSTRAINT uq_oa_artifact_source_hash UNIQUE (
        source_type,
        content_hash_version,
        source_conversation_hash
    ),
    CONSTRAINT ck_oa_artifact_time_range
        CHECK (started_at IS NULL OR ended_at IS NULL OR ended_at >= started_at)
);

CREATE INDEX ix_oa_artifact_import_id
    ON oa_artifact (import_id);

CREATE INDEX ix_oa_artifact_enrichment_status
    ON oa_artifact (enrichment_status);

CREATE TABLE oa_conversation_participant (
    participant_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    participant_role TEXT NOT NULL CHECK (participant_role IN ('user', 'assistant', 'system', 'tool', 'unknown')),
    display_name TEXT,
    provider_name TEXT,
    model_name TEXT,
    source_participant_key TEXT,
    sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_oa_participant_artifact_sequence UNIQUE (artifact_id, sequence_no)
);

CREATE INDEX ix_oa_conversation_participant_artifact_id
    ON oa_conversation_participant (artifact_id);

CREATE TABLE oa_segment (
    segment_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    participant_id TEXT REFERENCES oa_conversation_participant (participant_id),
    segment_type TEXT NOT NULL CHECK (segment_type IN ('message', 'message_window')),
    source_segment_key TEXT,
    parent_segment_id TEXT REFERENCES oa_segment (segment_id),
    sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
    created_at_source TIMESTAMPTZ,
    text_content TEXT,
    text_content_hash TEXT,
    locator_json JSONB,
    visibility_status TEXT NOT NULL CHECK (visibility_status IN ('visible', 'hidden', 'skipped_unsupported')),
    unsupported_content_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_oa_segment_artifact_sequence_type UNIQUE (artifact_id, sequence_no, segment_type),
    CONSTRAINT ck_oa_segment_message_text CHECK (segment_type <> 'message' OR text_content IS NOT NULL)
);

CREATE INDEX ix_oa_segment_artifact_id
    ON oa_segment (artifact_id);

CREATE INDEX ix_oa_segment_participant_id
    ON oa_segment (participant_id);

CREATE TABLE oa_enrichment_job (
    job_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    job_type TEXT NOT NULL CHECK (job_type IN ('artifact_enrichment')),
    job_status TEXT NOT NULL CHECK (
        job_status IN ('pending', 'running', 'completed', 'partial', 'failed', 'retryable')
    ),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    priority_no INTEGER NOT NULL DEFAULT 100 CHECK (priority_no >= 0),
    claimed_by TEXT,
    claimed_at TIMESTAMPTZ,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload_json JSONB NOT NULL,
    last_error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT ck_oa_job_claim_pair CHECK (
        (claimed_by IS NULL AND claimed_at IS NULL) OR
        (claimed_by IS NOT NULL AND claimed_at IS NOT NULL)
    )
);

CREATE INDEX ix_oa_enrichment_job_status
    ON oa_enrichment_job (job_status);

CREATE INDEX ix_oa_enrichment_job_artifact_id
    ON oa_enrichment_job (artifact_id);

CREATE INDEX ix_oa_enrichment_job_available_at
    ON oa_enrichment_job (available_at);

CREATE TABLE oa_derivation_run (
    derivation_run_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    job_id TEXT REFERENCES oa_enrichment_job (job_id),
    run_type TEXT NOT NULL CHECK (
        run_type IN (
            'summary_extraction',
            'classification_extraction',
            'memory_extraction',
            'summary_reduction',
            'memory_reduction',
            'context_pack_assembly'
        )
    ),
    pipeline_name TEXT NOT NULL,
    pipeline_version TEXT NOT NULL,
    provider_name TEXT,
    model_name TEXT,
    prompt_version TEXT,
    run_status TEXT NOT NULL CHECK (run_status IN ('running', 'completed', 'failed')),
    input_scope_type TEXT NOT NULL CHECK (input_scope_type IN ('artifact', 'segment_window', 'artifact_reduce')),
    input_scope_json JSONB NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    CONSTRAINT ck_oa_derivation_time_range CHECK (completed_at IS NULL OR completed_at >= started_at)
);

CREATE INDEX ix_oa_derivation_run_artifact_id
    ON oa_derivation_run (artifact_id);

CREATE INDEX ix_oa_derivation_run_job_id
    ON oa_derivation_run (job_id);

CREATE TABLE oa_derived_object (
    derived_object_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    derivation_run_id TEXT NOT NULL REFERENCES oa_derivation_run (derivation_run_id),
    derived_object_type TEXT NOT NULL CHECK (derived_object_type IN ('summary', 'classification', 'memory')),
    origin_kind TEXT NOT NULL CHECK (origin_kind IN ('explicit', 'deterministic', 'inferred', 'user_confirmed')),
    object_status TEXT NOT NULL CHECK (object_status IN ('active', 'superseded', 'failed')),
    confidence_score NUMERIC(5,4),
    confidence_label TEXT,
    scope_type TEXT NOT NULL CHECK (scope_type IN ('artifact', 'segment')),
    scope_id TEXT NOT NULL,
    title TEXT,
    body_text TEXT,
    object_json JSONB,
    supersedes_derived_object_id TEXT REFERENCES oa_derived_object (derived_object_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_oa_derived_object_confidence
        CHECK (confidence_score IS NULL OR (confidence_score >= 0 AND confidence_score <= 1))
);

CREATE INDEX ix_oa_derived_object_artifact_id
    ON oa_derived_object (artifact_id);

CREATE INDEX ix_oa_derived_object_run_id
    ON oa_derived_object (derivation_run_id);

CREATE INDEX ix_oa_derived_object_type
    ON oa_derived_object (derived_object_type);

CREATE TABLE oa_evidence_link (
    evidence_link_id TEXT PRIMARY KEY,
    derived_object_id TEXT NOT NULL REFERENCES oa_derived_object (derived_object_id) ON DELETE CASCADE,
    segment_id TEXT NOT NULL REFERENCES oa_segment (segment_id),
    evidence_role TEXT NOT NULL CHECK (evidence_role IN ('primary_support', 'secondary_support', 'reduction_input')),
    evidence_rank INTEGER NOT NULL CHECK (evidence_rank >= 1),
    support_strength TEXT NOT NULL CHECK (support_strength IN ('strong', 'medium', 'weak')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_oa_evidence_link_rank UNIQUE (derived_object_id, evidence_rank),
    CONSTRAINT uq_oa_evidence_link_segment UNIQUE (derived_object_id, segment_id)
);

CREATE INDEX ix_oa_evidence_link_derived_object_id
    ON oa_evidence_link (derived_object_id);

CREATE INDEX ix_oa_evidence_link_segment_id
    ON oa_evidence_link (segment_id);

CREATE TABLE oa_context_pack_cache (
    context_pack_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    pack_type TEXT NOT NULL CHECK (pack_type IN ('conversation_resume')),
    pack_status TEXT NOT NULL CHECK (pack_status IN ('active', 'stale')),
    request_hash TEXT NOT NULL,
    pack_json JSONB NOT NULL,
    derivation_run_id TEXT NOT NULL REFERENCES oa_derivation_run (derivation_run_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    CONSTRAINT uq_oa_context_pack_request UNIQUE (artifact_id, pack_type, request_hash),
    CONSTRAINT ck_oa_context_pack_expiry CHECK (expires_at IS NULL OR expires_at >= created_at)
);
