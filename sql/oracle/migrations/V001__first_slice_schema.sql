CREATE TABLE oa_object_ref (
    object_id VARCHAR2(36 CHAR) NOT NULL,
    object_kind VARCHAR2(40 CHAR) NOT NULL,
    storage_provider VARCHAR2(40 CHAR) NOT NULL,
    storage_key VARCHAR2(1024 CHAR) NOT NULL,
    mime_type VARCHAR2(255 CHAR) NOT NULL,
    size_bytes NUMBER(19) NOT NULL,
    sha256 VARCHAR2(64 CHAR) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_oa_object_ref PRIMARY KEY (object_id),
    CONSTRAINT uq_oa_object_ref_sha256 UNIQUE (sha256),
    CONSTRAINT ck_oa_object_kind CHECK (
        object_kind IN ('import_payload', 'canonical_source_copy')
    ),
    CONSTRAINT ck_oa_object_size CHECK (size_bytes >= 0)
);

CREATE TABLE oa_import (
    import_id VARCHAR2(36 CHAR) NOT NULL,
    source_type VARCHAR2(40 CHAR) NOT NULL,
    import_status VARCHAR2(32 CHAR) NOT NULL,
    payload_object_id VARCHAR2(36 CHAR) NOT NULL,
    source_filename VARCHAR2(1024 CHAR),
    source_content_hash VARCHAR2(64 CHAR) NOT NULL,
    conversation_count_detected NUMBER(10) DEFAULT 0 NOT NULL,
    conversation_count_imported NUMBER(10) DEFAULT 0 NOT NULL,
    conversation_count_failed NUMBER(10) DEFAULT 0 NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    error_message CLOB,
    CONSTRAINT pk_oa_import PRIMARY KEY (import_id),
    CONSTRAINT fk_oa_import_payload_object FOREIGN KEY (payload_object_id)
        REFERENCES oa_object_ref (object_id),
    CONSTRAINT ck_oa_import_source_type CHECK (
        source_type IN ('chatgpt_export')
    ),
    CONSTRAINT ck_oa_import_status CHECK (
        import_status IN (
            'pending',
            'parsing',
            'completed',
            'completed_with_errors',
            'failed'
        )
    ),
    CONSTRAINT ck_oa_import_counts_detected CHECK (conversation_count_detected >= 0),
    CONSTRAINT ck_oa_import_counts_imported CHECK (conversation_count_imported >= 0),
    CONSTRAINT ck_oa_import_counts_failed CHECK (conversation_count_failed >= 0),
    CONSTRAINT ck_oa_import_counts_total CHECK (
        conversation_count_imported + conversation_count_failed <= conversation_count_detected
    )
);

CREATE INDEX ix_oa_import_source_content_hash
    ON oa_import (source_content_hash);

CREATE TABLE oa_artifact (
    artifact_id VARCHAR2(36 CHAR) NOT NULL,
    import_id VARCHAR2(36 CHAR) NOT NULL,
    artifact_class VARCHAR2(32 CHAR) NOT NULL,
    source_type VARCHAR2(40 CHAR) NOT NULL,
    artifact_status VARCHAR2(32 CHAR) NOT NULL,
    enrichment_status VARCHAR2(32 CHAR) NOT NULL,
    source_conversation_key VARCHAR2(255 CHAR),
    source_conversation_hash VARCHAR2(64 CHAR) NOT NULL,
    title VARCHAR2(1024 CHAR),
    created_at_source TIMESTAMP WITH TIME ZONE,
    captured_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE,
    ended_at TIMESTAMP WITH TIME ZONE,
    primary_language VARCHAR2(32 CHAR),
    content_hash_version VARCHAR2(32 CHAR) NOT NULL,
    content_facets_json CLOB NOT NULL,
    normalization_version VARCHAR2(64 CHAR) NOT NULL,
    error_message CLOB,
    CONSTRAINT pk_oa_artifact PRIMARY KEY (artifact_id),
    CONSTRAINT fk_oa_artifact_import FOREIGN KEY (import_id)
        REFERENCES oa_import (import_id),
    CONSTRAINT uq_oa_artifact_source_hash UNIQUE (
        source_type,
        content_hash_version,
        source_conversation_hash
    ),
    CONSTRAINT ck_oa_artifact_class CHECK (
        artifact_class IN ('conversation')
    ),
    CONSTRAINT ck_oa_artifact_source_type CHECK (
        source_type IN ('chatgpt_export')
    ),
    CONSTRAINT ck_oa_artifact_status CHECK (
        artifact_status IN ('captured', 'normalized', 'failed')
    ),
    CONSTRAINT ck_oa_artifact_enrichment_status CHECK (
        enrichment_status IN ('pending', 'running', 'completed', 'partial', 'failed')
    ),
    CONSTRAINT ck_oa_artifact_json CHECK (content_facets_json IS JSON),
    CONSTRAINT ck_oa_artifact_time_range CHECK (
        started_at IS NULL OR ended_at IS NULL OR ended_at >= started_at
    )
);

CREATE INDEX ix_oa_artifact_import_id
    ON oa_artifact (import_id);

CREATE INDEX ix_oa_artifact_enrichment_status
    ON oa_artifact (enrichment_status);

CREATE TABLE oa_conversation_participant (
    participant_id VARCHAR2(36 CHAR) NOT NULL,
    artifact_id VARCHAR2(36 CHAR) NOT NULL,
    participant_role VARCHAR2(32 CHAR) NOT NULL,
    display_name VARCHAR2(255 CHAR),
    provider_name VARCHAR2(255 CHAR),
    model_name VARCHAR2(255 CHAR),
    source_participant_key VARCHAR2(255 CHAR),
    sequence_no NUMBER(10) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_oa_conversation_participant PRIMARY KEY (participant_id),
    CONSTRAINT fk_oa_participant_artifact FOREIGN KEY (artifact_id)
        REFERENCES oa_artifact (artifact_id)
        ON DELETE CASCADE,
    CONSTRAINT uq_oa_participant_artifact_sequence UNIQUE (artifact_id, sequence_no),
    CONSTRAINT ck_oa_participant_role CHECK (
        participant_role IN ('user', 'assistant', 'system', 'tool', 'unknown')
    ),
    CONSTRAINT ck_oa_participant_sequence CHECK (sequence_no >= 0)
);

CREATE INDEX ix_oa_conversation_participant_artifact_id
    ON oa_conversation_participant (artifact_id);

CREATE TABLE oa_segment (
    segment_id VARCHAR2(36 CHAR) NOT NULL,
    artifact_id VARCHAR2(36 CHAR) NOT NULL,
    participant_id VARCHAR2(36 CHAR),
    segment_type VARCHAR2(32 CHAR) NOT NULL,
    source_segment_key VARCHAR2(255 CHAR),
    parent_segment_id VARCHAR2(36 CHAR),
    sequence_no NUMBER(10) NOT NULL,
    created_at_source TIMESTAMP WITH TIME ZONE,
    text_content CLOB,
    text_content_hash VARCHAR2(64 CHAR),
    locator_json CLOB,
    visibility_status VARCHAR2(32 CHAR) NOT NULL,
    unsupported_content_json CLOB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_oa_segment PRIMARY KEY (segment_id),
    CONSTRAINT fk_oa_segment_artifact FOREIGN KEY (artifact_id)
        REFERENCES oa_artifact (artifact_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_oa_segment_participant FOREIGN KEY (participant_id)
        REFERENCES oa_conversation_participant (participant_id),
    CONSTRAINT fk_oa_segment_parent FOREIGN KEY (parent_segment_id)
        REFERENCES oa_segment (segment_id),
    CONSTRAINT uq_oa_segment_artifact_sequence_type UNIQUE (
        artifact_id,
        sequence_no,
        segment_type
    ),
    CONSTRAINT ck_oa_segment_type CHECK (
        segment_type IN ('message', 'message_window')
    ),
    CONSTRAINT ck_oa_segment_visibility CHECK (
        visibility_status IN ('visible', 'hidden', 'skipped_unsupported')
    ),
    CONSTRAINT ck_oa_segment_sequence CHECK (sequence_no >= 0),
    CONSTRAINT ck_oa_segment_message_text CHECK (
        segment_type <> 'message' OR text_content IS NOT NULL
    ),
    CONSTRAINT ck_oa_segment_locator_json CHECK (
        locator_json IS NULL OR locator_json IS JSON
    ),
    CONSTRAINT ck_oa_segment_unsupported_json CHECK (
        unsupported_content_json IS NULL OR unsupported_content_json IS JSON
    )
);

CREATE INDEX ix_oa_segment_artifact_id
    ON oa_segment (artifact_id);

CREATE INDEX ix_oa_segment_participant_id
    ON oa_segment (participant_id);

CREATE TABLE oa_enrichment_job (
    job_id VARCHAR2(36 CHAR) NOT NULL,
    artifact_id VARCHAR2(36 CHAR) NOT NULL,
    job_type VARCHAR2(40 CHAR) NOT NULL,
    job_status VARCHAR2(32 CHAR) NOT NULL,
    attempt_count NUMBER(10) DEFAULT 0 NOT NULL,
    max_attempts NUMBER(10) DEFAULT 3 NOT NULL,
    priority_no NUMBER(10) DEFAULT 100 NOT NULL,
    claimed_by VARCHAR2(255 CHAR),
    claimed_at TIMESTAMP WITH TIME ZONE,
    available_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    payload_json CLOB NOT NULL,
    last_error_message CLOB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT pk_oa_enrichment_job PRIMARY KEY (job_id),
    CONSTRAINT fk_oa_job_artifact FOREIGN KEY (artifact_id)
        REFERENCES oa_artifact (artifact_id)
        ON DELETE CASCADE,
    CONSTRAINT ck_oa_job_type CHECK (
        job_type IN ('artifact_enrichment')
    ),
    CONSTRAINT ck_oa_job_status CHECK (
        job_status IN (
            'pending',
            'running',
            'completed',
            'partial',
            'failed',
            'retryable'
        )
    ),
    CONSTRAINT ck_oa_job_attempt_count CHECK (attempt_count >= 0),
    CONSTRAINT ck_oa_job_max_attempts CHECK (max_attempts > 0),
    CONSTRAINT ck_oa_job_priority CHECK (priority_no >= 0),
    CONSTRAINT ck_oa_job_payload_json CHECK (payload_json IS JSON),
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
    derivation_run_id VARCHAR2(36 CHAR) NOT NULL,
    artifact_id VARCHAR2(36 CHAR) NOT NULL,
    job_id VARCHAR2(36 CHAR),
    run_type VARCHAR2(40 CHAR) NOT NULL,
    pipeline_name VARCHAR2(255 CHAR) NOT NULL,
    pipeline_version VARCHAR2(64 CHAR) NOT NULL,
    provider_name VARCHAR2(255 CHAR),
    model_name VARCHAR2(255 CHAR),
    prompt_version VARCHAR2(64 CHAR),
    run_status VARCHAR2(32 CHAR) NOT NULL,
    input_scope_type VARCHAR2(32 CHAR) NOT NULL,
    input_scope_json CLOB NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    error_message CLOB,
    CONSTRAINT pk_oa_derivation_run PRIMARY KEY (derivation_run_id),
    CONSTRAINT fk_oa_derivation_run_artifact FOREIGN KEY (artifact_id)
        REFERENCES oa_artifact (artifact_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_oa_derivation_run_job FOREIGN KEY (job_id)
        REFERENCES oa_enrichment_job (job_id),
    CONSTRAINT ck_oa_derivation_run_type CHECK (
        run_type IN (
            'summary_extraction',
            'classification_extraction',
            'memory_extraction',
            'summary_reduction',
            'memory_reduction',
            'context_pack_assembly'
        )
    ),
    CONSTRAINT ck_oa_derivation_run_status CHECK (
        run_status IN ('running', 'completed', 'failed')
    ),
    CONSTRAINT ck_oa_derivation_scope_type CHECK (
        input_scope_type IN ('artifact', 'segment_window', 'artifact_reduce')
    ),
    CONSTRAINT ck_oa_derivation_input_scope_json CHECK (input_scope_json IS JSON),
    CONSTRAINT ck_oa_derivation_time_range CHECK (
        completed_at IS NULL OR completed_at >= started_at
    )
);

CREATE INDEX ix_oa_derivation_run_artifact_id
    ON oa_derivation_run (artifact_id);

CREATE INDEX ix_oa_derivation_run_job_id
    ON oa_derivation_run (job_id);

CREATE TABLE oa_derived_object (
    derived_object_id VARCHAR2(36 CHAR) NOT NULL,
    artifact_id VARCHAR2(36 CHAR) NOT NULL,
    derivation_run_id VARCHAR2(36 CHAR) NOT NULL,
    derived_object_type VARCHAR2(32 CHAR) NOT NULL,
    origin_kind VARCHAR2(32 CHAR) NOT NULL,
    object_status VARCHAR2(32 CHAR) NOT NULL,
    confidence_score NUMBER(5,4),
    confidence_label VARCHAR2(32 CHAR),
    scope_type VARCHAR2(32 CHAR) NOT NULL,
    scope_id VARCHAR2(36 CHAR) NOT NULL,
    title VARCHAR2(255 CHAR),
    body_text CLOB,
    object_json CLOB,
    supersedes_derived_object_id VARCHAR2(36 CHAR),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_oa_derived_object PRIMARY KEY (derived_object_id),
    CONSTRAINT fk_oa_derived_object_artifact FOREIGN KEY (artifact_id)
        REFERENCES oa_artifact (artifact_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_oa_derived_object_run FOREIGN KEY (derivation_run_id)
        REFERENCES oa_derivation_run (derivation_run_id),
    CONSTRAINT fk_oa_derived_object_supersedes FOREIGN KEY (supersedes_derived_object_id)
        REFERENCES oa_derived_object (derived_object_id),
    CONSTRAINT ck_oa_derived_object_type CHECK (
        derived_object_type IN ('summary', 'classification', 'memory')
    ),
    CONSTRAINT ck_oa_derived_object_origin CHECK (
        origin_kind IN ('explicit', 'deterministic', 'inferred', 'user_confirmed')
    ),
    CONSTRAINT ck_oa_derived_object_status CHECK (
        object_status IN ('active', 'superseded', 'failed')
    ),
    CONSTRAINT ck_oa_derived_object_scope CHECK (
        scope_type IN ('artifact', 'segment')
    ),
    CONSTRAINT ck_oa_derived_object_confidence CHECK (
        confidence_score IS NULL OR (confidence_score >= 0 AND confidence_score <= 1)
    ),
    CONSTRAINT ck_oa_derived_object_json CHECK (
        object_json IS NULL OR object_json IS JSON
    )
);

CREATE INDEX ix_oa_derived_object_artifact_id
    ON oa_derived_object (artifact_id);

CREATE INDEX ix_oa_derived_object_run_id
    ON oa_derived_object (derivation_run_id);

CREATE INDEX ix_oa_derived_object_type
    ON oa_derived_object (derived_object_type);

CREATE TABLE oa_evidence_link (
    evidence_link_id VARCHAR2(36 CHAR) NOT NULL,
    derived_object_id VARCHAR2(36 CHAR) NOT NULL,
    segment_id VARCHAR2(36 CHAR) NOT NULL,
    evidence_role VARCHAR2(32 CHAR) NOT NULL,
    evidence_rank NUMBER(10) NOT NULL,
    support_strength VARCHAR2(16 CHAR) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_oa_evidence_link PRIMARY KEY (evidence_link_id),
    CONSTRAINT fk_oa_evidence_link_object FOREIGN KEY (derived_object_id)
        REFERENCES oa_derived_object (derived_object_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_oa_evidence_link_segment FOREIGN KEY (segment_id)
        REFERENCES oa_segment (segment_id),
    CONSTRAINT uq_oa_evidence_link_rank UNIQUE (
        derived_object_id,
        evidence_rank
    ),
    CONSTRAINT uq_oa_evidence_link_segment UNIQUE (
        derived_object_id,
        segment_id
    ),
    CONSTRAINT ck_oa_evidence_role CHECK (
        evidence_role IN ('primary_support', 'secondary_support', 'reduction_input')
    ),
    CONSTRAINT ck_oa_evidence_strength CHECK (
        support_strength IN ('strong', 'medium', 'weak')
    ),
    CONSTRAINT ck_oa_evidence_rank CHECK (evidence_rank >= 1)
);

CREATE INDEX ix_oa_evidence_link_derived_object_id
    ON oa_evidence_link (derived_object_id);

CREATE INDEX ix_oa_evidence_link_segment_id
    ON oa_evidence_link (segment_id);

CREATE TABLE oa_context_pack_cache (
    context_pack_id VARCHAR2(36 CHAR) NOT NULL,
    artifact_id VARCHAR2(36 CHAR) NOT NULL,
    pack_type VARCHAR2(40 CHAR) NOT NULL,
    pack_status VARCHAR2(16 CHAR) NOT NULL,
    request_hash VARCHAR2(64 CHAR) NOT NULL,
    pack_json CLOB NOT NULL,
    derivation_run_id VARCHAR2(36 CHAR) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT pk_oa_context_pack_cache PRIMARY KEY (context_pack_id),
    CONSTRAINT fk_oa_context_pack_artifact FOREIGN KEY (artifact_id)
        REFERENCES oa_artifact (artifact_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_oa_context_pack_run FOREIGN KEY (derivation_run_id)
        REFERENCES oa_derivation_run (derivation_run_id),
    CONSTRAINT uq_oa_context_pack_request UNIQUE (artifact_id, pack_type, request_hash),
    CONSTRAINT ck_oa_context_pack_type CHECK (
        pack_type IN ('conversation_resume')
    ),
    CONSTRAINT ck_oa_context_pack_status CHECK (
        pack_status IN ('active', 'stale')
    ),
    CONSTRAINT ck_oa_context_pack_json CHECK (pack_json IS JSON),
    CONSTRAINT ck_oa_context_pack_expiry CHECK (
        expires_at IS NULL OR expires_at >= created_at
    )
);
