CREATE TABLE oa_object_ref (
    object_id TEXT PRIMARY KEY,
    object_kind TEXT NOT NULL CHECK (object_kind IN ('import_payload', 'canonical_source_copy')),
    storage_provider TEXT NOT NULL,
    storage_key TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    sha256 TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE oa_import (
    import_id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL CHECK (
        source_type IN (
            'chatgpt_export',
            'claude_export',
            'grok_export',
            'gemini_takeout',
            'text_file',
            'markdown_file',
            'obsidian_vault'
        )
    ),
    import_status TEXT NOT NULL CHECK (
        import_status IN ('pending', 'parsing', 'completed', 'completed_with_errors', 'failed')
    ),
    payload_object_id TEXT NOT NULL REFERENCES oa_object_ref(object_id),
    source_filename TEXT,
    source_content_hash TEXT NOT NULL,
    conversation_count_detected INTEGER NOT NULL DEFAULT 0 CHECK (conversation_count_detected >= 0),
    conversation_count_imported INTEGER NOT NULL DEFAULT 0 CHECK (conversation_count_imported >= 0),
    conversation_count_failed INTEGER NOT NULL DEFAULT 0 CHECK (conversation_count_failed >= 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    completed_at TEXT,
    error_message TEXT,
    CHECK (conversation_count_imported + conversation_count_failed <= conversation_count_detected)
);

CREATE INDEX ix_oa_import_source_content_hash ON oa_import(source_content_hash);

CREATE TABLE oa_artifact (
    artifact_id TEXT PRIMARY KEY,
    import_id TEXT NOT NULL REFERENCES oa_import(import_id),
    artifact_class TEXT NOT NULL CHECK (artifact_class IN ('conversation', 'document')),
    source_type TEXT NOT NULL CHECK (
        source_type IN (
            'chatgpt_export',
            'claude_export',
            'grok_export',
            'gemini_takeout',
            'text_file',
            'markdown_file',
            'obsidian_vault'
        )
    ),
    artifact_status TEXT NOT NULL CHECK (artifact_status IN ('captured', 'normalized', 'failed')),
    enrichment_status TEXT NOT NULL CHECK (
        enrichment_status IN ('pending', 'running', 'completed', 'partial', 'failed')
    ),
    source_conversation_key TEXT,
    source_conversation_hash TEXT NOT NULL,
    title TEXT,
    created_at_source TEXT,
    captured_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    started_at TEXT,
    ended_at TEXT,
    primary_language TEXT,
    content_hash_version TEXT NOT NULL,
    content_facets_json TEXT NOT NULL,
    normalization_version TEXT NOT NULL,
    error_message TEXT,
    UNIQUE (source_type, content_hash_version, source_conversation_hash)
);

CREATE INDEX ix_oa_artifact_import_id ON oa_artifact(import_id);
CREATE INDEX ix_oa_artifact_enrichment_status ON oa_artifact(enrichment_status);
CREATE INDEX ix_oa_artifact_captured_at ON oa_artifact(captured_at);
CREATE INDEX ix_oa_artifact_source_conversation_key ON oa_artifact(source_conversation_key);

CREATE TABLE oa_conversation_participant (
    participant_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact(artifact_id) ON DELETE CASCADE,
    participant_role TEXT NOT NULL CHECK (
        participant_role IN ('user', 'assistant', 'system', 'tool', 'unknown')
    ),
    display_name TEXT,
    provider_name TEXT,
    model_name TEXT,
    source_participant_key TEXT,
    sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (artifact_id, sequence_no)
);

CREATE INDEX ix_oa_conversation_participant_artifact_id
    ON oa_conversation_participant(artifact_id);

CREATE TABLE oa_segment (
    segment_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact(artifact_id) ON DELETE CASCADE,
    participant_id TEXT REFERENCES oa_conversation_participant(participant_id),
    segment_type TEXT NOT NULL CHECK (segment_type IN ('content_block', 'content_window')),
    source_segment_key TEXT,
    parent_segment_id TEXT REFERENCES oa_segment(segment_id),
    sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
    created_at_source TEXT,
    text_content TEXT,
    text_content_hash TEXT,
    locator_json TEXT,
    visibility_status TEXT NOT NULL CHECK (
        visibility_status IN ('visible', 'hidden', 'skipped_unsupported')
    ),
    unsupported_content_json TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (artifact_id, sequence_no, segment_type),
    CHECK (segment_type <> 'content_block' OR text_content IS NOT NULL)
);

CREATE INDEX ix_oa_segment_artifact_id ON oa_segment(artifact_id);
CREATE INDEX ix_oa_segment_participant_id ON oa_segment(participant_id);

CREATE TABLE oa_enrichment_job (
    job_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact(artifact_id) ON DELETE CASCADE,
    job_type TEXT NOT NULL CHECK (
        job_type IN ('artifact_preprocess', 'artifact_extract', 'artifact_reconcile', 'derived_object_embed')
    ),
    job_status TEXT NOT NULL CHECK (
        job_status IN ('pending', 'running', 'completed', 'partial', 'failed', 'retryable')
    ),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    priority_no INTEGER NOT NULL DEFAULT 100 CHECK (priority_no >= 0),
    claimed_by TEXT,
    claimed_at TEXT,
    available_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    payload_json TEXT NOT NULL,
    last_error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    completed_at TEXT,
    enrichment_tier TEXT NOT NULL DEFAULT 'default' CHECK (enrichment_tier = 'default'),
    spawned_by_job_id TEXT REFERENCES oa_enrichment_job(job_id),
    required_capabilities TEXT NOT NULL DEFAULT '["text"]',
    CHECK (
        (claimed_by IS NULL AND claimed_at IS NULL) OR
        (claimed_by IS NOT NULL AND claimed_at IS NOT NULL)
    )
);

CREATE INDEX ix_oa_enrichment_job_artifact_id ON oa_enrichment_job(artifact_id);
CREATE INDEX ix_oa_enrichment_job_available_at ON oa_enrichment_job(available_at);
CREATE INDEX ix_oa_enrichment_job_status ON oa_enrichment_job(job_status);
CREATE INDEX ix_oa_enrichment_job_tier_status
    ON oa_enrichment_job(enrichment_tier, job_status, available_at);
CREATE INDEX ix_oa_enrichment_job_spawned_by
    ON oa_enrichment_job(spawned_by_job_id) WHERE spawned_by_job_id IS NOT NULL;

CREATE TABLE oa_derivation_run (
    derivation_run_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact(artifact_id) ON DELETE CASCADE,
    job_id TEXT REFERENCES oa_enrichment_job(job_id),
    run_type TEXT NOT NULL CHECK (
        run_type IN (
            'artifact_extraction',
            'archive_retrieval',
            'artifact_reconciliation',
            'context_pack_assembly',
            'agent_contributed'
        )
    ),
    pipeline_name TEXT NOT NULL,
    pipeline_version TEXT NOT NULL,
    provider_name TEXT,
    model_name TEXT,
    prompt_version TEXT,
    run_status TEXT NOT NULL CHECK (run_status IN ('running', 'completed', 'failed')),
    input_scope_type TEXT NOT NULL CHECK (input_scope_type IN ('artifact', 'segment_window', 'artifact_reduce')),
    input_scope_json TEXT NOT NULL,
    started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    completed_at TEXT,
    error_message TEXT
);

CREATE INDEX ix_oa_derivation_run_artifact_id ON oa_derivation_run(artifact_id);
CREATE INDEX ix_oa_derivation_run_job_id ON oa_derivation_run(job_id);

CREATE TABLE oa_derived_object (
    derived_object_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact(artifact_id) ON DELETE CASCADE,
    derivation_run_id TEXT NOT NULL REFERENCES oa_derivation_run(derivation_run_id),
    derived_object_type TEXT NOT NULL CHECK (
        derived_object_type IN ('summary', 'classification', 'memory', 'relationship', 'entity')
    ),
    origin_kind TEXT NOT NULL CHECK (
        origin_kind IN ('explicit', 'deterministic', 'inferred', 'user_confirmed', 'agent_contributed')
    ),
    object_status TEXT NOT NULL CHECK (object_status IN ('active', 'superseded', 'failed', 'rejected')),
    confidence_score REAL,
    confidence_label TEXT,
    scope_type TEXT NOT NULL CHECK (scope_type IN ('artifact', 'segment')),
    scope_id TEXT NOT NULL,
    title TEXT,
    body_text TEXT,
    object_json TEXT,
    supersedes_derived_object_id TEXT REFERENCES oa_derived_object(derived_object_id),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    candidate_key TEXT GENERATED ALWAYS AS (json_extract(object_json, '$.candidate_key')) STORED,
    CHECK (confidence_score IS NULL OR (confidence_score >= 0.0 AND confidence_score <= 1.0))
);

CREATE INDEX ix_oa_derived_object_artifact_id ON oa_derived_object(artifact_id);
CREATE INDEX ix_oa_derived_object_run_id ON oa_derived_object(derivation_run_id);
CREATE INDEX ix_oa_derived_object_type ON oa_derived_object(derived_object_type);
CREATE INDEX ix_oa_derived_object_candidate_key
    ON oa_derived_object(candidate_key) WHERE candidate_key IS NOT NULL;

CREATE TABLE oa_archive_link (
    archive_link_id TEXT PRIMARY KEY,
    source_object_id TEXT NOT NULL REFERENCES oa_derived_object(derived_object_id),
    target_object_id TEXT NOT NULL REFERENCES oa_derived_object(derived_object_id),
    link_type TEXT NOT NULL CHECK (
        link_type IN ('same_as', 'continues', 'contradicts', 'same_topic', 'refers_to')
    ),
    confidence_score REAL,
    rationale TEXT,
    origin_kind TEXT NOT NULL DEFAULT 'agent_contributed',
    contributed_by TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (source_object_id, target_object_id, link_type),
    CHECK (source_object_id <> target_object_id)
);

CREATE INDEX ix_oa_archive_link_source ON oa_archive_link(source_object_id);
CREATE INDEX ix_oa_archive_link_target ON oa_archive_link(target_object_id);
CREATE INDEX ix_oa_archive_link_type ON oa_archive_link(link_type);

CREATE TABLE oa_artifact_extraction_result (
    extraction_result_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact(artifact_id) ON DELETE CASCADE,
    job_id TEXT NOT NULL REFERENCES oa_enrichment_job(job_id) ON DELETE CASCADE,
    pipeline_name TEXT NOT NULL,
    pipeline_version TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX ix_oa_artifact_extraction_result_artifact_id
    ON oa_artifact_extraction_result(artifact_id);
CREATE INDEX ix_oa_artifact_extraction_result_job_id
    ON oa_artifact_extraction_result(job_id);

CREATE TABLE oa_reconciliation_decision (
    reconciliation_decision_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact(artifact_id) ON DELETE CASCADE,
    job_id TEXT NOT NULL REFERENCES oa_enrichment_job(job_id) ON DELETE CASCADE,
    extraction_result_id TEXT NOT NULL REFERENCES oa_artifact_extraction_result(extraction_result_id) ON DELETE CASCADE,
    pipeline_name TEXT NOT NULL,
    pipeline_version TEXT NOT NULL,
    decision_kind TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    target_key TEXT NOT NULL,
    matched_object_id TEXT,
    rationale TEXT NOT NULL,
    decision_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX ix_oa_reconciliation_decision_artifact_id
    ON oa_reconciliation_decision(artifact_id);

CREATE TABLE oa_enrichment_batch (
    provider_batch_id TEXT PRIMARY KEY,
    provider_name TEXT NOT NULL,
    stage_name TEXT NOT NULL,
    phase_name TEXT NOT NULL,
    owner_worker_id TEXT NOT NULL,
    batch_status TEXT NOT NULL DEFAULT 'running' CHECK (batch_status IN ('running', 'completed', 'failed')),
    context_json TEXT,
    submitted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    completed_at TEXT,
    last_error_message TEXT
);

CREATE INDEX ix_oa_enrichment_batch_stage_status
    ON oa_enrichment_batch(stage_name, batch_status, submitted_at);

CREATE TABLE oa_enrichment_batch_job (
    provider_batch_id TEXT NOT NULL REFERENCES oa_enrichment_batch(provider_batch_id) ON DELETE CASCADE,
    job_id TEXT NOT NULL REFERENCES oa_enrichment_job(job_id) ON DELETE CASCADE,
    job_order INTEGER NOT NULL,
    PRIMARY KEY (provider_batch_id, job_id),
    UNIQUE (provider_batch_id, job_order)
);

CREATE INDEX ix_oa_enrichment_batch_job_job_id ON oa_enrichment_batch_job(job_id);

CREATE TABLE oa_review_decision (
    review_decision_id TEXT PRIMARY KEY,
    item_id TEXT NOT NULL,
    item_kind TEXT NOT NULL CHECK (
        item_kind IN (
            'artifact_needs_attention',
            'artifact_missing_summary',
            'object_low_confidence',
            'candidate_key_collision',
            'object_missing_evidence'
        )
    ),
    artifact_id TEXT NOT NULL REFERENCES oa_artifact(artifact_id) ON DELETE CASCADE,
    derived_object_id TEXT REFERENCES oa_derived_object(derived_object_id) ON DELETE CASCADE,
    decision_status TEXT NOT NULL CHECK (decision_status IN ('noted', 'dismissed', 'resolved')),
    note_text TEXT,
    decided_by TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX ix_oa_review_decision_artifact_id ON oa_review_decision(artifact_id);
CREATE INDEX ix_oa_review_decision_derived_object_id ON oa_review_decision(derived_object_id);
CREATE INDEX ix_oa_review_decision_item_id ON oa_review_decision(item_id, created_at DESC);
CREATE INDEX ix_oa_review_decision_item_status ON oa_review_decision(item_id, decision_status);

CREATE TABLE oa_artifact_note_property (
    artifact_note_property_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact(artifact_id) ON DELETE CASCADE,
    property_key TEXT NOT NULL,
    value_kind TEXT NOT NULL CHECK (
        value_kind IN ('string', 'number', 'boolean', 'date', 'datetime', 'list', 'null', 'json')
    ),
    value_text TEXT,
    value_json TEXT NOT NULL,
    sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (artifact_id, sequence_no)
);

CREATE INDEX ix_oa_artifact_note_property_artifact_id
    ON oa_artifact_note_property(artifact_id);
CREATE INDEX ix_oa_artifact_note_property_key
    ON oa_artifact_note_property(property_key);

CREATE TABLE oa_artifact_note_tag (
    artifact_note_tag_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact(artifact_id) ON DELETE CASCADE,
    raw_tag TEXT NOT NULL,
    normalized_tag TEXT NOT NULL,
    tag_path TEXT NOT NULL,
    source_kind TEXT NOT NULL CHECK (source_kind IN ('frontmatter', 'inline')),
    sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (artifact_id, sequence_no)
);

CREATE INDEX ix_oa_artifact_note_tag_artifact_id ON oa_artifact_note_tag(artifact_id);
CREATE INDEX ix_oa_artifact_note_tag_normalized ON oa_artifact_note_tag(normalized_tag);

CREATE TABLE oa_artifact_note_alias (
    artifact_note_alias_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact(artifact_id) ON DELETE CASCADE,
    alias_text TEXT NOT NULL,
    normalized_alias TEXT NOT NULL,
    sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (artifact_id, sequence_no)
);

CREATE INDEX ix_oa_artifact_note_alias_artifact_id ON oa_artifact_note_alias(artifact_id);
CREATE INDEX ix_oa_artifact_note_alias_normalized ON oa_artifact_note_alias(normalized_alias);

CREATE TABLE oa_artifact_note_link (
    artifact_note_link_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact(artifact_id) ON DELETE CASCADE,
    source_segment_id TEXT REFERENCES oa_segment(segment_id) ON DELETE SET NULL,
    link_kind TEXT NOT NULL CHECK (link_kind IN ('link', 'embed')),
    target_kind TEXT NOT NULL CHECK (target_kind IN ('note', 'heading', 'block', 'external', 'attachment')),
    raw_target TEXT NOT NULL,
    normalized_target TEXT,
    display_text TEXT,
    target_path TEXT,
    target_heading TEXT,
    target_block TEXT,
    external_url TEXT,
    resolved_artifact_id TEXT REFERENCES oa_artifact(artifact_id) ON DELETE SET NULL,
    resolution_status TEXT NOT NULL CHECK (resolution_status IN ('resolved', 'unresolved', 'external')),
    locator_json TEXT,
    sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (artifact_id, sequence_no)
);

CREATE INDEX ix_oa_artifact_note_link_artifact_id ON oa_artifact_note_link(artifact_id);
CREATE INDEX ix_oa_artifact_note_link_resolved_artifact_id ON oa_artifact_note_link(resolved_artifact_id);
CREATE INDEX ix_oa_artifact_note_link_normalized_target ON oa_artifact_note_link(normalized_target);

CREATE TABLE oa_artifact_link (
    artifact_link_id TEXT PRIMARY KEY,
    source_artifact_id TEXT NOT NULL REFERENCES oa_artifact(artifact_id) ON DELETE CASCADE,
    target_artifact_id TEXT NOT NULL REFERENCES oa_artifact(artifact_id) ON DELETE CASCADE,
    link_type TEXT NOT NULL CHECK (link_type IN ('wikilink', 'shared_tag', 'alias', 'reconciled_object')),
    link_value TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (source_artifact_id, target_artifact_id, link_type, link_value),
    CHECK (source_artifact_id <> target_artifact_id)
);

CREATE INDEX ix_oa_artifact_link_source ON oa_artifact_link(source_artifact_id);
CREATE INDEX ix_oa_artifact_link_target ON oa_artifact_link(target_artifact_id);
CREATE INDEX ix_oa_artifact_link_type ON oa_artifact_link(link_type);

CREATE TABLE oa_context_pack_cache (
    context_pack_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact(artifact_id) ON DELETE CASCADE,
    pack_type TEXT NOT NULL CHECK (pack_type = 'conversation_resume'),
    pack_status TEXT NOT NULL CHECK (pack_status IN ('active', 'stale')),
    request_hash TEXT NOT NULL,
    pack_json TEXT NOT NULL,
    derivation_run_id TEXT NOT NULL REFERENCES oa_derivation_run(derivation_run_id),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    expires_at TEXT,
    UNIQUE (artifact_id, pack_type, request_hash)
);

CREATE VIRTUAL TABLE oa_artifact_fts USING fts5(
    title,
    content='oa_artifact',
    content_rowid='rowid',
    tokenize='unicode61'
);

CREATE VIRTUAL TABLE oa_segment_fts USING fts5(
    text_content,
    content='oa_segment',
    content_rowid='rowid',
    tokenize='unicode61'
);

CREATE VIRTUAL TABLE oa_derived_object_fts USING fts5(
    title,
    body_text,
    content='oa_derived_object',
    content_rowid='rowid',
    tokenize='unicode61'
);

CREATE TRIGGER oa_artifact_fts_ai AFTER INSERT ON oa_artifact BEGIN
  INSERT INTO oa_artifact_fts(rowid, title) VALUES (new.rowid, new.title);
END;
CREATE TRIGGER oa_artifact_fts_ad AFTER DELETE ON oa_artifact BEGIN
  INSERT INTO oa_artifact_fts(oa_artifact_fts, rowid, title) VALUES('delete', old.rowid, old.title);
END;
CREATE TRIGGER oa_artifact_fts_au AFTER UPDATE ON oa_artifact BEGIN
  INSERT INTO oa_artifact_fts(oa_artifact_fts, rowid, title) VALUES('delete', old.rowid, old.title);
  INSERT INTO oa_artifact_fts(rowid, title) VALUES (new.rowid, new.title);
END;

CREATE TRIGGER oa_segment_fts_ai AFTER INSERT ON oa_segment BEGIN
  INSERT INTO oa_segment_fts(rowid, text_content) VALUES (new.rowid, new.text_content);
END;
CREATE TRIGGER oa_segment_fts_ad AFTER DELETE ON oa_segment BEGIN
  INSERT INTO oa_segment_fts(oa_segment_fts, rowid, text_content) VALUES('delete', old.rowid, old.text_content);
END;
CREATE TRIGGER oa_segment_fts_au AFTER UPDATE ON oa_segment BEGIN
  INSERT INTO oa_segment_fts(oa_segment_fts, rowid, text_content) VALUES('delete', old.rowid, old.text_content);
  INSERT INTO oa_segment_fts(rowid, text_content) VALUES (new.rowid, new.text_content);
END;

CREATE TRIGGER oa_derived_object_fts_ai AFTER INSERT ON oa_derived_object BEGIN
  INSERT INTO oa_derived_object_fts(rowid, title, body_text) VALUES (new.rowid, new.title, new.body_text);
END;
CREATE TRIGGER oa_derived_object_fts_ad AFTER DELETE ON oa_derived_object BEGIN
  INSERT INTO oa_derived_object_fts(oa_derived_object_fts, rowid, title, body_text) VALUES('delete', old.rowid, old.title, old.body_text);
END;
CREATE TRIGGER oa_derived_object_fts_au AFTER UPDATE ON oa_derived_object BEGIN
  INSERT INTO oa_derived_object_fts(oa_derived_object_fts, rowid, title, body_text) VALUES('delete', old.rowid, old.title, old.body_text);
  INSERT INTO oa_derived_object_fts(rowid, title, body_text) VALUES (new.rowid, new.title, new.body_text);
END;
