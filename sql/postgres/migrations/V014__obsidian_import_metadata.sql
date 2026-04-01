ALTER TABLE oa_import
    DROP CONSTRAINT IF EXISTS oa_import_source_type_check;

ALTER TABLE oa_import
    ADD CONSTRAINT oa_import_source_type_check
    CHECK (
        source_type IN (
            'chatgpt_export',
            'claude_export',
            'grok_export',
            'gemini_takeout',
            'text_file',
            'markdown_file',
            'obsidian_vault'
        )
    );

ALTER TABLE oa_artifact
    DROP CONSTRAINT IF EXISTS oa_artifact_source_type_check;

ALTER TABLE oa_artifact
    ADD CONSTRAINT oa_artifact_source_type_check
    CHECK (
        source_type IN (
            'chatgpt_export',
            'claude_export',
            'grok_export',
            'gemini_takeout',
            'text_file',
            'markdown_file',
            'obsidian_vault'
        )
    );

CREATE INDEX IF NOT EXISTS ix_oa_artifact_source_conversation_key
    ON oa_artifact (source_conversation_key);

CREATE TABLE oa_artifact_note_property (
    artifact_note_property_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    property_key TEXT NOT NULL,
    value_kind TEXT NOT NULL CHECK (
        value_kind IN ('string', 'number', 'boolean', 'date', 'datetime', 'list', 'null', 'json')
    ),
    value_text TEXT,
    value_json JSONB NOT NULL,
    sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_oa_artifact_note_property_sequence UNIQUE (artifact_id, sequence_no)
);

CREATE INDEX ix_oa_artifact_note_property_artifact_id
    ON oa_artifact_note_property (artifact_id);

CREATE INDEX ix_oa_artifact_note_property_key
    ON oa_artifact_note_property (property_key);

CREATE TABLE oa_artifact_note_tag (
    artifact_note_tag_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    raw_tag TEXT NOT NULL,
    normalized_tag TEXT NOT NULL,
    tag_path TEXT NOT NULL,
    source_kind TEXT NOT NULL CHECK (source_kind IN ('frontmatter', 'inline')),
    sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_oa_artifact_note_tag_sequence UNIQUE (artifact_id, sequence_no)
);

CREATE INDEX ix_oa_artifact_note_tag_artifact_id
    ON oa_artifact_note_tag (artifact_id);

CREATE INDEX ix_oa_artifact_note_tag_normalized
    ON oa_artifact_note_tag (normalized_tag);

CREATE TABLE oa_artifact_note_alias (
    artifact_note_alias_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    alias_text TEXT NOT NULL,
    normalized_alias TEXT NOT NULL,
    sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_oa_artifact_note_alias_sequence UNIQUE (artifact_id, sequence_no)
);

CREATE INDEX ix_oa_artifact_note_alias_artifact_id
    ON oa_artifact_note_alias (artifact_id);

CREATE INDEX ix_oa_artifact_note_alias_normalized
    ON oa_artifact_note_alias (normalized_alias);

CREATE TABLE oa_artifact_note_link (
    artifact_note_link_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    source_segment_id TEXT REFERENCES oa_segment (segment_id) ON DELETE SET NULL,
    link_kind TEXT NOT NULL CHECK (link_kind IN ('link', 'embed')),
    target_kind TEXT NOT NULL CHECK (target_kind IN ('note', 'heading', 'block', 'external', 'attachment')),
    raw_target TEXT NOT NULL,
    normalized_target TEXT,
    display_text TEXT,
    target_path TEXT,
    target_heading TEXT,
    target_block TEXT,
    external_url TEXT,
    resolved_artifact_id TEXT REFERENCES oa_artifact (artifact_id) ON DELETE SET NULL,
    resolution_status TEXT NOT NULL CHECK (resolution_status IN ('resolved', 'unresolved', 'external')),
    locator_json JSONB,
    sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_oa_artifact_note_link_sequence UNIQUE (artifact_id, sequence_no)
);

CREATE INDEX ix_oa_artifact_note_link_artifact_id
    ON oa_artifact_note_link (artifact_id);

CREATE INDEX ix_oa_artifact_note_link_resolved_artifact_id
    ON oa_artifact_note_link (resolved_artifact_id);

CREATE INDEX ix_oa_artifact_note_link_normalized_target
    ON oa_artifact_note_link (normalized_target);
