ALTER TABLE oa_import DROP CONSTRAINT ck_oa_import_source_type;

ALTER TABLE oa_import ADD CONSTRAINT ck_oa_import_source_type CHECK (
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

ALTER TABLE oa_artifact DROP CONSTRAINT ck_oa_artifact_source_type;

ALTER TABLE oa_artifact ADD CONSTRAINT ck_oa_artifact_source_type CHECK (
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

ALTER TABLE oa_artifact DROP CONSTRAINT ck_oa_artifact_class;

ALTER TABLE oa_artifact ADD CONSTRAINT ck_oa_artifact_class CHECK (
    artifact_class IN ('conversation', 'document')
);

UPDATE oa_segment
SET segment_type = CASE segment_type
    WHEN 'message' THEN 'content_block'
    WHEN 'message_window' THEN 'content_window'
    ELSE segment_type
END
WHERE segment_type IN ('message', 'message_window');

ALTER TABLE oa_segment DROP CONSTRAINT ck_oa_segment_type;

ALTER TABLE oa_segment ADD CONSTRAINT ck_oa_segment_type CHECK (
    segment_type IN ('content_block', 'content_window')
);

ALTER TABLE oa_segment DROP CONSTRAINT ck_oa_segment_message_text;

ALTER TABLE oa_segment ADD CONSTRAINT ck_oa_segment_content_block_text CHECK (
    segment_type <> 'content_block' OR text_content IS NOT NULL
);

CREATE INDEX ix_oa_artifact_source_conversation_key
    ON oa_artifact (source_conversation_key);

CREATE TABLE oa_artifact_note_property (
    artifact_note_property_id VARCHAR2(64 CHAR) NOT NULL,
    artifact_id VARCHAR2(64 CHAR) NOT NULL,
    property_key VARCHAR2(255 CHAR) NOT NULL,
    value_kind VARCHAR2(32 CHAR) NOT NULL,
    value_text CLOB,
    value_json CLOB NOT NULL,
    sequence_no NUMBER(10) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_oa_artifact_note_property PRIMARY KEY (artifact_note_property_id),
    CONSTRAINT fk_oa_note_property_artifact FOREIGN KEY (artifact_id)
        REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    CONSTRAINT uq_oa_note_property_sequence UNIQUE (artifact_id, sequence_no),
    CONSTRAINT ck_oa_note_property_kind CHECK (
        value_kind IN ('string', 'number', 'boolean', 'date', 'datetime', 'list', 'null', 'json')
    ),
    CONSTRAINT ck_oa_note_property_json CHECK (value_json IS JSON)
);

CREATE INDEX ix_oa_note_property_artifact_id
    ON oa_artifact_note_property (artifact_id);

CREATE INDEX ix_oa_note_property_key
    ON oa_artifact_note_property (property_key);

CREATE TABLE oa_artifact_note_tag (
    artifact_note_tag_id VARCHAR2(64 CHAR) NOT NULL,
    artifact_id VARCHAR2(64 CHAR) NOT NULL,
    raw_tag VARCHAR2(255 CHAR) NOT NULL,
    normalized_tag VARCHAR2(255 CHAR) NOT NULL,
    tag_path VARCHAR2(255 CHAR) NOT NULL,
    source_kind VARCHAR2(32 CHAR) NOT NULL,
    sequence_no NUMBER(10) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_oa_artifact_note_tag PRIMARY KEY (artifact_note_tag_id),
    CONSTRAINT fk_oa_note_tag_artifact FOREIGN KEY (artifact_id)
        REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    CONSTRAINT uq_oa_note_tag_sequence UNIQUE (artifact_id, sequence_no),
    CONSTRAINT ck_oa_note_tag_source CHECK (source_kind IN ('frontmatter', 'inline'))
);

CREATE INDEX ix_oa_note_tag_artifact_id
    ON oa_artifact_note_tag (artifact_id);

CREATE INDEX ix_oa_note_tag_normalized
    ON oa_artifact_note_tag (normalized_tag);

CREATE TABLE oa_artifact_note_alias (
    artifact_note_alias_id VARCHAR2(64 CHAR) NOT NULL,
    artifact_id VARCHAR2(64 CHAR) NOT NULL,
    alias_text VARCHAR2(4000 CHAR) NOT NULL,
    normalized_alias VARCHAR2(4000 CHAR) NOT NULL,
    sequence_no NUMBER(10) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_oa_artifact_note_alias PRIMARY KEY (artifact_note_alias_id),
    CONSTRAINT fk_oa_note_alias_artifact FOREIGN KEY (artifact_id)
        REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    CONSTRAINT uq_oa_note_alias_sequence UNIQUE (artifact_id, sequence_no)
);

CREATE INDEX ix_oa_note_alias_artifact_id
    ON oa_artifact_note_alias (artifact_id);

CREATE INDEX ix_oa_note_alias_normalized
    ON oa_artifact_note_alias (normalized_alias);

CREATE TABLE oa_artifact_note_link (
    artifact_note_link_id VARCHAR2(64 CHAR) NOT NULL,
    artifact_id VARCHAR2(64 CHAR) NOT NULL,
    source_segment_id VARCHAR2(64 CHAR),
    link_kind VARCHAR2(32 CHAR) NOT NULL,
    target_kind VARCHAR2(32 CHAR) NOT NULL,
    raw_target VARCHAR2(4000 CHAR) NOT NULL,
    normalized_target VARCHAR2(4000 CHAR),
    display_text VARCHAR2(4000 CHAR),
    target_path VARCHAR2(4000 CHAR),
    target_heading VARCHAR2(4000 CHAR),
    target_block VARCHAR2(4000 CHAR),
    external_url VARCHAR2(4000 CHAR),
    resolved_artifact_id VARCHAR2(64 CHAR),
    resolution_status VARCHAR2(32 CHAR) NOT NULL,
    locator_json CLOB,
    sequence_no NUMBER(10) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_oa_artifact_note_link PRIMARY KEY (artifact_note_link_id),
    CONSTRAINT fk_oa_note_link_artifact FOREIGN KEY (artifact_id)
        REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    CONSTRAINT fk_oa_note_link_segment FOREIGN KEY (source_segment_id)
        REFERENCES oa_segment (segment_id) ON DELETE SET NULL,
    CONSTRAINT fk_oa_note_link_resolved_artifact FOREIGN KEY (resolved_artifact_id)
        REFERENCES oa_artifact (artifact_id) ON DELETE SET NULL,
    CONSTRAINT uq_oa_note_link_sequence UNIQUE (artifact_id, sequence_no),
    CONSTRAINT ck_oa_note_link_kind CHECK (link_kind IN ('link', 'embed')),
    CONSTRAINT ck_oa_note_link_target_kind CHECK (
        target_kind IN ('note', 'heading', 'block', 'external', 'attachment')
    ),
    CONSTRAINT ck_oa_note_link_resolution_status CHECK (
        resolution_status IN ('resolved', 'unresolved', 'external')
    ),
    CONSTRAINT ck_oa_note_link_locator_json CHECK (
        locator_json IS NULL OR locator_json IS JSON
    )
);

CREATE INDEX ix_oa_note_link_artifact_id
    ON oa_artifact_note_link (artifact_id);

CREATE INDEX ix_oa_note_link_resolved_artifact_id
    ON oa_artifact_note_link (resolved_artifact_id);

CREATE INDEX ix_oa_note_link_normalized_target
    ON oa_artifact_note_link (normalized_target);
