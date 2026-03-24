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
            'markdown_file'
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
            'markdown_file'
        )
    );

ALTER TABLE oa_artifact
    DROP CONSTRAINT IF EXISTS oa_artifact_artifact_class_check;

ALTER TABLE oa_artifact
    ADD CONSTRAINT oa_artifact_artifact_class_check
    CHECK (artifact_class IN ('conversation', 'document'));

UPDATE oa_segment
SET segment_type = CASE segment_type
    WHEN 'message' THEN 'content_block'
    WHEN 'message_window' THEN 'content_window'
    ELSE segment_type
END
WHERE segment_type IN ('message', 'message_window');

ALTER TABLE oa_segment
    DROP CONSTRAINT IF EXISTS oa_segment_segment_type_check;

ALTER TABLE oa_segment
    ADD CONSTRAINT oa_segment_segment_type_check
    CHECK (segment_type IN ('content_block', 'content_window'));

ALTER TABLE oa_segment
    DROP CONSTRAINT IF EXISTS ck_oa_segment_message_text;

ALTER TABLE oa_segment
    ADD CONSTRAINT ck_oa_segment_content_block_text
    CHECK (segment_type <> 'content_block' OR text_content IS NOT NULL);
