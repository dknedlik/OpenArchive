ALTER TABLE oa_import
    DROP CONSTRAINT IF EXISTS oa_import_source_type_check;

ALTER TABLE oa_import
    ADD CONSTRAINT oa_import_source_type_check
    CHECK (
        source_type IN (
            'chatgpt_export',
            'claude_export',
            'grok_export',
            'gemini_takeout'
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
            'gemini_takeout'
        )
    );
