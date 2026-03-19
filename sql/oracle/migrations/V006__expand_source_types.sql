ALTER TABLE oa_import DROP CONSTRAINT ck_oa_import_source_type;

ALTER TABLE oa_import ADD CONSTRAINT ck_oa_import_source_type CHECK (
    source_type IN (
        'chatgpt_export',
        'claude_export',
        'grok_export',
        'gemini_takeout'
    )
);

ALTER TABLE oa_artifact DROP CONSTRAINT ck_oa_artifact_source_type;

ALTER TABLE oa_artifact ADD CONSTRAINT ck_oa_artifact_source_type CHECK (
    source_type IN (
        'chatgpt_export',
        'claude_export',
        'grok_export',
        'gemini_takeout'
    )
);
