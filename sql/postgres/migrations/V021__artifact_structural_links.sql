CREATE TABLE oa_artifact_link (
    artifact_link_id TEXT PRIMARY KEY,
    source_artifact_id TEXT NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    target_artifact_id TEXT NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    link_type TEXT NOT NULL CHECK (link_type IN ('wikilink', 'shared_tag', 'alias')),
    link_value TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_oa_artifact_link_no_self_ref CHECK (source_artifact_id <> target_artifact_id),
    CONSTRAINT uq_oa_artifact_link_edge UNIQUE (
        source_artifact_id,
        target_artifact_id,
        link_type,
        link_value
    )
);

CREATE INDEX ix_oa_artifact_link_source
    ON oa_artifact_link (source_artifact_id);

CREATE INDEX ix_oa_artifact_link_target
    ON oa_artifact_link (target_artifact_id);

CREATE INDEX ix_oa_artifact_link_type
    ON oa_artifact_link (link_type);
