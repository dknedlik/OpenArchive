CREATE TABLE oa_artifact_link (
    artifact_link_id VARCHAR2(64 CHAR) NOT NULL,
    source_artifact_id VARCHAR2(64 CHAR) NOT NULL,
    target_artifact_id VARCHAR2(64 CHAR) NOT NULL,
    link_type VARCHAR2(32 CHAR) NOT NULL,
    link_value VARCHAR2(4000 CHAR) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_oa_artifact_link PRIMARY KEY (artifact_link_id),
    CONSTRAINT fk_oa_artifact_link_source FOREIGN KEY (source_artifact_id)
        REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    CONSTRAINT fk_oa_artifact_link_target FOREIGN KEY (target_artifact_id)
        REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    CONSTRAINT ck_oa_artifact_link_type CHECK (
        link_type IN ('wikilink', 'shared_tag', 'alias')
    ),
    CONSTRAINT ck_oa_artifact_link_no_self_ref CHECK (
        source_artifact_id <> target_artifact_id
    ),
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
