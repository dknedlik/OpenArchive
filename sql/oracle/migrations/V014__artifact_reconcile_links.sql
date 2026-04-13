ALTER TABLE oa_artifact_link DROP CONSTRAINT ck_oa_artifact_link_type;

ALTER TABLE oa_artifact_link ADD CONSTRAINT ck_oa_artifact_link_type CHECK (
    link_type IN ('wikilink', 'shared_tag', 'alias', 'reconciled_object')
);

MERGE INTO oa_artifact_link t
USING (
    SELECT
        'artlink-backfill-' || SUBSTR(STANDARD_HASH(al.archive_link_id, 'SHA256'), 1, 24) AS artifact_link_id,
        src.artifact_id AS source_artifact_id,
        tgt.artifact_id AS target_artifact_id,
        'reconciled_object' AS link_type,
        al.link_type || ':' || src.derived_object_type || ':' ||
            COALESCE(NULLIF(src.title, ''), NULLIF(src.candidate_key, ''), src.derived_object_id) AS link_value
    FROM oa_archive_link al
    JOIN oa_derived_object src
      ON src.derived_object_id = al.source_object_id
    JOIN oa_derived_object tgt
      ON tgt.derived_object_id = al.target_object_id
    WHERE src.artifact_id <> tgt.artifact_id
) s
ON (
    t.source_artifact_id = s.source_artifact_id
    AND t.target_artifact_id = s.target_artifact_id
    AND t.link_type = s.link_type
    AND t.link_value = s.link_value
)
WHEN NOT MATCHED THEN INSERT (
    artifact_link_id,
    source_artifact_id,
    target_artifact_id,
    link_type,
    link_value
) VALUES (
    s.artifact_link_id,
    s.source_artifact_id,
    s.target_artifact_id,
    s.link_type,
    s.link_value
);
