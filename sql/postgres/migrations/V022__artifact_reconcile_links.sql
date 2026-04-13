ALTER TABLE oa_artifact_link
    DROP CONSTRAINT IF EXISTS oa_artifact_link_link_type_check;

ALTER TABLE oa_artifact_link
    ADD CONSTRAINT oa_artifact_link_link_type_check
    CHECK (link_type IN ('wikilink', 'shared_tag', 'alias', 'reconciled_object'));

INSERT INTO oa_artifact_link (
    artifact_link_id,
    source_artifact_id,
    target_artifact_id,
    link_type,
    link_value
)
SELECT
    'artlink-backfill-' || substr(md5(al.archive_link_id), 1, 24),
    src.artifact_id,
    tgt.artifact_id,
    'reconciled_object',
    al.link_type || ':' || src.derived_object_type || ':' ||
        COALESCE(NULLIF(src.title, ''), NULLIF(src.candidate_key, ''), src.derived_object_id)
FROM oa_archive_link al
JOIN oa_derived_object src
  ON src.derived_object_id = al.source_object_id
JOIN oa_derived_object tgt
  ON tgt.derived_object_id = al.target_object_id
WHERE src.artifact_id <> tgt.artifact_id
ON CONFLICT (source_artifact_id, target_artifact_id, link_type, link_value) DO NOTHING;
