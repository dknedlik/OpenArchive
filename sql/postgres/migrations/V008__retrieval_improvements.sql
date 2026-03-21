-- V008: Retrieval improvements
--
-- 1. GIN index on oa_derived_object.object_json for JSONB containment queries
-- 2. Generated column extracting candidate_key from object_json (memories/entities)
-- 3. Index on oa_artifact.captured_at for temporal queries
-- 4. New oa_archive_link table for cross-object links (agent write-back)
-- 5. Expand origin_kind CHECK on oa_derived_object to include 'agent_contributed'

-- 1. GIN index for JSONB containment queries (@>, ?)
CREATE INDEX ix_oa_derived_object_object_json ON oa_derived_object USING GIN (object_json);

-- 2. Generated column + partial index for candidate_key lookups
ALTER TABLE oa_derived_object ADD COLUMN candidate_key TEXT
    GENERATED ALWAYS AS (object_json->>'candidate_key') STORED;
CREATE INDEX ix_oa_derived_object_candidate_key ON oa_derived_object (candidate_key)
    WHERE candidate_key IS NOT NULL;

-- 3. Temporal index on artifact capture time
CREATE INDEX ix_oa_artifact_captured_at ON oa_artifact (captured_at);

-- 4. Archive link table — cross-object relationships contributed by consuming agents
CREATE TABLE oa_archive_link (
    archive_link_id   TEXT        NOT NULL PRIMARY KEY,
    source_object_id  TEXT        NOT NULL REFERENCES oa_derived_object(derived_object_id),
    target_object_id  TEXT        NOT NULL REFERENCES oa_derived_object(derived_object_id),
    link_type         TEXT        NOT NULL,
    confidence_score  NUMERIC(5,4),
    rationale         TEXT,
    origin_kind       TEXT        NOT NULL DEFAULT 'agent_contributed',
    contributed_by    TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_oa_archive_link_type CHECK (link_type IN ('same_as', 'continues', 'contradicts', 'same_topic', 'refers_to'))
);

CREATE INDEX ix_oa_archive_link_source ON oa_archive_link (source_object_id);
CREATE INDEX ix_oa_archive_link_target ON oa_archive_link (target_object_id);
CREATE INDEX ix_oa_archive_link_type ON oa_archive_link (link_type);

-- One canonical link per (source, target, type) triple.
-- Agents confirming an existing link should update the row, not duplicate it.
CREATE UNIQUE INDEX uq_oa_archive_link_edge
    ON oa_archive_link (source_object_id, target_object_id, link_type);

-- Prevent self-referential links.
ALTER TABLE oa_archive_link
    ADD CONSTRAINT ck_oa_archive_link_no_self_ref
    CHECK (source_object_id <> target_object_id);

-- 5. Expand origin_kind CHECK on oa_derived_object to include 'agent_contributed'
--    V001 defined an inline CHECK; Postgres names it oa_derived_object_origin_kind_check.
ALTER TABLE oa_derived_object
    DROP CONSTRAINT oa_derived_object_origin_kind_check;

ALTER TABLE oa_derived_object
    ADD CONSTRAINT oa_derived_object_origin_kind_check
    CHECK (origin_kind IN ('explicit', 'deterministic', 'inferred', 'user_confirmed', 'agent_contributed'));
