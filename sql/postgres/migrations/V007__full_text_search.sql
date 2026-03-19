-- Full-text search: add tsvector generated columns + GIN indexes to the three
-- tables that contribute to archive search results.
--
-- english dictionary: stems words (run/running/ran → run), removes stop words.
-- Weights: A = title, B = body / segment text (ts_rank respects weight tiers).

ALTER TABLE oa_artifact
    ADD COLUMN title_tsv tsvector
        GENERATED ALWAYS AS (to_tsvector('english', coalesce(title, ''))) STORED;

CREATE INDEX ix_oa_artifact_title_tsv ON oa_artifact USING GIN (title_tsv);

ALTER TABLE oa_segment
    ADD COLUMN text_content_tsv tsvector
        GENERATED ALWAYS AS (to_tsvector('english', coalesce(text_content, ''))) STORED;

CREATE INDEX ix_oa_segment_text_content_tsv ON oa_segment USING GIN (text_content_tsv);

ALTER TABLE oa_derived_object
    ADD COLUMN search_tsv tsvector
        GENERATED ALWAYS AS (
            setweight(to_tsvector('english', coalesce(title, '')),     'A') ||
            setweight(to_tsvector('english', coalesce(body_text, '')), 'B')
        ) STORED;

CREATE INDEX ix_oa_derived_object_search_tsv ON oa_derived_object USING GIN (search_tsv);
