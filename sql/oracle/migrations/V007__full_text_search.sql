-- Full-text search: Oracle Text CONTEXT indexes on the three tables that
-- contribute to archive search results.
--
-- oa_artifact and oa_segment get simple CONTEXT indexes.
-- oa_derived_object combines title + body_text via MULTI_COLUMN_DATASTORE,
-- mirroring the weighted tsvector used on the Postgres side.
--
-- SYNC (ON COMMIT) keeps indexes current without manual sync calls.

CREATE INDEX ix_oa_artifact_title_ctx ON oa_artifact (title)
    INDEXTYPE IS CTXSYS.CONTEXT
    PARAMETERS ('SYNC (ON COMMIT)');

CREATE INDEX ix_oa_segment_text_content_ctx ON oa_segment (text_content)
    INDEXTYPE IS CTXSYS.CONTEXT
    PARAMETERS ('SYNC (ON COMMIT)');

BEGIN
    CTX_DDL.CREATE_PREFERENCE('oa_derived_obj_store', 'MULTI_COLUMN_DATASTORE');
    CTX_DDL.SET_ATTRIBUTE('oa_derived_obj_store', 'COLUMNS', 'title, body_text');
END;
/

CREATE INDEX ix_oa_derived_object_fts ON oa_derived_object (title)
    INDEXTYPE IS CTXSYS.CONTEXT
    PARAMETERS ('DATASTORE oa_derived_obj_store SYNC (ON COMMIT)');
