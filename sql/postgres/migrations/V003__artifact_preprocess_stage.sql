DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT c.conname
        FROM pg_constraint c
        JOIN pg_class r ON r.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = 'public'
          AND r.relname = 'oa_enrichment_job'
          AND c.contype = 'c'
          AND pg_get_constraintdef(c.oid) LIKE '%job_type%'
    LOOP
        EXECUTE format(
            'ALTER TABLE oa_enrichment_job DROP CONSTRAINT %I',
            constraint_name
        );
    END LOOP;
END
$$;

ALTER TABLE oa_enrichment_job
    ADD CONSTRAINT ck_oa_job_type CHECK (
        job_type IN ('artifact_preprocess', 'artifact_enrichment')
    );
