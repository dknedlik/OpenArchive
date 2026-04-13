-- Collapse enrichment job routing to a single runtime tier.

BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE oa_enrichment_job DROP CONSTRAINT ck_oa_job_tier';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -2443 THEN
            RAISE;
        END IF;
END;
/

UPDATE oa_enrichment_job
SET enrichment_tier = 'default'
WHERE enrichment_tier IN ('standard', 'quality');

ALTER TABLE oa_enrichment_job MODIFY (
    enrichment_tier DEFAULT 'default'
);

ALTER TABLE oa_enrichment_job ADD CONSTRAINT ck_oa_job_tier CHECK (
    enrichment_tier = 'default'
);
