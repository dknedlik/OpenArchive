-- Segment-level evidence links are no longer part of the active provenance model.
-- New extraction, reconciliation, and writeback flows do not read or write this table.

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE oa_evidence_link';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;
/
