-- Segment-level evidence links are no longer part of the active provenance model.
-- New extraction, reconciliation, and writeback flows do not read or write this table.

DROP TABLE IF EXISTS oa_evidence_link;
