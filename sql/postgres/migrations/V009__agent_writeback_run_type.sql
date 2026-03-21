-- V009: Expand derivation_run run_type CHECK to include 'agent_contributed'
--
-- Agent write-back creates lightweight derivation runs without a job. The
-- existing CHECK (from V004) only allows pipeline-driven run types.

ALTER TABLE oa_derivation_run
    DROP CONSTRAINT ck_oa_derivation_run_type;

ALTER TABLE oa_derivation_run
    ADD CONSTRAINT ck_oa_derivation_run_type CHECK (
        run_type IN (
            'artifact_extraction',
            'archive_retrieval',
            'artifact_reconciliation',
            'context_pack_assembly',
            'agent_contributed'
        )
    );
