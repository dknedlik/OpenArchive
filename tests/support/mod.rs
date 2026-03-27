#![allow(dead_code)]
#![allow(unused_imports)]

mod contracts;
mod fixtures;
mod harness;

pub use contracts::{
    contract_claim_complete_happy_path, contract_claim_fail_terminal,
    contract_claim_retryable_reclaim_complete, contract_claim_returns_none_when_empty,
    contract_claim_skips_future_available_at, contract_concurrent_claim_protection,
    contract_non_claiming_worker_cannot_complete_job, contract_payload_matches_documented_schema,
    contract_rejects_cross_artifact_evidence_links_without_writing_rows,
    contract_rerun_supersedes_previous_active_objects,
    contract_retryable_exhausted_becomes_terminal,
    contract_rolls_back_partial_writes_when_evidence_insert_fails,
    contract_write_import_duplicate_artifact_hash_is_idempotent,
    contract_write_import_duplicate_payload_is_idempotent,
    contract_write_import_partial_success_finalizes_completed_with_errors,
    contract_write_single_import_happy_path,
    contract_writes_summary_classification_and_memory_with_evidence,
};
pub use fixtures::{
    lock_live_test, make_test_import_fixture, make_test_import_fixture_with_max_attempts,
    payload_sha256, seed_postgres_stub_derivations, sha256_hex, unique_suffix, TestImportFixture,
};
pub use harness::{
    DerivationRunRecord, DerivedMetadataHarness, ImportRecord, JobRecord, ProviderHarness,
};
