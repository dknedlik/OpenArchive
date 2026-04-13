use super::*;

#[path = "pipeline_cleanup.rs"]
mod cleanup;
#[path = "pipeline_error.rs"]
mod error;
#[path = "pipeline_prompts.rs"]
mod prompts;
#[path = "pipeline_reconciliation.rs"]
mod reconciliation;
#[path = "pipeline_schema.rs"]
mod schema;

pub use error::ProcessorError;
pub use reconciliation::memory_candidate_key_from_fields;

pub(crate) use cleanup::{
    canonicalize_entity_type, cleanup_artifact_processor_output, normalize_candidate_key_text,
    normalize_optional_text,
};
pub(crate) use prompts::{
    attach_output_preview, build_reconciliation_prompt, build_repair_prompt, preview,
    should_retry_with_repair, RECONCILIATION_SYSTEM_PROMPT,
};
pub(crate) use reconciliation::{validate_input, ModelReconciliationOutput};
pub(crate) use schema::{reconciliation_output_schema, reconciliation_output_schema_wrapper};

#[cfg(test)]
pub(crate) use reconciliation::ModelReconciliationDecision;
