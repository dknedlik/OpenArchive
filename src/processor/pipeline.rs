use super::*;

#[path = "pipeline_candidate.rs"]
mod candidate;
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
pub use schema::structured_output_schema;

pub(crate) use candidate::ModelCandidateArtifactOutput;
pub(crate) use cleanup::{
    canonicalize_entity_type, canonicalize_memory_type_for_input, cleanup_artifact_processor_output,
    normalize_candidate_key_text, normalize_optional_text,
};
#[cfg(test)]
pub(crate) use cleanup::canonicalize_memory_type;
pub(crate) use super::artifact_policy::extraction_policy_for;
pub(crate) use prompts::{
    allowed_artifact_evidence_refs, attach_output_preview, build_reconciliation_prompt,
    build_repair_prompt, build_segment_alias_map, build_two_phase_candidate_user_prompt,
    candidate_system_prompt, preview, should_retry_with_repair, should_shape_artifact_input,
    ANTHROPIC_PROMPT_VERSION, GEMINI_PROMPT_VERSION, GROK_PROMPT_VERSION, OPENAI_PROMPT_VERSION,
    RECONCILIATION_SYSTEM_PROMPT, TWO_PHASE_CANDIDATE_MAX_OUTPUT_TOKENS,
};
pub(crate) use reconciliation::{
    retain_valid_items, validate_evidence_ids, validate_input, validate_text_field,
    ModelReconciliationOutput,
};
pub(crate) use schema::{
    candidate_output_schema_with_allowed_refs, candidate_output_schema_wrapper,
    parse_candidate_output, reconciliation_output_schema, reconciliation_output_schema_wrapper,
};

#[cfg(test)]
pub(crate) use prompts::{build_conversation_user_prompt, build_document_user_prompt};
#[cfg(test)]
pub(crate) use reconciliation::ModelReconciliationDecision;
