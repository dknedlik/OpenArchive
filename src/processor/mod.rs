mod anthropic;
mod artifact_classifier;
mod artifact_policy;
mod extraction_pipeline;
mod gemini;
mod grok;
mod hosted;
mod oci;
mod openai;
mod pipeline;
mod stub;
mod types;

#[cfg(test)]
mod tests;

pub use anthropic::AnthropicProcessorFactory;
pub use artifact_classifier::{
    classify_artifact, classify_artifact_debug, ArchetypeScore, ArtifactArchetype,
    ArtifactClassificationDebug, ArtifactClassificationProfile, ArtifactFacet, FacetScore,
};
pub use gemini::{
    GeminiBatchClient, GeminiBatchEnrichmentRequest, GeminiBatchJob, GeminiProcessorFactory,
};
pub use grok::GrokProcessorFactory;
pub use oci::OciProcessorFactory;
pub use openai::OpenAiProcessorFactory;
pub use pipeline::{memory_candidate_key_from_fields, structured_output_schema, ProcessorError};
pub use stub::{StubProcessor, StubProcessorFactory, StubReconciliationProcessor};
pub use types::{
    ArtifactBatchProcessor, ArtifactProcessor, ArtifactProcessorFactory, ArtifactProcessorInput,
    ArtifactProcessorOutput, BatchHandle, BatchPollResult, ClassificationOutput, EntityOutput,
    ExtractionBatchSubmitter, InferenceUsage, MemoryOutput, ReconciliationBatchProcessor,
    ReconciliationBatchSubmitter, ReconciliationDecisionOutput, ReconciliationProcessor,
    ReconciliationProcessorInput, RelationshipOutput, SummaryOutput,
};

pub(crate) use extraction_pipeline::{
    ExtractionPipelineProcessor, SequentialArtifactBatchProcessor,
};
pub(crate) use hosted::{HostedReconciliationProcessor, InferenceClient, InferenceResult};
pub(crate) use openai::{
    OpenRouterResponsesContentItem, OpenRouterResponsesInputItem, OpenRouterResponsesRequest,
    OpenRouterResponsesResponse, OpenRouterResponsesTextConfig, OpenRouterUsage,
};
pub(crate) use pipeline::{
    allowed_artifact_evidence_refs, attach_output_preview, build_reconciliation_prompt,
    build_repair_prompt, build_two_phase_candidate_user_prompt,
    build_two_phase_candidate_user_prompt_with_flavor, candidate_output_schema_with_allowed_refs,
    candidate_output_schema_wrapper, candidate_system_prompt, cleanup_artifact_processor_output,
    parse_candidate_output, preview, reconciliation_output_schema,
    reconciliation_output_schema_wrapper, should_retry_with_repair, should_shape_artifact_input,
    validate_input, ModelReconciliationOutput, PromptFlavor, ANTHROPIC_PROMPT_VERSION,
    GEMINI_PROMPT_VERSION, GROK_PROMPT_VERSION, OPENAI_PROMPT_VERSION,
    RECONCILIATION_SYSTEM_PROMPT, TWO_PHASE_CANDIDATE_MAX_OUTPUT_TOKENS,
};
#[cfg(test)]
pub(crate) use pipeline::{
    build_conversation_user_prompt, build_conversation_user_prompt_with_flavor,
    build_document_user_prompt, build_document_user_prompt_with_flavor, canonicalize_memory_type,
    canonicalize_memory_type_for_input, ModelReconciliationDecision,
};
pub(crate) use types::{
    artifact_processor_batch_custom_id, MAX_CLASSIFICATIONS, MAX_MEMORIES, MAX_RETRIEVAL_INTENTS,
    MEMORY_TYPE_VALUES,
};
