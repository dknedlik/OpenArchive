mod anthropic;
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
pub use gemini::{
    GeminiBatchClient, GeminiBatchEnrichmentRequest, GeminiBatchJob, GeminiProcessorFactory,
};
pub use grok::GrokProcessorFactory;
pub use oci::OciProcessorFactory;
pub use openai::OpenAiProcessorFactory;
pub use pipeline::{memory_candidate_key_from_fields, ProcessorError};
pub use stub::{StubProcessor, StubProcessorFactory, StubReconciliationProcessor};
pub use types::{
    ArtifactBatchProcessor, ArtifactProcessor, ArtifactProcessorFactory, ArtifactProcessorInput,
    ArtifactProcessorOutput, ClassificationOutput, EntityOutput, InferenceUsage, MemoryOutput,
    ReconciliationBatchProcessor, ReconciliationDecisionOutput, ReconciliationProcessor,
    ReconciliationProcessorInput, RelationshipOutput, SummaryOutput,
};

pub(crate) use extraction_pipeline::{
    ExtractionPipelineProcessor, SequentialArtifactBatchProcessor,
};
pub(crate) use hosted::{HostedReconciliationProcessor, InferenceClient, InferenceResult};
pub(crate) use openai::{
    OpenRouterResponsesContentItem, OpenRouterResponsesInputItem, OpenRouterResponsesRequest,
    OpenRouterResponsesResponse, OpenRouterResponsesTextConfig,
};
#[cfg(test)]
pub(crate) use pipeline::ModelReconciliationDecision;
pub(crate) use pipeline::{
    attach_output_preview, build_reconciliation_prompt, build_repair_prompt,
    cleanup_artifact_processor_output, preview, reconciliation_output_schema,
    reconciliation_output_schema_wrapper, should_retry_with_repair, validate_input,
    ModelReconciliationOutput, RECONCILIATION_SYSTEM_PROMPT,
};
