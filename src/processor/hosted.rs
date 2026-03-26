use std::sync::Arc;

use super::*;

pub(crate) trait InferenceClient: Send + Sync {
    fn complete_json(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
    ) -> Result<InferenceResult, ProcessorError>;
}

#[derive(Debug, Clone)]
pub(crate) struct InferenceResult {
    pub(crate) output_text: String,
    pub(crate) usage: Option<InferenceUsage>,
}

pub(crate) trait EnrichmentStrategy: Send + Sync {
    fn prompt_version(&self) -> &'static str;
    fn process(
        &self,
        processor: &HostedArtifactProcessor,
        input: &ArtifactProcessorInput,
    ) -> Result<ArtifactProcessorOutput, ProcessorError>;
}

pub(crate) struct HostedArtifactProcessor {
    pub(crate) client: Arc<dyn InferenceClient>,
    pub(crate) model: String,
    pub(crate) pipeline_name: &'static str,
    pub(crate) provider_name: &'static str,
    pub(crate) strategy: Arc<dyn EnrichmentStrategy>,
}

pub(crate) struct HostedReconciliationProcessor {
    pub(crate) client: Arc<dyn InferenceClient>,
    pub(crate) model: String,
    pub(crate) system_prompt: &'static str,
}

impl ArtifactProcessor for HostedArtifactProcessor {
    fn process(
        &self,
        input: &ArtifactProcessorInput,
    ) -> Result<ArtifactProcessorOutput, ProcessorError> {
        validate_input(input)?;
        self.strategy.process(self, input)
    }
}

impl HostedArtifactProcessor {
    fn run_prompt(
        &self,
        input: &ArtifactProcessorInput,
        user_prompt: &str,
    ) -> Result<ArtifactProcessorOutput, ProcessorError> {
        match self.process_once(input, user_prompt) {
            Ok(output) => Ok(output),
            Err(error) if should_retry_with_repair(&error) => {
                let repair_prompt = build_repair_prompt(user_prompt, &error);
                self.process_once(input, &repair_prompt)
            }
            Err(error) => Err(error),
        }
    }

    fn process_once(
        &self,
        input: &ArtifactProcessorInput,
        user_prompt: &str,
    ) -> Result<ArtifactProcessorOutput, ProcessorError> {
        let candidate_result = self.client.complete_json(
            &self.model,
            candidate_system_prompt(input),
            user_prompt,
            &candidate_output_schema_wrapper(input),
        )?;
        let candidate = parse_candidate_output(&candidate_result.output_text, input)
            .map_err(|err| attach_output_preview(err, &candidate_result.output_text))?;
        Ok(candidate.into_processor_output(
            input,
            self.model.clone(),
            candidate_result.usage,
            self.pipeline_name,
            self.provider_name,
            self.strategy.prompt_version(),
        ))
    }
}

impl HostedReconciliationProcessor {
    fn reconcile_once(
        &self,
        input: &ReconciliationProcessorInput,
        user_prompt: &str,
    ) -> Result<Vec<ReconciliationDecisionOutput>, ProcessorError> {
        let inference_result = self.client.complete_json(
            &self.model,
            self.system_prompt,
            user_prompt,
            &reconciliation_output_schema_wrapper(),
        )?;
        let parsed: ModelReconciliationOutput = serde_json::from_str(&inference_result.output_text)
            .map_err(|source| ProcessorError::ParseModelJson {
                source,
                body_preview: preview(&inference_result.output_text),
            })?;
        parsed.into_validated_outputs(input)
    }
}

impl ReconciliationProcessor for HostedReconciliationProcessor {
    fn reconcile(
        &self,
        input: &ReconciliationProcessorInput,
    ) -> Result<Vec<ReconciliationDecisionOutput>, ProcessorError> {
        let prompt = build_reconciliation_prompt(input)?;
        match self.reconcile_once(input, &prompt) {
            Ok(output) => Ok(output),
            Err(error) if should_retry_with_repair(&error) => {
                let repair_prompt = build_repair_prompt(&prompt, &error);
                self.reconcile_once(input, &repair_prompt)
            }
            Err(error) => Err(error),
        }
    }
}

pub(crate) struct ConversationEnrichmentStrategy {
    prompt_version: &'static str,
}

impl ConversationEnrichmentStrategy {
    pub(crate) fn anthropic_default() -> Arc<dyn EnrichmentStrategy> {
        Arc::new(Self {
            prompt_version: ANTHROPIC_PROMPT_VERSION,
        })
    }
}

impl EnrichmentStrategy for ConversationEnrichmentStrategy {
    fn prompt_version(&self) -> &'static str {
        self.prompt_version
    }

    fn process(
        &self,
        processor: &HostedArtifactProcessor,
        input: &ArtifactProcessorInput,
    ) -> Result<ArtifactProcessorOutput, ProcessorError> {
        let prompt = build_two_phase_candidate_user_prompt(input)?;
        processor.run_prompt(input, &prompt)
    }
}
