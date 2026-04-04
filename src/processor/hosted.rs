use std::sync::Arc;

use super::*;

pub(crate) trait InferenceClient: Send + Sync {
    fn complete_text(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
    ) -> Result<InferenceResult, ProcessorError>;

    fn complete_text_with_max_output_tokens_override(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        _max_output_tokens: u32,
    ) -> Result<InferenceResult, ProcessorError> {
        self.complete_text(model, system_prompt, user_prompt)
    }

    fn complete_json(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
    ) -> Result<InferenceResult, ProcessorError>;

    fn complete_json_with_max_output_tokens_override(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
        _max_output_tokens: u32,
    ) -> Result<InferenceResult, ProcessorError> {
        self.complete_json(model, system_prompt, user_prompt, schema)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InferenceResult {
    pub(crate) output_text: String,
    pub(crate) usage: Option<InferenceUsage>,
}

pub(crate) struct HostedReconciliationProcessor {
    pub(crate) client: Arc<dyn InferenceClient>,
    pub(crate) model: String,
    pub(crate) system_prompt: &'static str,
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
