use thiserror::Error;

const DEFAULT_RETRYABLE_BACKOFF_SECONDS: i64 = 60;
const DEFAULT_RATE_LIMIT_BACKOFF_SECONDS: i64 = 300;
const MAX_RATE_LIMIT_BACKOFF_SECONDS: i64 = 1800;

#[derive(Debug, Error)]
pub enum ProcessorError {
    #[error("unsupported enrichment tier {tier}")]
    UnsupportedTier { tier: String },

    #[error("invalid processor input: {detail}")]
    InvalidInput { detail: String },

    #[error("failed to serialize processor prompt")]
    SerializePrompt {
        #[source]
        source: serde_json::Error,
    },

    #[error("failed to build inference HTTP client")]
    BuildHttpClient {
        #[source]
        source: reqwest::Error,
    },

    #[error("failed to send inference request")]
    SendInferenceRequest {
        #[source]
        source: reqwest::Error,
    },

    #[error("failed to read inference response body")]
    ReadInferenceResponse {
        #[source]
        source: reqwest::Error,
    },

    #[error("inference returned HTTP status {status}: {body_preview}")]
    InferenceHttpStatus {
        status: u16,
        body_preview: String,
        retry_after_seconds: Option<i64>,
    },

    #[error("failed to parse inference response JSON: {body_preview}")]
    ParseInferenceResponse {
        #[source]
        source: serde_json::Error,
        body_preview: String,
    },

    #[error("failed to parse model output JSON: {body_preview}")]
    ParseModelJson {
        #[source]
        source: serde_json::Error,
        body_preview: String,
    },

    #[error("invalid model output: {detail}")]
    InvalidModelOutput { detail: String },

    #[error("{message}")]
    Message { message: String },
}

impl ProcessorError {
    pub(crate) fn compact_reason(&self) -> String {
        match self {
            ProcessorError::ParseModelJson { body_preview, .. } => {
                format!("response was not valid JSON; preview: {body_preview}")
            }
            ProcessorError::InvalidModelOutput { detail } => detail.clone(),
            other => other.to_string(),
        }
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            ProcessorError::SendInferenceRequest { .. }
            | ProcessorError::ReadInferenceResponse { .. }
            | ProcessorError::ParseInferenceResponse { .. } => true,
            ProcessorError::InferenceHttpStatus { status, .. } => {
                matches!(*status, 408 | 409 | 425 | 429 | 500..=599)
            }
            _ => false,
        }
    }

    pub fn should_reschedule_without_attempt(&self) -> bool {
        matches!(
            self,
            ProcessorError::InferenceHttpStatus { status: 429, .. }
        )
    }

    pub fn retry_after_seconds(&self) -> Option<i64> {
        match self {
            ProcessorError::InferenceHttpStatus {
                retry_after_seconds,
                ..
            } => *retry_after_seconds,
            _ => None,
        }
    }

    pub fn recommended_retry_after_seconds(&self) -> i64 {
        match self {
            ProcessorError::InferenceHttpStatus { status: 429, .. } => self
                .retry_after_seconds()
                .unwrap_or(DEFAULT_RATE_LIMIT_BACKOFF_SECONDS),
            _ => DEFAULT_RETRYABLE_BACKOFF_SECONDS,
        }
    }

    pub fn recommended_stage_backoff_seconds(&self, consecutive_rate_limits: u32) -> Option<i64> {
        if !self.should_reschedule_without_attempt() {
            return None;
        }

        let base = self.recommended_retry_after_seconds().max(1);
        let multiplier_shift = consecutive_rate_limits.min(3);
        let multiplier = 1_i64.checked_shl(multiplier_shift).unwrap_or(8);
        Some(
            base.saturating_mul(multiplier)
                .min(MAX_RATE_LIMIT_BACKOFF_SECONDS),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::ProcessorError;

    #[test]
    fn quota_http_status_reschedules_without_consuming_attempt() {
        let err = ProcessorError::InferenceHttpStatus {
            status: 429,
            body_preview: "quota exceeded".to_string(),
            retry_after_seconds: None,
        };

        assert!(err.is_retryable());
        assert!(err.should_reschedule_without_attempt());
        assert_eq!(err.recommended_retry_after_seconds(), 300);
        assert_eq!(err.recommended_stage_backoff_seconds(0), Some(300));
        assert_eq!(err.recommended_stage_backoff_seconds(1), Some(600));
    }

    #[test]
    fn server_errors_remain_retryable_but_consume_attempts() {
        let err = ProcessorError::InferenceHttpStatus {
            status: 503,
            body_preview: "service unavailable".to_string(),
            retry_after_seconds: None,
        };

        assert!(err.is_retryable());
        assert!(!err.should_reschedule_without_attempt());
        assert_eq!(err.recommended_retry_after_seconds(), 60);
        assert_eq!(err.recommended_stage_backoff_seconds(0), None);
    }

    #[test]
    fn explicit_retry_after_hint_is_preserved() {
        let err = ProcessorError::InferenceHttpStatus {
            status: 429,
            body_preview: "quota exceeded".to_string(),
            retry_after_seconds: Some(123),
        };

        assert_eq!(err.retry_after_seconds(), Some(123));
        assert_eq!(err.recommended_retry_after_seconds(), 123);
        assert_eq!(err.recommended_stage_backoff_seconds(0), Some(123));
    }
}
