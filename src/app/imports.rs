use std::sync::Arc;

use crate::error::OpenArchiveError;
use crate::import_service::{
    import_chatgpt_payload, import_claude_payload, import_gemini_payload, import_grok_payload,
    import_markdown_payload, import_payload, import_text_payload, ImportResponse,
};
use crate::object_store::ObjectStore;
use crate::storage::{ImportWriteStore, PayloadFormat, SourceType};

pub struct ImportApplicationService {
    import_store: Arc<dyn ImportWriteStore + Send + Sync>,
    object_store: Arc<dyn ObjectStore + Send + Sync>,
}

impl ImportApplicationService {
    pub fn new(
        import_store: Arc<dyn ImportWriteStore + Send + Sync>,
        object_store: Arc<dyn ObjectStore + Send + Sync>,
    ) -> Self {
        Self {
            import_store,
            object_store,
        }
    }

    pub fn import_chatgpt_payload(
        &self,
        payload_bytes: &[u8],
    ) -> Result<ImportResponse, OpenArchiveError> {
        import_chatgpt_payload(
            self.import_store.as_ref(),
            self.object_store.as_ref(),
            payload_bytes,
        )
    }

    pub fn import_claude_payload(
        &self,
        payload_bytes: &[u8],
    ) -> Result<ImportResponse, OpenArchiveError> {
        import_claude_payload(
            self.import_store.as_ref(),
            self.object_store.as_ref(),
            payload_bytes,
        )
    }

    pub fn import_grok_payload(
        &self,
        payload_bytes: &[u8],
    ) -> Result<ImportResponse, OpenArchiveError> {
        import_grok_payload(
            self.import_store.as_ref(),
            self.object_store.as_ref(),
            payload_bytes,
        )
    }

    pub fn import_gemini_payload(
        &self,
        payload_bytes: &[u8],
    ) -> Result<ImportResponse, OpenArchiveError> {
        import_gemini_payload(
            self.import_store.as_ref(),
            self.object_store.as_ref(),
            payload_bytes,
        )
    }

    pub fn import_text_payload(
        &self,
        payload_bytes: &[u8],
    ) -> Result<ImportResponse, OpenArchiveError> {
        import_text_payload(
            self.import_store.as_ref(),
            self.object_store.as_ref(),
            payload_bytes,
        )
    }

    pub fn import_markdown_payload(
        &self,
        payload_bytes: &[u8],
    ) -> Result<ImportResponse, OpenArchiveError> {
        import_markdown_payload(
            self.import_store.as_ref(),
            self.object_store.as_ref(),
            payload_bytes,
        )
    }

    pub fn import_payload(
        &self,
        source_type: SourceType,
        payload_format: PayloadFormat,
        payload_bytes: &[u8],
    ) -> Result<ImportResponse, OpenArchiveError> {
        import_payload(
            self.import_store.as_ref(),
            self.object_store.as_ref(),
            source_type,
            payload_format,
            payload_bytes,
        )
    }
}
