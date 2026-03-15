use std::sync::Arc;

use crate::error::OpenArchiveError;
use crate::import_service::{import_chatgpt_payload, ImportResponse};
use crate::object_store::ObjectStore;
use crate::storage::ImportWriteStore;

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
}
