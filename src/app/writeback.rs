use std::sync::Arc;

use crate::error::{OpenArchiveError, Result};
use crate::storage::writeback_store::{NewAgentMemory, NewArchiveLink, WritebackStore};

pub struct WritebackService {
    store: Arc<dyn WritebackStore + Send + Sync>,
}

impl WritebackService {
    pub fn new(store: Arc<dyn WritebackStore + Send + Sync>) -> Self {
        Self { store }
    }

    pub fn store_memory(&self, memory: NewAgentMemory) -> Result<String> {
        let id = memory.derived_object_id.clone();
        self.store
            .store_agent_memory(&memory)
            .map_err(OpenArchiveError::from)?;
        Ok(id)
    }

    pub fn store_link(&self, link: NewArchiveLink) -> Result<String> {
        let id = link.archive_link_id.clone();
        self.store
            .store_archive_link(&link)
            .map_err(OpenArchiveError::from)?;
        Ok(id)
    }
}
