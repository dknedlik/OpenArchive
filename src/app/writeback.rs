use std::sync::Arc;

use crate::error::{OpenArchiveError, Result};
use crate::storage::types::ObjectStatus;
use crate::storage::writeback_store::{
    NewAgentEntity, NewAgentMemory, NewArchiveLink, UpdateObjectStatus, WritebackStore,
};

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

    pub fn update_object_status(&self, update: UpdateObjectStatus) -> Result<()> {
        // Validate: new_status must be Superseded or Rejected
        if update.new_status != ObjectStatus::Superseded
            && update.new_status != ObjectStatus::Rejected
        {
            return Err(OpenArchiveError::Invariant(
                "update_object_status only accepts Superseded or Rejected".to_string(),
            ));
        }
        // Validate: replacement_object_id only valid with Superseded
        if update.replacement_object_id.is_some() && update.new_status != ObjectStatus::Superseded {
            return Err(OpenArchiveError::Invariant(
                "replacement_object_id is only valid when superseding".to_string(),
            ));
        }
        self.store
            .update_object_status(&update)
            .map_err(OpenArchiveError::from)
    }

    pub fn store_entity(&self, entity: NewAgentEntity) -> Result<String> {
        let id = entity.derived_object_id.clone();
        self.store
            .store_agent_entity(&entity)
            .map_err(OpenArchiveError::from)?;
        Ok(id)
    }
}
