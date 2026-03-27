use crate::config::PostgresConfig;
use crate::error::StorageResult;
use crate::postgres_db::SharedPostgresClient;
use crate::storage::writeback_store::{
    NewAgentEntity, NewAgentMemory, NewArchiveLink, UpdateObjectStatus, WritebackStore,
};

use super::writeback;

pub struct PostgresWritebackStore {
    client: SharedPostgresClient,
}

impl PostgresWritebackStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self {
            client: SharedPostgresClient::new(config),
        }
    }
}

impl WritebackStore for PostgresWritebackStore {
    fn store_agent_memory(&self, memory: &NewAgentMemory) -> StorageResult<()> {
        self.client.with_client(|client| {
            writeback::store_agent_memory(client, self.client.connection_string(), memory)
        })
    }

    fn store_archive_link(&self, link: &NewArchiveLink) -> StorageResult<()> {
        self.client.with_client(|client| {
            writeback::store_archive_link(client, self.client.connection_string(), link)
        })
    }

    fn update_object_status(&self, update: &UpdateObjectStatus) -> StorageResult<()> {
        self.client.with_client(|client| {
            writeback::update_object_status(client, self.client.connection_string(), update)
        })
    }

    fn store_agent_entity(&self, entity: &NewAgentEntity) -> StorageResult<()> {
        self.client.with_client(|client| {
            writeback::store_agent_entity(client, self.client.connection_string(), entity)
        })
    }
}
