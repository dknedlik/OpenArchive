use crate::config::PostgresConfig;
use crate::error::{DbError, DbResult};
use std::sync::{Arc, Mutex};

pub fn connect(config: &PostgresConfig) -> DbResult<postgres::Client> {
    postgres::Client::connect(&config.connection_string, postgres::NoTls).map_err(|source| {
        DbError::ConnectPostgres {
            connection_string: config.connection_string.clone(),
            source: Box::new(source),
        }
    })
}

#[derive(Clone)]
pub struct SharedPostgresClient {
    config: PostgresConfig,
    client: Arc<Mutex<Option<postgres::Client>>>,
}

impl SharedPostgresClient {
    pub fn new(config: PostgresConfig) -> Self {
        Self {
            config,
            client: Arc::new(Mutex::new(None)),
        }
    }

    pub fn connection_string(&self) -> &str {
        &self.config.connection_string
    }

    pub fn with_client<T, E, F>(&self, f: F) -> std::result::Result<T, E>
    where
        E: From<DbError>,
        F: FnOnce(&mut postgres::Client) -> std::result::Result<T, E>,
    {
        let mut slot = match self.client.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        if slot.is_none() {
            *slot = Some(connect(&self.config).map_err(E::from)?);
        }

        let client = slot
            .as_mut()
            .expect("shared postgres client should exist after connect");
        f(client)
    }
}
