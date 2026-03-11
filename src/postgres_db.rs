use crate::config::PostgresConfig;
use crate::error::{DbError, DbResult};

pub fn connect(config: &PostgresConfig) -> DbResult<postgres::Client> {
    postgres::Client::connect(&config.connection_string, postgres::NoTls).map_err(|source| {
        DbError::ConnectPostgres {
            connection_string: config.connection_string.clone(),
            source,
        }
    })
}
