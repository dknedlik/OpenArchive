use rusqlite::Connection;

use crate::config::SqliteConfig;
use crate::error::{DbError, DbResult};

pub fn connect(config: &SqliteConfig) -> DbResult<Connection> {
    let connection = Connection::open(&config.path).map_err(|source| DbError::ConnectSqlite {
        path: config.path.display().to_string(),
        source: Box::new(source),
    })?;

    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(|source| DbError::ConnectSqlite {
            path: config.path.display().to_string(),
            source: Box::new(source),
        })?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(|source| DbError::ConnectSqlite {
            path: config.path.display().to_string(),
            source: Box::new(source),
        })?;
    connection
        .pragma_update(None, "synchronous", "NORMAL")
        .map_err(|source| DbError::ConnectSqlite {
            path: config.path.display().to_string(),
            source: Box::new(source),
        })?;
    connection
        .busy_timeout(config.busy_timeout)
        .map_err(|source| DbError::ConnectSqlite {
            path: config.path.display().to_string(),
            source: Box::new(source),
        })?;

    Ok(connection)
}
