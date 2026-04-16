use crate::config::{AppConfig, OracleConfig, PostgresConfig, RelationalStoreConfig, SqliteConfig};
use crate::db;
use crate::error::{preview_sql_statement, MigrationsError, MigrationsResult};
use ::oracle::Connection;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

pub fn check(config: &AppConfig) -> MigrationsResult<()> {
    match &config.relational_store {
        RelationalStoreConfig::Postgres(pg_config) => postgres::check(pg_config),
        RelationalStoreConfig::Sqlite(sqlite_config) => sqlite::check(sqlite_config),
        RelationalStoreConfig::Oracle(db_config) => oracle::check(db_config),
    }
}

pub fn migrate(config: &AppConfig) -> MigrationsResult<()> {
    match &config.relational_store {
        RelationalStoreConfig::Postgres(pg_config) => postgres::migrate(pg_config),
        RelationalStoreConfig::Sqlite(sqlite_config) => sqlite::migrate(sqlite_config),
        RelationalStoreConfig::Oracle(db_config) => oracle::migrate(db_config),
    }
}

pub fn reset(config: &AppConfig) -> MigrationsResult<()> {
    match &config.relational_store {
        RelationalStoreConfig::Postgres(pg_config) => postgres::reset(pg_config),
        RelationalStoreConfig::Sqlite(sqlite_config) => sqlite::reset(sqlite_config),
        RelationalStoreConfig::Oracle(db_config) => oracle::reset(db_config),
    }
}

pub mod oracle {
    use super::*;

    const MIGRATIONS_DIR: &str = "sql/oracle/migrations";

    pub fn check(config: &OracleConfig) -> MigrationsResult<()> {
        let conn = db::connect(config)?;
        ensure_schema_migration_table(&conn)?;
        let pending = pending_migrations(&conn)?;

        if pending.is_empty() {
            println!("schema is current");
            return Ok(());
        }

        println!("pending migrations:");
        for migration in pending {
            println!("  {} {}", migration.version, migration.name);
        }

        Err(MigrationsError::DatabaseNotUpToDate)
    }

    pub fn migrate(config: &OracleConfig) -> MigrationsResult<()> {
        let conn = db::connect(config)?;
        ensure_schema_migration_table(&conn)?;
        let pending = pending_migrations(&conn)?;

        if pending.is_empty() {
            println!("no pending migrations");
            return Ok(());
        }

        for migration in pending {
            apply_migration(&conn, &migration)?;
        }

        Ok(())
    }

    pub fn reset(config: &OracleConfig) -> MigrationsResult<()> {
        let conn = db::connect(config)?;
        reset_schema_objects(&conn)
    }

    fn pending_migrations(conn: &Connection) -> MigrationsResult<Vec<Migration>> {
        let applied = load_applied_migrations(conn)?;
        let mut pending = Vec::new();

        for migration in load_migrations()? {
            if let Some(existing_checksum) = applied.get(&migration.version) {
                if existing_checksum != &migration.checksum {
                    return Err(MigrationsError::ChecksumMismatch {
                        version: migration.version,
                        filename: migration.filename,
                    });
                }
                continue;
            }

            pending.push(migration);
        }

        Ok(pending)
    }

    fn apply_migration(conn: &Connection, migration: &Migration) -> MigrationsResult<()> {
        println!("applying {} {}", migration.version, migration.name);
        execute_sql_script(conn, migration)?;

        conn.execute(
            "insert into oa_schema_migration (version, name, filename, checksum, applied_at) values (:1, :2, :3, :4, systimestamp)",
            &[&migration.version, &migration.name, &migration.filename, &migration.checksum],
        )
        .map_err(|source| MigrationsError::RecordMigration {
            filename: migration.filename.clone(),
            source: Box::new(source),
        })?;

        conn.commit()
            .map_err(|source| MigrationsError::CommitMigration {
                filename: migration.filename.clone(),
                source: Box::new(source),
            })?;

        println!("applied {} {}", migration.version, migration.name);
        Ok(())
    }

    fn ensure_schema_migration_table(conn: &Connection) -> MigrationsResult<()> {
        let plsql = r#"
declare
begin
    execute immediate q'[
        create table oa_schema_migration (
            version varchar2(32 char) not null,
            name varchar2(255 char) not null,
            filename varchar2(255 char) not null,
            checksum varchar2(64 char) not null,
            applied_at timestamp with time zone default systimestamp not null,
            constraint pk_oa_schema_migration primary key (version)
        )
    ]';
exception
    when others then
        if sqlcode != -955 then
            raise;
        end if;
end;
"#;

        conn.execute(plsql, &[])
            .map_err(|source| MigrationsError::EnsureSchemaMigrationTable {
                source: Box::new(source),
            })?;
        conn.commit()
            .map_err(|source| MigrationsError::CommitSchemaMigrationBootstrap {
                source: Box::new(source),
            })?;
        Ok(())
    }

    fn reset_schema_objects(conn: &Connection) -> MigrationsResult<()> {
        let plsql = r#"
declare
begin
    for rec in (
        select table_name
        from user_tables
        where table_name like 'OA\_%' escape '\'
        order by table_name desc
    ) loop
        execute immediate 'drop table "' || rec.table_name || '" cascade constraints purge';
    end loop;
end;
"#;

        conn.execute(plsql, &[])
            .map_err(|source| MigrationsError::ResetSchemaObjects {
                source: Box::new(source),
            })?;
        Ok(())
    }

    fn load_applied_migrations(
        conn: &Connection,
    ) -> MigrationsResult<std::collections::BTreeMap<String, String>> {
        let mut map = std::collections::BTreeMap::new();
        let rows = conn
            .query(
                "select version, checksum from oa_schema_migration order by version",
                &[],
            )
            .map_err(|source| MigrationsError::LoadAppliedMigrations {
                source: Box::new(source),
            })?;

        for row_result in rows {
            let row: ::oracle::Row =
                row_result.map_err(|source| MigrationsError::ReadMigrationHistoryRow {
                    source: Box::new(source),
                })?;
            let version: String =
                row.get(0)
                    .map_err(|source| MigrationsError::ReadMigrationVersion {
                        source: Box::new(source),
                    })?;
            let checksum: String =
                row.get(1)
                    .map_err(|source| MigrationsError::ReadMigrationChecksum {
                        source: Box::new(source),
                    })?;
            map.insert(version, checksum);
        }

        Ok(map)
    }

    fn load_migrations() -> MigrationsResult<Vec<Migration>> {
        load_migrations_from_dir(Path::new(MIGRATIONS_DIR))
    }

    fn execute_sql_script(conn: &Connection, migration: &Migration) -> MigrationsResult<()> {
        for statement in split_sql_statements(&migration.sql)? {
            if statement.trim().is_empty() {
                continue;
            }

            conn.execute(&statement, &[]).map_err(|source| {
                MigrationsError::ExecuteMigrationStatement {
                    filename: migration.filename.clone(),
                    statement_preview: preview_sql_statement(&statement),
                    source: Box::new(source),
                }
            })?;
        }

        Ok(())
    }
}

pub mod postgres {
    use super::*;
    use ::postgres::Client;

    pub const MIGRATIONS_DIR: &str = "sql/postgres/migrations";

    pub fn check(config: &PostgresConfig) -> MigrationsResult<()> {
        let mut client = crate::postgres_db::connect(config)?;
        ensure_schema_migration_table(&mut client)?;
        let pending = pending_migrations(&mut client)?;

        if pending.is_empty() {
            println!("schema is current");
            return Ok(());
        }

        println!("pending migrations:");
        for migration in pending {
            println!("  {} {}", migration.version, migration.name);
        }

        Err(MigrationsError::DatabaseNotUpToDate)
    }

    pub fn migrate(config: &PostgresConfig) -> MigrationsResult<()> {
        let mut client = crate::postgres_db::connect(config)?;
        ensure_schema_migration_table(&mut client)?;
        let pending = pending_migrations(&mut client)?;

        if pending.is_empty() {
            println!("no pending migrations");
            return Ok(());
        }

        for migration in pending {
            apply_migration(&mut client, &migration)?;
        }

        Ok(())
    }

    pub fn reset(config: &PostgresConfig) -> MigrationsResult<()> {
        let mut client = crate::postgres_db::connect(config)?;
        reset_schema_objects(&mut client)
    }

    fn ensure_schema_migration_table(client: &mut Client) -> MigrationsResult<()> {
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS oa_schema_migration (
                    version TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )",
            )
            .map_err(|source| {
                MigrationsError::Db(crate::error::DbError::ConnectPostgres {
                    connection_string: "postgres".to_string(),
                    source: Box::new(source),
                })
            })?;
        Ok(())
    }

    fn reset_schema_objects(client: &mut Client) -> MigrationsResult<()> {
        let rows = client
            .query(
                "SELECT tablename FROM pg_tables WHERE schemaname = current_schema() AND tablename LIKE 'oa_%'",
                &[],
            )
            .map_err(|source| MigrationsError::Db(crate::error::DbError::ConnectPostgres {
                connection_string: "postgres".to_string(),
                source: Box::new(source),
            }))?;

        for row in rows {
            let table_name: String = row.get::<usize, String>(0);
            client
                .batch_execute(&format!("DROP TABLE IF EXISTS \"{}\" CASCADE", table_name))
                .map_err(|source| {
                    MigrationsError::Db(crate::error::DbError::ConnectPostgres {
                        connection_string: "postgres".to_string(),
                        source: Box::new(source),
                    })
                })?;
        }
        Ok(())
    }

    fn pending_migrations(client: &mut Client) -> MigrationsResult<Vec<Migration>> {
        let applied = load_applied_migrations(client)?;
        let mut pending = Vec::new();

        for migration in load_migrations_from_dir(Path::new(MIGRATIONS_DIR))? {
            if let Some(existing_checksum) = applied.get(&migration.version) {
                if existing_checksum != &migration.checksum {
                    return Err(MigrationsError::ChecksumMismatch {
                        version: migration.version,
                        filename: migration.filename,
                    });
                }
                continue;
            }

            pending.push(migration);
        }

        Ok(pending)
    }

    fn load_applied_migrations(
        client: &mut Client,
    ) -> MigrationsResult<std::collections::BTreeMap<String, String>> {
        let mut map = std::collections::BTreeMap::new();
        let rows = client
            .query(
                "SELECT version, checksum FROM oa_schema_migration ORDER BY version",
                &[],
            )
            .map_err(|source| {
                MigrationsError::Db(crate::error::DbError::ConnectPostgres {
                    connection_string: "postgres".to_string(),
                    source: Box::new(source),
                })
            })?;

        for row in rows {
            let version: String = row.get::<usize, String>(0);
            let checksum: String = row.get::<usize, String>(1);
            map.insert(version, checksum);
        }

        Ok(map)
    }

    fn apply_migration(client: &mut Client, migration: &Migration) -> MigrationsResult<()> {
        println!("applying {} {}", migration.version, migration.name);
        client.batch_execute(&migration.sql).map_err(|source| {
            MigrationsError::Db(crate::error::DbError::ConnectPostgres {
                connection_string: "postgres".to_string(),
                source: Box::new(source),
            })
        })?;
        client
            .execute(
                "INSERT INTO oa_schema_migration (version, name, filename, checksum, applied_at) VALUES ($1, $2, $3, $4, NOW())",
                &[&migration.version, &migration.name, &migration.filename, &migration.checksum],
            )
            .map_err(|source| MigrationsError::Db(crate::error::DbError::ConnectPostgres {
                connection_string: "postgres".to_string(),
                source: Box::new(source),
            }))?;
        println!("applied {} {}", migration.version, migration.name);
        Ok(())
    }
}

pub mod sqlite {
    use super::*;
    use rusqlite::{params, Connection};

    pub const MIGRATIONS_DIR: &str = "sql/sqlite/migrations";

    pub fn check(config: &SqliteConfig) -> MigrationsResult<()> {
        let connection = crate::sqlite_db::connect(config)?;
        ensure_schema_migration_table(&connection)?;
        let pending = pending_migrations(&connection)?;

        if pending.is_empty() {
            println!("schema is current");
            return Ok(());
        }

        println!("pending migrations:");
        for migration in pending {
            println!("  {} {}", migration.version, migration.name);
        }

        Err(MigrationsError::DatabaseNotUpToDate)
    }

    pub fn migrate(config: &SqliteConfig) -> MigrationsResult<()> {
        let mut connection = crate::sqlite_db::connect(config)?;
        ensure_schema_migration_table(&connection)?;
        let pending = pending_migrations(&connection)?;

        if pending.is_empty() {
            println!("no pending migrations");
            return Ok(());
        }

        for migration in pending {
            apply_migration(&mut connection, &migration)?;
        }

        Ok(())
    }

    pub fn reset(config: &SqliteConfig) -> MigrationsResult<()> {
        let connection = crate::sqlite_db::connect(config)?;
        reset_schema_objects(&connection)
    }

    fn ensure_schema_migration_table(connection: &Connection) -> MigrationsResult<()> {
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS oa_schema_migration (
                    version TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
                )",
            )
            .map_err(|source| MigrationsError::EnsureSqliteSchemaMigrationTable {
                source: Box::new(source),
            })?;
        Ok(())
    }

    fn reset_schema_objects(connection: &Connection) -> MigrationsResult<()> {
        let mut stmt = connection
            .prepare(
                "SELECT name, type
                 FROM sqlite_master
                 WHERE name LIKE 'oa_%'
                   AND name <> 'oa_schema_migration'
                   AND type IN ('table', 'view', 'trigger', 'index')",
            )
            .map_err(|source| MigrationsError::ResetSqliteSchemaObjects {
                source: Box::new(source),
            })?;
        let objects = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|source| MigrationsError::ResetSqliteSchemaObjects {
                source: Box::new(source),
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| MigrationsError::ResetSqliteSchemaObjects {
                source: Box::new(source),
            })?;

        for (name, object_type) in objects {
            let ddl = match object_type.as_str() {
                "table" => format!("DROP TABLE IF EXISTS \"{name}\""),
                "view" => format!("DROP VIEW IF EXISTS \"{name}\""),
                "trigger" => format!("DROP TRIGGER IF EXISTS \"{name}\""),
                "index" => format!("DROP INDEX IF EXISTS \"{name}\""),
                _ => continue,
            };
            connection.execute_batch(&ddl).map_err(|source| {
                MigrationsError::ResetSqliteSchemaObjects {
                    source: Box::new(source),
                }
            })?;
        }
        Ok(())
    }

    fn pending_migrations(connection: &Connection) -> MigrationsResult<Vec<Migration>> {
        let applied = load_applied_migrations(connection)?;
        let mut pending = Vec::new();

        for migration in load_migrations_from_dir(Path::new(MIGRATIONS_DIR))? {
            if let Some(existing_checksum) = applied.get(&migration.version) {
                if existing_checksum != &migration.checksum {
                    return Err(MigrationsError::ChecksumMismatch {
                        version: migration.version,
                        filename: migration.filename,
                    });
                }
                continue;
            }
            pending.push(migration);
        }

        Ok(pending)
    }

    fn load_applied_migrations(
        connection: &Connection,
    ) -> MigrationsResult<std::collections::BTreeMap<String, String>> {
        let mut map = std::collections::BTreeMap::new();
        let mut stmt = connection
            .prepare("SELECT version, checksum FROM oa_schema_migration ORDER BY version")
            .map_err(|source| MigrationsError::LoadSqliteAppliedMigrations {
                source: Box::new(source),
            })?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|source| MigrationsError::LoadSqliteAppliedMigrations {
                source: Box::new(source),
            })?;

        for row in rows {
            let (version, checksum) =
                row.map_err(|source| MigrationsError::LoadSqliteAppliedMigrations {
                    source: Box::new(source),
                })?;
            map.insert(version, checksum);
        }

        Ok(map)
    }

    fn apply_migration(connection: &mut Connection, migration: &Migration) -> MigrationsResult<()> {
        println!("applying {} {}", migration.version, migration.name);
        let tx = connection.transaction().map_err(|source| {
            MigrationsError::LoadSqliteAppliedMigrations {
                source: Box::new(source),
            }
        })?;

        tx.execute_batch(&migration.sql).map_err(|source| {
            MigrationsError::ExecuteSqliteMigrationStatement {
                filename: migration.filename.clone(),
                statement_preview: preview_sql_statement(&migration.sql),
                source: Box::new(source),
            }
        })?;

        tx.execute(
            "INSERT INTO oa_schema_migration (version, name, filename, checksum, applied_at)
             VALUES (?1, ?2, ?3, ?4, strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            params![
                migration.version,
                migration.name,
                migration.filename,
                migration.checksum
            ],
        )
        .map_err(|source| MigrationsError::RecordSqliteMigration {
            filename: migration.filename.clone(),
            source: Box::new(source),
        })?;
        tx.commit()
            .map_err(|source| MigrationsError::RecordSqliteMigration {
                filename: migration.filename.clone(),
                source: Box::new(source),
            })?;

        println!("applied {} {}", migration.version, migration.name);
        Ok(())
    }
}

#[derive(Debug)]
struct Migration {
    version: String,
    name: String,
    filename: String,
    checksum: String,
    sql: String,
}

fn load_migrations_from_dir(dir: &Path) -> MigrationsResult<Vec<Migration>> {
    let mut entries = Vec::new();

    for entry in fs::read_dir(dir).map_err(|source| MigrationsError::ReadMigrationsDir {
        path: dir.to_path_buf(),
        source: Box::new(source),
    })? {
        let entry = entry.map_err(|source| MigrationsError::ReadMigrationEntry {
            source: Box::new(source),
        })?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("sql") {
            continue;
        }
        entries.push(load_migration(&path)?);
    }

    entries.sort_by(|left, right| left.version.cmp(&right.version));
    Ok(entries)
}

fn load_migration(path: &Path) -> MigrationsResult<Migration> {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| MigrationsError::InvalidMigrationFilename {
            path: path.to_path_buf(),
        })?
        .to_string();

    let (version, name) = parse_filename(&filename)?;
    let sql = fs::read_to_string(path).map_err(|source| MigrationsError::ReadMigrationFile {
        path: path.to_path_buf(),
        source: Box::new(source),
    })?;
    let checksum = format!("{:x}", Sha256::digest(sql.as_bytes()));

    Ok(Migration {
        version,
        name,
        filename,
        checksum,
        sql,
    })
}

fn parse_filename(filename: &str) -> MigrationsResult<(String, String)> {
    let base = filename.strip_suffix(".sql").ok_or_else(|| {
        MigrationsError::MigrationFileMissingSqlSuffix {
            filename: filename.to_string(),
        }
    })?;
    let (version_part, name_part) =
        base.split_once("__")
            .ok_or_else(|| MigrationsError::MigrationFileInvalidPattern {
                filename: filename.to_string(),
            })?;

    if !version_part.starts_with('V') || version_part.len() < 2 {
        return Err(MigrationsError::MigrationFileInvalidVersionPrefix {
            filename: filename.to_string(),
        });
    }

    Ok((
        version_part.trim_start_matches('V').to_string(),
        name_part.replace('_', " "),
    ))
}

fn split_sql_statements(sql: &str) -> MigrationsResult<Vec<String>> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;
    let mut in_plsql_block = false;

    for line in sql.lines() {
        let trimmed = line.trim();
        if trimmed == "/" && !in_single_quote {
            if !current.trim().is_empty() {
                statements.push(current.trim().to_string());
                current.clear();
            }
            in_plsql_block = false;
            continue;
        }

        for ch in line.chars() {
            if ch == '\'' {
                in_single_quote = !in_single_quote;
            }
            current.push(ch);
        }
        current.push('\n');

        if !in_single_quote {
            let upper = trimmed.to_ascii_uppercase();
            if upper == "BEGIN"
                || upper == "DECLARE"
                || upper.starts_with("BEGIN ")
                || upper.starts_with("DECLARE ")
            {
                in_plsql_block = true;
            }
            if !in_plsql_block && trimmed.ends_with(';') {
                let statement = current.trim().trim_end_matches(';').trim().to_string();
                if !statement.is_empty() {
                    statements.push(statement);
                }
                current.clear();
            }
        }
    }

    if in_single_quote {
        return Err(MigrationsError::UnterminatedStringLiteral);
    }

    if !current.trim().is_empty() {
        statements.push(current.trim().to_string());
    }

    Ok(statements)
}
