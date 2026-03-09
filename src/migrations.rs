use crate::config::DbConfig;
use crate::db;
use anyhow::{anyhow, bail, Context, Result};
use oracle::Connection;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

const MIGRATIONS_DIR: &str = "sql/migrations";

#[derive(Debug)]
pub struct Migration {
    pub version: String,
    pub name: String,
    pub filename: String,
    pub checksum: String,
    pub sql: String,
}

pub fn check(config: &DbConfig) -> Result<()> {
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

    bail!("database is not up to date");
}

pub fn migrate(config: &DbConfig) -> Result<()> {
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

pub fn reset(config: &DbConfig) -> Result<()> {
    let conn = db::connect(config)?;
    reset_schema_objects(&conn)
}

fn pending_migrations(conn: &Connection) -> Result<Vec<Migration>> {
    let applied = load_applied_migrations(conn)?;
    let mut pending = Vec::new();

    for migration in load_migrations()? {
        if let Some(existing_checksum) = applied.get(&migration.version) {
            if existing_checksum != &migration.checksum {
                bail!(
                    "migration checksum mismatch for version {} (file {})",
                    migration.version,
                    migration.filename
                );
            }
            continue;
        }

        pending.push(migration);
    }

    Ok(pending)
}

fn apply_migration(conn: &Connection, migration: &Migration) -> Result<()> {
    println!("applying {} {}", migration.version, migration.name);
    execute_sql_script(conn, &migration.sql)
        .with_context(|| format!("failed while executing {}", migration.filename))?;

    conn.execute(
        "insert into oa_schema_migration (version, name, filename, checksum, applied_at) values (:1, :2, :3, :4, systimestamp)",
        &[&migration.version, &migration.name, &migration.filename, &migration.checksum],
    )
    .with_context(|| format!("failed to record migration {}", migration.filename))?;

    conn.commit()
        .with_context(|| format!("failed to commit migration {}", migration.filename))?;

    println!("applied {} {}", migration.version, migration.name);
    Ok(())
}

fn ensure_schema_migration_table(conn: &Connection) -> Result<()> {
    let plsql = r#"
declare
    table_missing exception;
    pragma exception_init(table_missing, -942);
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
        .context("failed to ensure oa_schema_migration exists")?;
    conn.commit()
        .context("failed to commit oa_schema_migration bootstrap")?;
    Ok(())
}

fn reset_schema_objects(conn: &Connection) -> Result<()> {
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
        .context("failed to reset open_archive schema objects")?;
    Ok(())
}

fn load_applied_migrations(conn: &Connection) -> Result<std::collections::BTreeMap<String, String>> {
    let mut map = std::collections::BTreeMap::new();
    let rows = conn
        .query(
            "select version, checksum from oa_schema_migration order by version",
            &[],
        )
        .context("failed to load applied migrations")?;

    for row_result in rows {
        let row = row_result.context("failed to read migration history row")?;
        let version: String = row.get(0).context("failed to read migration version")?;
        let checksum: String = row.get(1).context("failed to read migration checksum")?;
        map.insert(version, checksum);
    }

    Ok(map)
}

fn load_migrations() -> Result<Vec<Migration>> {
    let dir = Path::new(MIGRATIONS_DIR);
    let mut entries = Vec::new();

    for entry in fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))? {
        let entry = entry.context("failed to read migration entry")?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("sql") {
            continue;
        }
        entries.push(load_migration(&path)?);
    }

    entries.sort_by(|left, right| left.version.cmp(&right.version));
    Ok(entries)
}

fn load_migration(path: &Path) -> Result<Migration> {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("invalid migration filename: {}", path.display()))?
        .to_string();

    let (version, name) = parse_filename(&filename)?;
    let sql = fs::read_to_string(path)
        .with_context(|| format!("failed to read migration {}", path.display()))?;
    let checksum = format!("{:x}", Sha256::digest(sql.as_bytes()));

    Ok(Migration {
        version,
        name,
        filename,
        checksum,
        sql,
    })
}

fn parse_filename(filename: &str) -> Result<(String, String)> {
    let base = filename
        .strip_suffix(".sql")
        .ok_or_else(|| anyhow!("migration file must end in .sql: {}", filename))?;
    let (version_part, name_part) = base
        .split_once("__")
        .ok_or_else(|| anyhow!("migration file must match VNNN__name.sql: {}", filename))?;

    if !version_part.starts_with('V') || version_part.len() < 2 {
        bail!("migration file must start with VNNN: {}", filename);
    }

    Ok((
        version_part.trim_start_matches('V').to_string(),
        name_part.replace('_', " "),
    ))
}

fn execute_sql_script(conn: &Connection, sql: &str) -> Result<()> {
    for statement in split_sql_statements(sql)? {
        if statement.trim().is_empty() {
            continue;
        }

        conn.execute(&statement, &[])
            .with_context(|| format!("statement failed:\n{}", statement))?;
    }

    Ok(())
}

fn split_sql_statements(sql: &str) -> Result<Vec<String>> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;

    for line in sql.lines() {
        let trimmed = line.trim();
        if trimmed == "/" && !in_single_quote {
            if !current.trim().is_empty() {
                statements.push(current.trim().to_string());
                current.clear();
            }
            continue;
        }

        for ch in line.chars() {
            if ch == '\'' {
                in_single_quote = !in_single_quote;
            }
            current.push(ch);
        }
        current.push('\n');

        if !in_single_quote && trimmed.ends_with(';') {
            let statement = current.trim().trim_end_matches(';').trim().to_string();
            if !statement.is_empty() {
                statements.push(statement);
            }
            current.clear();
        }
    }

    if in_single_quote {
        bail!("unterminated string literal in migration SQL");
    }

    if !current.trim().is_empty() {
        statements.push(current.trim().to_string());
    }

    Ok(statements)
}
