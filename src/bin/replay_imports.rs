use anyhow::{anyhow, Context, Result};
use clap::Parser;
use open_archive::config::ObjectStoreConfig;
use open_archive::object_store::{
    LocalFsObjectStore, ObjectStore, S3CompatibleObjectStore, StoredObject,
};
use open_archive::storage::SourceType;
use reqwest::blocking::Client;
use reqwest::StatusCode;

#[derive(Parser, Debug)]
#[command(name = "replay_imports")]
#[command(
    about = "Replay persisted raw import payloads from Postgres into a target OpenArchive app"
)]
struct Cli {
    #[arg(
        long,
        default_value = "postgres://openarchive:openarchive@127.0.0.1:5432/openarchive"
    )]
    postgres_url: String,

    #[arg(long, default_value = "http://127.0.0.1:3111")]
    target_base_url: String,

    #[arg(long)]
    import_id: Option<String>,

    #[arg(long)]
    limit: Option<usize>,

    #[arg(long, default_value_t = false)]
    stop_on_error: bool,
}

#[derive(Debug)]
struct ReplayImport {
    import_id: String,
    source_type: SourceType,
    source_filename: Option<String>,
    stored_object: StoredObject,
}

fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();

    let object_store = build_object_store()?;
    let imports = load_imports(&cli)?;
    let client = Client::builder()
        .build()
        .context("failed to build HTTP client")?;

    println!("imports_found={}", imports.len());

    let mut succeeded = 0usize;
    let mut failed = 0usize;

    for import in imports {
        let route = import_route(import.source_type);
        let url = format!("{}{}", cli.target_base_url.trim_end_matches('/'), route);
        println!(
            "replaying import_id={} source_type={} filename={}",
            import.import_id,
            import.source_type.as_str(),
            import.source_filename.as_deref().unwrap_or("-")
        );

        let bytes = object_store
            .get_object_bytes(&import.stored_object)
            .with_context(|| format!("failed to read object for import {}", import.import_id))?;

        let response =
            client.post(&url).body(bytes).send().with_context(|| {
                format!("failed to POST import {} to {}", import.import_id, url)
            })?;

        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            failed += 1;
            eprintln!(
                "import_id={} status={} body={}",
                import.import_id, status, body
            );
            if cli.stop_on_error {
                return Err(anyhow!(
                    "stopping after replay failure for {}",
                    import.import_id
                ));
            }
            continue;
        }

        let body = response.text().unwrap_or_default();
        println!("import_id={} replay_ok response={}", import.import_id, body);
        succeeded += 1;
    }

    println!("imports_replayed_ok={succeeded}");
    println!("imports_replayed_failed={failed}");

    Ok(())
}

fn build_object_store() -> Result<Box<dyn ObjectStore + Send + Sync>> {
    match ObjectStoreConfig::from_env().context("failed to load object-store config")? {
        ObjectStoreConfig::LocalFs(config) => Ok(Box::new(LocalFsObjectStore::new(config.root))),
        ObjectStoreConfig::S3Compatible(config) => Ok(Box::new(
            S3CompatibleObjectStore::new(config).context("failed to build S3 object store")?,
        )),
    }
}

fn load_imports(cli: &Cli) -> Result<Vec<ReplayImport>> {
    let mut client = postgres::Client::connect(&cli.postgres_url, postgres::NoTls)
        .with_context(|| format!("failed to connect to source postgres {}", cli.postgres_url))?;

    let mut sql = String::from(
        "SELECT i.import_id,
                i.source_type,
                i.source_filename,
                o.object_id,
                o.storage_provider,
                o.storage_key,
                o.mime_type,
                o.size_bytes,
                o.sha256
         FROM oa_import i
         JOIN oa_object_ref o ON o.object_id = i.payload_object_id",
    );

    let mut params: Vec<&(dyn postgres::types::ToSql + Sync)> = Vec::new();
    let import_id_param;
    if let Some(import_id) = cli.import_id.as_ref() {
        sql.push_str(" WHERE i.import_id = $1");
        import_id_param = import_id.clone();
        params.push(&import_id_param);
    }
    sql.push_str(" ORDER BY i.created_at ASC, i.import_id ASC");

    let rows = client
        .query(&sql, &params)
        .context("failed to query source imports")?;

    let mut imports = Vec::with_capacity(rows.len());
    for row in rows {
        let source_type_raw: String = row.get(1);
        let source_type = SourceType::parse(&source_type_raw)
            .ok_or_else(|| anyhow!("unsupported source_type {}", source_type_raw))?;
        imports.push(ReplayImport {
            import_id: row.get(0),
            source_type,
            source_filename: row.get(2),
            stored_object: StoredObject {
                object_id: row.get(3),
                provider: row.get(4),
                storage_key: row.get(5),
                mime_type: row.get(6),
                size_bytes: row.get(7),
                sha256: row.get(8),
            },
        });
    }

    if let Some(limit) = cli.limit {
        imports.truncate(limit);
    }

    Ok(imports)
}

fn import_route(source_type: SourceType) -> &'static str {
    match source_type {
        SourceType::ChatGptExport => "/imports/chatgpt",
        SourceType::ClaudeExport => "/imports/claude",
        SourceType::GrokExport => "/imports/grok",
        SourceType::GeminiTakeout => "/imports/gemini",
        SourceType::TextFile => "/imports/text",
        SourceType::MarkdownFile => "/imports/markdown",
        SourceType::ObsidianVault => "/imports/obsidian",
    }
}
