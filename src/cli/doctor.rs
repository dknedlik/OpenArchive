use anyhow::Context;
use open_archive::bootstrap::build_service_bundle;
use open_archive::config::{
    AppConfig, InferenceConfig, ObjectStoreConfig, RelationalStoreConfig, VectorStoreConfig,
};
use open_archive::migrations;
use open_archive::{SecretBackend, SecretStore};

pub(crate) fn doctor_command() -> anyhow::Result<()> {
    let secret_store = SecretStore::new();
    let config = AppConfig::load().context("failed to load application configuration")?;
    let mut failures = 0usize;
    print_doctor_result("config", Ok("loaded".to_string()), &mut failures);

    // config_file check
    let config_path = dirs::home_dir()
        .map(|home| home.join(".open_archive").join("config.toml"))
        .filter(|p| p.exists());
    match config_path {
        Some(path) => println!("[ok] config_file: {}", path.display()),
        None => print_doctor_warn(
            "config_file",
            "not found (using env vars only — run 'open_archive init')",
        ),
    }

    // keyring check
    match secret_store.backend() {
        SecretBackend::Keyring => println!("[ok] keyring: keyring"),
        SecretBackend::PlainFile => {
            print_doctor_warn("keyring", "plain_file (secrets stored unencrypted)")
        }
    }

    // api_key check
    let api_key_result: Result<String, anyhow::Error> = match &config.inference {
        InferenceConfig::Stub => Ok("no enrichment provider configured".to_string()),
        InferenceConfig::Gemini(_) => {
            let key_name = "OA_GEMINI_API_KEY";
            if secret_store.get(key_name).is_some() {
                Ok("gemini key present".to_string())
            } else {
                Err(anyhow::anyhow!(
                    "{key_name} not found — run 'open_archive init'"
                ))
            }
        }
        InferenceConfig::OpenAi(_) => {
            let key_name = "OA_OPENAI_API_KEY";
            if secret_store.get(key_name).is_some() {
                Ok("openai key present".to_string())
            } else {
                Err(anyhow::anyhow!(
                    "{key_name} not found — run 'open_archive init'"
                ))
            }
        }
        InferenceConfig::Anthropic(_) => {
            let key_name = "OA_ANTHROPIC_API_KEY";
            if secret_store.get(key_name).is_some() {
                Ok("anthropic key present".to_string())
            } else {
                Err(anyhow::anyhow!(
                    "{key_name} not found — run 'open_archive init'"
                ))
            }
        }
        InferenceConfig::Grok(_) => {
            let key_name = "OA_GROK_API_KEY";
            if secret_store.get(key_name).is_some() {
                Ok("grok key present".to_string())
            } else {
                Err(anyhow::anyhow!(
                    "{key_name} not found — run 'open_archive init'"
                ))
            }
        }
        InferenceConfig::Oci(_) => Ok("no enrichment provider configured".to_string()),
    };
    print_doctor_result("api_key", api_key_result, &mut failures);

    let db_status = match &config.relational_store {
        RelationalStoreConfig::Sqlite(sqlite) => open_archive::sqlite_db::connect(sqlite)
            .map(|_| format!("sqlite {}", sqlite.path.display()))
            .map_err(anyhow::Error::new),
        RelationalStoreConfig::Postgres(pg) => open_archive::postgres_db::connect(pg)
            .map(|_| "postgres reachable".to_string())
            .map_err(anyhow::Error::new),
        RelationalStoreConfig::Oracle(oracle) => open_archive::db::connect(oracle)
            .map(|_| "oracle reachable".to_string())
            .map_err(anyhow::Error::new),
    };
    print_doctor_result("database", db_status, &mut failures);

    print_doctor_result(
        "migrations",
        migrations::check(&config)
            .map(|_| "up to date".to_string())
            .map_err(anyhow::Error::new),
        &mut failures,
    );

    let object_store_status = match &config.object_store {
        ObjectStoreConfig::LocalFs(local_fs) => {
            if local_fs.root.is_dir() {
                Ok(format!("local_fs {}", local_fs.root.display()))
            } else {
                Err(anyhow::anyhow!(
                    "object store root {} does not exist",
                    local_fs.root.display()
                ))
            }
        }
        ObjectStoreConfig::S3Compatible(s3) => Ok(format!("s3 {} {}", s3.endpoint, s3.bucket)),
    };
    print_doctor_result("object_store", object_store_status, &mut failures);

    let mut runtime_config = config.clone();
    let qdrant_status = match &runtime_config.vector_store {
        VectorStoreConfig::Disabled => Ok("disabled".to_string()),
        VectorStoreConfig::PostgresPgVector => Ok("postgres pgvector".to_string()),
        VectorStoreConfig::Qdrant(_) => {
            open_archive::qdrant_sidecar::ensure_managed_qdrant(&mut runtime_config)
                .map(|_| match &runtime_config.vector_store {
                    VectorStoreConfig::Qdrant(qdrant) => format!("healthy {}", qdrant.url),
                    _ => "healthy".to_string(),
                })
                .map_err(anyhow::Error::new)
        }
    };
    print_doctor_result("vector_store", qdrant_status, &mut failures);

    // qdrant_binary check (only for Qdrant vector store)
    if matches!(&runtime_config.vector_store, VectorStoreConfig::Qdrant(_)) {
        let qdrant_binary_result: Result<String, anyhow::Error> =
            open_archive::qdrant_sidecar::qdrant_binary_exists(&config)
                .map_err(anyhow::Error::new)
                .and_then(|exists| {
                    if exists {
                        Ok("found".to_string())
                    } else {
                        Err(anyhow::anyhow!(
                            "not found — run 'open_archive install-qdrant'"
                        ))
                    }
                });
        print_doctor_result("qdrant_binary", qdrant_binary_result, &mut failures);
    }

    print_doctor_result(
        "provider_wiring",
        build_service_bundle(&runtime_config)
            .map(|_| "constructed".to_string())
            .map_err(anyhow::Error::new),
        &mut failures,
    );

    if failures == 0 {
        println!("doctor=ok");
        Ok(())
    } else {
        Err(anyhow::anyhow!("doctor found {failures} issue(s)"))
    }
}

fn print_doctor_result(name: &str, result: Result<String, anyhow::Error>, failures: &mut usize) {
    match result {
        Ok(detail) => println!("[ok] {name}: {detail}"),
        Err(err) => {
            *failures += 1;
            println!("[fail] {name}: {err}");
        }
    }
}

fn print_doctor_warn(name: &str, detail: &str) {
    println!("[warn] {name}: {detail}");
}
