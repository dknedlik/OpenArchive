use anyhow::Context;
use open_archive::bootstrap::build_service_bundle;
use open_archive::config::{
    AppConfig, ObjectStoreConfig, RelationalStoreConfig, VectorStoreConfig,
};
use open_archive::migrations;
use open_archive::SecretStore;

pub(crate) fn doctor_command() -> anyhow::Result<()> {
    let secret_store = SecretStore::new();
    let config = AppConfig::load().context("failed to load application configuration")?;
    let mut failures = 0usize;
    print_doctor_result("config", Ok("loaded".to_string()), &mut failures);

    print_doctor_result(
        "secrets",
        Ok(secret_store.backend().as_str().to_string()),
        &mut failures,
    );

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
