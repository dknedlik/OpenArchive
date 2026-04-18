#![cfg(test)]

use super::*;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn clear_test_env() {
    for key in [
        "OA_RELATIONAL_STORE",
        "OA_OBJECT_STORE",
        "OA_S3_ENDPOINT",
        "OA_S3_REGION",
        "OA_S3_BUCKET",
        "OA_S3_ACCESS_KEY_ID",
        "OA_S3_SECRET_ACCESS_KEY",
        "OA_S3_KEY_PREFIX",
        "OA_S3_URL_STYLE",
        "OA_MODEL_PROVIDER",
        "OA_HEAVY_MODEL",
        "OA_FAST_MODEL",
        "OA_EMBEDDING_PROVIDER",
        "OA_EMBEDDING_MODEL",
        "OA_EMBEDDING_DIMENSIONS",
        "OA_INFERENCE_MODE",
        "OA_OPENAI_API_KEY",
        "OA_OPENAI_BASE_URL",
        "OA_OPENAI_MAX_OUTPUT_TOKENS",
        "OA_OPENAI_REPAIR_MAX_OUTPUT_TOKENS",
        "OA_OPENAI_REASONING_EFFORT",
        "OA_STUB_EMBEDDING_MODEL",
        "OA_STUB_EMBEDDING_DIMENSIONS",
        "OA_GEMINI_API_KEY",
        "OA_GEMINI_BASE_URL",
        "OA_GEMINI_MAX_OUTPUT_TOKENS",
        "OA_GEMINI_REPAIR_MAX_OUTPUT_TOKENS",
        "OA_ANTHROPIC_API_KEY",
        "OA_ANTHROPIC_BASE_URL",
        "OA_ANTHROPIC_MAX_OUTPUT_TOKENS",
        "OA_GROK_API_KEY",
        "OA_GROK_BASE_URL",
        "OA_GROK_MAX_OUTPUT_TOKENS",
        "OA_GROK_REPAIR_MAX_OUTPUT_TOKENS",
        "OA_OCI_REGION",
        "OA_OCI_COMPARTMENT_ID",
        "OA_OCI_CLI_PATH",
        "OA_OCI_PROFILE",
        "OA_OCI_MAX_OUTPUT_TOKENS",
        "OA_OCI_REPAIR_MAX_OUTPUT_TOKENS",
        "OA_ORACLE_CONNECT_STRING",
        "OA_ORACLE_USERNAME",
        "OA_ORACLE_PASSWORD",
        "OA_ORACLE_CALL_TIMEOUT_MS",
        "OA_DB_POOL_MIN",
        "OA_DB_POOL_MAX",
        "OA_DB_POOL_INCREMENT",
        "OA_DB_POOL_STMT_CACHE_SIZE",
        "OA_DB_POOL_GET_TIMEOUT_MS",
        "OA_DB_POOL_PING_INTERVAL_MS",
        "OA_HTTP_BIND",
        "OA_HTTP_REQUEST_WORKERS",
        "OA_ENRICHMENT_WORKERS",
        "OA_DIRECT_EXTRACT_WORKERS",
        "OA_DIRECT_RECONCILE_WORKERS",
        "OA_DIRECT_EMBEDDING_WORKERS",
        "OA_BATCH_EXTRACT_WORKERS",
        "OA_BATCH_RECONCILE_WORKERS",
        "OA_BATCH_EMBEDDING_WORKERS",
        "OA_BATCH_EXTRACT_BATCH_SIZE",
        "OA_BATCH_EXTRACT_MAX_CONCURRENT",
        "OA_BATCH_RECONCILE_BATCH_SIZE",
        "OA_BATCH_RECONCILE_MAX_CONCURRENT",
        "OA_ENRICHMENT_POLL_INTERVAL_MS",
        "OA_EXTRACT_CHUNK_SEGMENTS",
        "OA_EXTRACT_CHUNK_OVERLAP",
        "OA_EXTRACT_CHUNK_MAX_CHARS",
        "OA_GEMINI_BATCH_ENABLED",
        "OA_GEMINI_BATCH_MAX_JOBS",
        "OA_GEMINI_BATCH_MAX_BYTES",
        "OA_GEMINI_BATCH_POLL_INTERVAL_MS",
        "OA_POSTGRES_URL",
        "OA_SQLITE_PATH",
        "OA_SQLITE_BUSY_TIMEOUT_MS",
        "OA_VECTOR_STORE",
        "OA_QDRANT_URL",
        "OA_QDRANT_COLLECTION",
        "OA_QDRANT_TIMEOUT_MS",
        "OA_QDRANT_EXACT",
        "OA_QDRANT_MANAGED",
        "OA_QDRANT_VERSION",
        "OA_QDRANT_INSTALL_ROOT",
        "OA_QDRANT_STORAGE_PATH",
        "OA_QDRANT_LOG_PATH",
        "OA_QDRANT_STARTUP_TIMEOUT_MS",
        "OA_QDRANT_BINARY_PATH",
        "OA_OPENAI_REPAIR_MAX_OUTPUT_TOKENS",
        "OA_OBJECT_STORE_ROOT",
        "DATABASE_URL",
        "OA_INFERENCE_PROVIDER",
        "OA_RECONCILE_INFERENCE_PROVIDER",
        "OA_OPENAI_STANDARD_MODEL",
        "OA_OPENAI_QUALITY_MODEL",
        "OA_OPENAI_RECONCILE_STANDARD_MODEL",
        "OA_OPENAI_RECONCILE_QUALITY_MODEL",
        "OA_OPENAI_EMBEDDING_MODEL",
        "OA_OPENAI_EMBEDDING_DIMENSIONS",
        "OA_GEMINI_STANDARD_MODEL",
        "OA_GEMINI_QUALITY_MODEL",
        "OA_GEMINI_FORMAT_STANDARD_MODEL",
        "OA_GEMINI_FORMAT_QUALITY_MODEL",
        "OA_GEMINI_RECONCILE_STANDARD_MODEL",
        "OA_GEMINI_RECONCILE_QUALITY_MODEL",
        "OA_GEMINI_EMBEDDING_MODEL",
        "OA_GEMINI_EMBEDDING_DIMENSIONS",
        "OA_ANTHROPIC_STANDARD_MODEL",
        "OA_ANTHROPIC_QUALITY_MODEL",
        "OA_ANTHROPIC_RECONCILE_STANDARD_MODEL",
        "OA_ANTHROPIC_RECONCILE_QUALITY_MODEL",
        "OA_GROK_STANDARD_MODEL",
        "OA_GROK_QUALITY_MODEL",
        "OA_GROK_RECONCILE_STANDARD_MODEL",
        "OA_GROK_RECONCILE_QUALITY_MODEL",
        "OA_OCI_STANDARD_MODEL",
        "OA_OCI_QUALITY_MODEL",
        "OA_OCI_RECONCILE_STANDARD_MODEL",
        "OA_OCI_RECONCILE_QUALITY_MODEL",
        "OA_EXTRACT_WORKERS",
        "OA_EXTRACT_BATCH_SIZE",
        "OA_EXTRACT_MAX_CONCURRENT",
        "OA_RECONCILE_WORKERS",
        "OA_RECONCILE_BATCH_SIZE",
        "OA_RECONCILE_MAX_CONCURRENT",
        "OA_EMBEDDING_WORKERS",
    ] {
        std::env::remove_var(key);
    }
}

#[test]
fn app_config_defaults_to_sqlite_qdrant_and_stubbed_future_providers() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var("HOME", "/tmp/open-archive-config-defaults");

    let config = AppConfig::from_env().expect("app config should load");

    assert!(matches!(
        config.relational_store,
        RelationalStoreConfig::Sqlite(_)
    ));
    let RelationalStoreConfig::Sqlite(sqlite) = config.relational_store else {
        panic!("expected sqlite config");
    };
    assert_eq!(
        sqlite.path,
        PathBuf::from("/tmp/open-archive-config-defaults/.open_archive/open_archive.db")
    );
    let ObjectStoreConfig::LocalFs(local_fs) = config.object_store else {
        panic!("expected local fs config");
    };
    assert_eq!(
        local_fs.root,
        PathBuf::from("/tmp/open-archive-config-defaults/.open_archive/objects")
    );
    let VectorStoreConfig::Qdrant(qdrant) = config.vector_store else {
        panic!("expected qdrant config");
    };
    assert_eq!(qdrant.url, "http://127.0.0.1:6333");
    assert!(qdrant.managed.enabled);
    assert_eq!(qdrant.managed.version, "1.17.1");
    assert_eq!(
        qdrant.managed.install_root,
        PathBuf::from("/tmp/open-archive-config-defaults/.open_archive/qdrant")
    );
    assert_eq!(config.inference, InferenceConfig::Stub);
    assert_eq!(config.embeddings, EmbeddingConfig::Disabled);
    assert_eq!(config.inference_mode, InferenceExecutionMode::Batch);
}

#[test]
fn postgres_provider_loads_when_configured() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var("OA_RELATIONAL_STORE", "postgres");
    std::env::set_var(
        "OA_POSTGRES_URL",
        "postgres://test:test@localhost/open_archive",
    );

    let config = AppConfig::from_env().expect("postgres provider should load");
    assert!(matches!(
        config.relational_store,
        RelationalStoreConfig::Postgres(_)
    ));
}

#[test]
fn sqlite_provider_loads_when_configured() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var("OA_RELATIONAL_STORE", "sqlite");
    std::env::set_var("OA_SQLITE_PATH", "/tmp/open_archive.sqlite");

    let config = AppConfig::from_env().expect("sqlite provider should load");
    let RelationalStoreConfig::Sqlite(sqlite) = config.relational_store else {
        panic!("expected sqlite config");
    };

    assert_eq!(sqlite.path, PathBuf::from("/tmp/open_archive.sqlite"));
    assert_eq!(sqlite.busy_timeout, Duration::from_secs(30));
    assert!(matches!(config.vector_store, VectorStoreConfig::Qdrant(_)));
}

#[test]
fn sqlite_and_local_fs_paths_expand_home_prefixes() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var("HOME", "/tmp/open-archive-home");
    std::env::set_var("OA_SQLITE_PATH", "${HOME}/db/open_archive.sqlite");
    std::env::set_var("OA_OBJECT_STORE_ROOT", "~/objects");

    let config = AppConfig::from_env().expect("path expansion should work");
    let RelationalStoreConfig::Sqlite(sqlite) = config.relational_store else {
        panic!("expected sqlite config");
    };
    let ObjectStoreConfig::LocalFs(local_fs) = config.object_store else {
        panic!("expected local fs config");
    };

    assert_eq!(
        sqlite.path,
        PathBuf::from("/tmp/open-archive-home/db/open_archive.sqlite")
    );
    assert_eq!(
        local_fs.root,
        PathBuf::from("/tmp/open-archive-home/objects")
    );
}

#[test]
fn oracle_provider_loads_typed_pool_defaults() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var("OA_RELATIONAL_STORE", "oracle");
    std::env::set_var("OA_ORACLE_CONNECT_STRING", "localhost:1521/FREEPDB1");
    std::env::set_var("OA_ORACLE_USERNAME", "oracle_user");
    std::env::set_var("OA_ORACLE_PASSWORD", "oracle_pass");

    let config = AppConfig::from_env().expect("oracle provider should load");
    let RelationalStoreConfig::Oracle(config) = config.relational_store else {
        panic!("expected oracle config");
    };

    assert_eq!(config.connect_string, "localhost:1521/FREEPDB1");
    assert_eq!(config.call_timeout, None);
    assert_eq!(config.pool.min_connections, 1);
    assert_eq!(config.pool.max_connections, 8);
    assert_eq!(config.pool.connection_increment, 1);
    assert_eq!(config.pool.stmt_cache_size, 50);
    assert_eq!(config.pool.get_timeout, Duration::from_secs(30));
    assert_eq!(config.pool.ping_interval, Duration::from_secs(60));
}

#[test]
fn invalid_relational_store_provider_is_rejected() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var("OA_RELATIONAL_STORE", "bogus");
    std::env::set_var(
        "OA_POSTGRES_URL",
        "postgres://test:test@localhost/open_archive",
    );

    let error = AppConfig::from_env().expect_err("provider should be rejected");
    assert!(matches!(
        error,
        ConfigError::InvalidEnumEnv {
            key: "OA_RELATIONAL_STORE",
            ..
        }
    ));
}

#[test]
fn qdrant_vector_provider_loads_with_defaults() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var(
        "OA_POSTGRES_URL",
        "postgres://test:test@localhost/open_archive",
    );
    std::env::set_var("OA_VECTOR_STORE", "qdrant");

    let config = AppConfig::from_env().expect("qdrant vector provider should load");
    let VectorStoreConfig::Qdrant(qdrant) = config.vector_store else {
        panic!("expected qdrant vector config");
    };

    assert_eq!(qdrant.url, "http://127.0.0.1:6333");
    assert_eq!(qdrant.collection_name, "oa_derived_object_embeddings");
    assert_eq!(qdrant.request_timeout, Duration::from_secs(30));
    assert!(qdrant.exact);
    assert!(qdrant.managed.enabled);
    assert_eq!(qdrant.managed.version, "1.17.1");
}

#[test]
fn qdrant_managed_paths_expand_home_prefixes() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var("HOME", "/tmp/open-archive-qdrant-home");
    std::env::set_var("OA_RELATIONAL_STORE", "sqlite");
    std::env::set_var("OA_QDRANT_INSTALL_ROOT", "${HOME}/managed-qdrant");
    std::env::set_var("OA_QDRANT_STORAGE_PATH", "~/managed-qdrant/storage");
    std::env::set_var("OA_QDRANT_LOG_PATH", "$HOME/managed-qdrant/qdrant.log");
    std::env::set_var("OA_QDRANT_BINARY_PATH", "${HOME}/bin/qdrant");

    let config = AppConfig::from_env().expect("qdrant config should load");
    let VectorStoreConfig::Qdrant(qdrant) = config.vector_store else {
        panic!("expected qdrant vector config");
    };

    assert_eq!(
        qdrant.managed.install_root,
        PathBuf::from("/tmp/open-archive-qdrant-home/managed-qdrant")
    );
    assert_eq!(
        qdrant.managed.storage_path,
        PathBuf::from("/tmp/open-archive-qdrant-home/managed-qdrant/storage")
    );
    assert_eq!(
        qdrant.managed.log_path,
        PathBuf::from("/tmp/open-archive-qdrant-home/managed-qdrant/qdrant.log")
    );
    assert_eq!(
        qdrant.managed.binary_path,
        Some(PathBuf::from("/tmp/open-archive-qdrant-home/bin/qdrant"))
    );
}

#[test]
fn s3_object_store_provider_loads_when_configured() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var(
        "OA_POSTGRES_URL",
        "postgres://test:test@localhost/open_archive",
    );
    std::env::set_var("OA_OBJECT_STORE", "s3");
    std::env::set_var("OA_S3_ENDPOINT", "http://localhost:9000");
    std::env::set_var("OA_S3_REGION", "us-east-1");
    std::env::set_var("OA_S3_BUCKET", "openarchive");
    std::env::set_var("OA_S3_ACCESS_KEY_ID", "openarchive");
    std::env::set_var("OA_S3_SECRET_ACCESS_KEY", "openarchive-secret");
    std::env::set_var("OA_S3_KEY_PREFIX", "objects");

    let config = AppConfig::from_env().expect("s3 object-store provider should load");
    let ObjectStoreConfig::S3Compatible(config) = config.object_store else {
        panic!("expected s3 object-store config");
    };

    assert_eq!(config.endpoint, "http://localhost:9000");
    assert_eq!(config.region, "us-east-1");
    assert_eq!(config.bucket, "openarchive");
    assert_eq!(config.key_prefix.as_deref(), Some("objects"));
    assert_eq!(config.url_style, S3UrlStyle::Path);
}

#[test]
fn openai_inference_provider_loads_when_configured() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var(
        "OA_POSTGRES_URL",
        "postgres://test:test@localhost/open_archive",
    );
    std::env::set_var("OA_MODEL_PROVIDER", "openai");
    std::env::set_var("OA_OPENAI_API_KEY", "test-key");
    std::env::set_var("OA_OPENAI_REASONING_EFFORT", "none");
    std::env::set_var("OA_HEAVY_MODEL", "gpt-4.1");
    std::env::set_var("OA_FAST_MODEL", "gpt-4.1-mini");

    let config = AppConfig::from_env().expect("openai inference provider should load");
    let InferenceConfig::OpenAi(config) = config.inference else {
        panic!("expected openai config");
    };

    assert_eq!(config.api_key, "test-key");
    assert_eq!(config.base_url, "https://api.openai.com/v1");
    assert_eq!(
        config.reasoning_effort_override,
        OpenAiReasoningEffort::None
    );
    assert_eq!(config.max_output_tokens, 4000);
    assert_eq!(config.repair_max_output_tokens, 8000);
    assert_eq!(config.heavy_model, "gpt-4.1");
    assert_eq!(config.fast_model, "gpt-4.1-mini");
}

#[test]
fn openai_embedding_provider_loads_when_configured() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var(
        "OA_POSTGRES_URL",
        "postgres://test:test@localhost/open_archive",
    );
    std::env::set_var("OA_EMBEDDING_PROVIDER", "openai");
    std::env::set_var("OA_OPENAI_API_KEY", "test-key");
    std::env::set_var("OA_EMBEDDING_MODEL", "text-embedding-3-small");
    std::env::set_var("OA_EMBEDDING_DIMENSIONS", "1536");

    let config = AppConfig::from_env().expect("embedding config should load");
    let EmbeddingConfig::OpenAi(config) = config.embeddings else {
        panic!("expected openai embedding config");
    };

    assert_eq!(config.api_key, "test-key");
    assert_eq!(config.base_url, "https://api.openai.com/v1");
    assert_eq!(config.embedding_model, "text-embedding-3-small");
    assert_eq!(config.embedding_dimensions, 1536);
}

#[test]
fn gemini_embedding_provider_loads_when_configured() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var(
        "OA_POSTGRES_URL",
        "postgres://test:test@localhost/open_archive",
    );
    std::env::set_var("OA_EMBEDDING_PROVIDER", "gemini");
    std::env::set_var("OA_GEMINI_API_KEY", "test-key");
    std::env::set_var("OA_EMBEDDING_MODEL", "gemini-embedding-001");
    std::env::set_var("OA_EMBEDDING_DIMENSIONS", "3072");

    let config = AppConfig::from_env().expect("embedding config should load");
    let EmbeddingConfig::Gemini(config) = config.embeddings else {
        panic!("expected gemini embedding config");
    };

    assert_eq!(config.api_key, "test-key");
    assert_eq!(
        config.base_url,
        "https://generativelanguage.googleapis.com/v1beta"
    );
    assert_eq!(config.embedding_model, "gemini-embedding-001");
    assert_eq!(config.embedding_dimensions, 3072);
}

#[test]
fn enrichment_pipeline_defaults_include_windowed_chunking_profile() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var(
        "OA_POSTGRES_URL",
        "postgres://test:test@localhost/open_archive",
    );

    let config =
        EnrichmentPipelineConfig::from_env().expect("pipeline config should load defaults");

    assert_eq!(config.chunking.max_segments_per_chunk, 20);
    assert_eq!(config.chunking.chunk_overlap_segments, 4);
    assert_eq!(config.chunking.max_chars_per_chunk, 25_000);
    assert_eq!(config.direct.embedding_workers, 1);
    assert_eq!(config.batch.embedding_workers, 1);
}

#[test]
fn enrichment_pipeline_supports_mode_specific_worker_overrides() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var(
        "OA_POSTGRES_URL",
        "postgres://test:test@localhost/open_archive",
    );
    std::env::set_var("OA_DIRECT_EXTRACT_WORKERS", "7");
    std::env::set_var("OA_DIRECT_RECONCILE_WORKERS", "6");
    std::env::set_var("OA_DIRECT_EMBEDDING_WORKERS", "4");
    std::env::set_var("OA_BATCH_EXTRACT_WORKERS", "1");
    std::env::set_var("OA_BATCH_EXTRACT_BATCH_SIZE", "1");
    std::env::set_var("OA_BATCH_EXTRACT_MAX_CONCURRENT", "60");
    std::env::set_var("OA_BATCH_RECONCILE_WORKERS", "1");
    std::env::set_var("OA_BATCH_RECONCILE_BATCH_SIZE", "1");
    std::env::set_var("OA_BATCH_RECONCILE_MAX_CONCURRENT", "60");
    std::env::set_var("OA_BATCH_EMBEDDING_WORKERS", "9");

    let config =
        EnrichmentPipelineConfig::from_env().expect("pipeline config should load overrides");

    assert_eq!(config.direct.extract_workers, 7);
    assert_eq!(config.direct.reconcile_workers, 6);
    assert_eq!(config.direct.embedding_workers, 4);
    assert_eq!(config.batch.extract_workers, 1);
    assert_eq!(config.batch.extract.batch_size, 1);
    assert_eq!(config.batch.extract.max_concurrent_batches, 60);
    assert_eq!(config.batch.reconcile_workers, 1);
    assert_eq!(config.batch.reconcile.batch_size, 1);
    assert_eq!(config.batch.reconcile.max_concurrent_batches, 60);
    assert_eq!(config.batch.embedding_workers, 9);
}

#[test]
fn inference_execution_mode_loads_when_configured() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var(
        "OA_POSTGRES_URL",
        "postgres://test:test@localhost/open_archive",
    );
    std::env::set_var("OA_INFERENCE_MODE", "direct");

    let config = AppConfig::from_env().expect("inference execution mode should load");
    assert_eq!(config.inference_mode, InferenceExecutionMode::Direct);
}

#[test]
fn gemini_inference_provider_loads_when_configured() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var(
        "OA_POSTGRES_URL",
        "postgres://test:test@localhost/open_archive",
    );
    std::env::set_var("OA_MODEL_PROVIDER", "gemini");
    std::env::set_var("OA_GEMINI_API_KEY", "test-key");
    std::env::set_var("OA_HEAVY_MODEL", "gemini-3-flash-preview");
    std::env::set_var("OA_FAST_MODEL", "gemini-2.5-flash-lite");

    let config = AppConfig::from_env().expect("gemini inference provider should load");
    let InferenceConfig::Gemini(config) = config.inference else {
        panic!("expected gemini config");
    };

    assert_eq!(config.api_key, "test-key");
    assert_eq!(
        config.base_url,
        "https://generativelanguage.googleapis.com/v1beta"
    );
    assert_eq!(config.max_output_tokens, 4000);
    assert_eq!(config.repair_max_output_tokens, 8000);
    assert_eq!(config.heavy_model, "gemini-3-flash-preview");
    assert_eq!(config.fast_model, "gemini-2.5-flash-lite");
}

#[test]
fn anthropic_inference_provider_loads_when_configured() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var(
        "OA_POSTGRES_URL",
        "postgres://test:test@localhost/open_archive",
    );
    std::env::set_var("OA_MODEL_PROVIDER", "anthropic");
    std::env::set_var("OA_ANTHROPIC_API_KEY", "test-key");
    std::env::set_var("OA_HEAVY_MODEL", "claude-sonnet-4-20250514");
    std::env::set_var("OA_FAST_MODEL", "claude-haiku-4-5");

    let config = AppConfig::from_env().expect("anthropic inference provider should load");
    let InferenceConfig::Anthropic(config) = config.inference else {
        panic!("expected anthropic config");
    };

    assert_eq!(config.api_key, "test-key");
    assert_eq!(config.base_url, "https://api.anthropic.com/v1");
    assert_eq!(config.max_output_tokens, 4000);
    assert_eq!(config.heavy_model, "claude-sonnet-4-20250514");
    assert_eq!(config.fast_model, "claude-haiku-4-5");
}

#[test]
fn grok_inference_provider_loads_when_configured() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var(
        "OA_POSTGRES_URL",
        "postgres://test:test@localhost/open_archive",
    );
    std::env::set_var("OA_MODEL_PROVIDER", "grok");
    std::env::set_var("OA_GROK_API_KEY", "test-key");
    std::env::set_var("OA_HEAVY_MODEL", "grok-4-fast-reasoning");
    std::env::set_var("OA_FAST_MODEL", "grok-4-fast-non-reasoning");

    let config = AppConfig::from_env().expect("grok inference provider should load");
    let InferenceConfig::Grok(config) = config.inference else {
        panic!("expected grok config");
    };

    assert_eq!(config.api_key, "test-key");
    assert_eq!(config.base_url, "https://api.x.ai/v1");
    assert_eq!(config.max_output_tokens, 4000);
    assert_eq!(config.repair_max_output_tokens, 8000);
    assert_eq!(config.heavy_model, "grok-4-fast-reasoning");
    assert_eq!(config.fast_model, "grok-4-fast-non-reasoning");
}

#[test]
fn oci_inference_provider_loads_when_configured() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var(
        "OA_POSTGRES_URL",
        "postgres://test:test@localhost/open_archive",
    );
    std::env::set_var("OA_MODEL_PROVIDER", "oci");
    std::env::set_var("OA_OCI_REGION", "us-chicago-1");
    std::env::set_var("OA_OCI_COMPARTMENT_ID", "ocid1.compartment.oc1..example");
    std::env::set_var("OA_HEAVY_MODEL", "meta.llama-3.3-70b-instruct");
    std::env::set_var("OA_FAST_MODEL", "cohere.command-a-03-2025");

    let config = AppConfig::from_env().expect("oci inference provider should load");
    let InferenceConfig::Oci(config) = config.inference else {
        panic!("expected oci config");
    };

    assert_eq!(config.region, "us-chicago-1");
    assert_eq!(config.compartment_id, "ocid1.compartment.oc1..example");
    assert_eq!(config.cli_path, "oci");
    assert_eq!(config.profile, None);
    assert_eq!(config.max_output_tokens, 4000);
    assert_eq!(config.repair_max_output_tokens, 8000);
    assert_eq!(config.heavy_model, "meta.llama-3.3-70b-instruct");
    assert_eq!(config.fast_model, "cohere.command-a-03-2025");
}

#[test]
fn oci_output_tokens_preserve_requested_values() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var(
        "OA_POSTGRES_URL",
        "postgres://test:test@localhost/open_archive",
    );
    std::env::set_var("OA_MODEL_PROVIDER", "oci");
    std::env::set_var("OA_OCI_REGION", "us-chicago-1");
    std::env::set_var("OA_OCI_COMPARTMENT_ID", "ocid1.compartment.oc1..example");
    std::env::set_var("OA_HEAVY_MODEL", "meta.llama-3.3-70b-instruct");
    std::env::set_var("OA_FAST_MODEL", "cohere.command-a-03-2025");
    std::env::set_var("OA_OCI_MAX_OUTPUT_TOKENS", "9000");
    std::env::set_var("OA_OCI_REPAIR_MAX_OUTPUT_TOKENS", "12000");

    let config = AppConfig::from_env().expect("oci config should load with requested tokens");
    let InferenceConfig::Oci(config) = config.inference else {
        panic!("expected oci config");
    };

    assert_eq!(config.max_output_tokens, 9000);
    assert_eq!(config.repair_max_output_tokens, 12000);
}

#[test]
fn legacy_worker_envs_are_ignored() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var(
        "OA_POSTGRES_URL",
        "postgres://test:test@localhost/open_archive",
    );
    std::env::set_var("OA_ENRICHMENT_WORKERS", "2");
    std::env::set_var("OA_EXTRACT_WORKERS", "9");
    std::env::set_var("OA_RECONCILE_WORKERS", "8");
    std::env::set_var("OA_EMBEDDING_WORKERS", "6");
    std::env::set_var("OA_EXTRACT_BATCH_SIZE", "5");
    std::env::set_var("OA_EXTRACT_MAX_CONCURRENT", "4");

    let config =
        EnrichmentPipelineConfig::from_env().expect("pipeline config should ignore legacy envs");

    assert_eq!(config.direct.extract_workers, 2);
    assert_eq!(config.direct.reconcile_workers, 2);
    assert_eq!(config.direct.embedding_workers, 1);
    assert_eq!(config.batch.extract_workers, 2);
    assert_eq!(config.batch.extract.batch_size, 5);
    assert_eq!(config.batch.extract.max_concurrent_batches, 3);
    assert_eq!(config.batch.reconcile_workers, 2);
    assert_eq!(config.batch.reconcile.batch_size, 5);
    assert_eq!(config.batch.reconcile.max_concurrent_batches, 2);
    assert_eq!(config.batch.embedding_workers, 1);
}

#[test]
fn legacy_model_envs_do_not_satisfy_new_contract() {
    let _guard = env_lock();
    clear_test_env();
    std::env::set_var(
        "OA_POSTGRES_URL",
        "postgres://test:test@localhost/open_archive",
    );
    std::env::set_var("OA_MODEL_PROVIDER", "gemini");
    std::env::set_var("OA_GEMINI_API_KEY", "gemini-key");
    std::env::set_var("OA_GEMINI_STANDARD_MODEL", "gemini-3-flash-preview");
    std::env::set_var(
        "OA_GEMINI_FORMAT_STANDARD_MODEL",
        "gemini-3.1-flash-lite-preview",
    );

    let error = AppConfig::from_env().expect_err("legacy model envs should not be accepted");

    assert!(matches!(
        error,
        ConfigError::MissingEnv {
            key: "OA_HEAVY_MODEL"
        }
    ));
}
