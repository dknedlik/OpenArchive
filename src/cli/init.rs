use anyhow::Context;
use clap::Args;
use open_archive::config::{
    expand_home_path, AnthropicConfig, AppConfig, EmbeddingConfig, GeminiConfig,
    GeminiEmbeddingConfig, GrokConfig, InferenceConfig, InferenceExecutionMode,
    LocalFsObjectStoreConfig, ManagedQdrantConfig, ObjectStoreConfig,
    OpenAiCompatibleEmbeddingConfig, OpenAiConfig, OpenAiEmbeddingConfig, OpenAiReasoningEffort,
    QdrantConfig, RelationalStoreConfig, SqliteConfig, VectorStoreConfig,
};
use open_archive::migrations;
use open_archive::SecretStore;
use rpassword::read_password;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[derive(Args)]
pub struct InitArgs {
    /// Run without interactive prompts (requires existing config/env)
    #[arg(long)]
    pub non_interactive: bool,
}

pub(crate) fn init_command(args: InitArgs) -> anyhow::Result<()> {
    println!("Welcome to OpenArchive. Let's get you set up.");

    let secret_store = SecretStore::new();
    // Try to load existing config, but it's OK if it doesn't exist
    let existing_config = AppConfig::load().ok();

    if args.non_interactive {
        return init_non_interactive(&secret_store, existing_config);
    }

    // Load existing values as defaults
    let existing_data_dir = existing_config
        .as_ref()
        .and_then(|c| match &c.object_store {
            ObjectStoreConfig::LocalFs(l) => Some(l.root.parent()?.to_path_buf()),
            _ => None,
        });

    // Step 1: Data directory
    let data_dir = prompt_data_dir(existing_data_dir)?;

    // Step 2: Enrichment provider
    let (inference_config, inference_provider) =
        prompt_inference_provider(&secret_store, existing_config.as_ref())?;

    // Step 3: Embedding provider
    let (embedding_config, embedding_provider) =
        prompt_embedding_provider(&secret_store, &inference_provider, existing_config.as_ref())?;

    // Build the full configuration
    let config = build_init_config(&data_dir, inference_config, embedding_config)?;

    println!("\nSetting up...");
    let mut failures = 0usize;

    // Step 8: Create directory structure
    let dir_result = create_init_directories(&config);
    print_init_result("data directory", &dir_result, &mut failures);

    // Step 9: Write config file
    let config_path = data_dir.join("config.toml");
    let config_result = config
        .write_to_file(&config_path)
        .map(|_| config_path.display().to_string());
    print_init_result(
        "config",
        &config_result.map_err(anyhow::Error::new),
        &mut failures,
    );

    // Step 10: Store API keys
    let inference_key_result =
        store_inference_key(&secret_store, &inference_provider, &config.inference);
    print_init_result("api key", &inference_key_result, &mut failures);

    let embedding_key_result =
        store_embedding_key(&secret_store, &embedding_provider, &config.embeddings);
    print_init_result("embedding key", &embedding_key_result, &mut failures);

    // Step 11: Verify object store
    let object_result = match &config.object_store {
        ObjectStoreConfig::LocalFs(l) => {
            if l.root.exists() {
                Ok(l.root.display().to_string())
            } else {
                Err(anyhow::anyhow!("object store root does not exist"))
            }
        }
        ObjectStoreConfig::S3Compatible(s3) => Ok(format!("s3 {} {}", s3.endpoint, s3.bucket)),
    };
    print_init_result("object store", &object_result, &mut failures);

    // Step 12: Verify Qdrant binary
    let qdrant_result = verify_qdrant_binary(&config);
    print_init_result("qdrant", &qdrant_result, &mut failures);

    // Step 13: Run migrations
    let migration_result = migrations::migrate(&config).map(|_| "applied".to_string());
    print_init_result(
        "migrations",
        &migration_result.map_err(anyhow::Error::new),
        &mut failures,
    );

    if failures == 0 {
        println!("\nReady. Try: open_archive import auto <path>");
        Ok(())
    } else {
        Err(anyhow::anyhow!("init completed with {failures} failure(s)"))
    }
}

fn prompt_data_dir(default: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    let default_str = default
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| {
            dirs::home_dir()
                .map(|h| h.join(".open_archive").display().to_string())
                .unwrap_or_else(|| "~/.open_archive".to_string())
        });

    print!("Data directory [{}]: ", default_str);
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let input = input.trim();

    let path = if input.is_empty() {
        PathBuf::from(&default_str)
    } else {
        expand_home_path(input).map_err(|e| anyhow::anyhow!("invalid path: {}", e))?
    };

    // Create if missing
    if !path.exists() {
        std::fs::create_dir_all(&path)
            .with_context(|| format!("failed to create data directory {}", path.display()))?;
    }

    Ok(path)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InferenceProvider {
    Gemini,
    OpenAi,
    Anthropic,
    Grok,
    Disabled,
}

fn prompt_inference_provider(
    secret_store: &SecretStore,
    existing: Option<&AppConfig>,
) -> anyhow::Result<(InferenceConfig, InferenceProvider)> {
    // Determine default from existing config
    let default = existing
        .map(|c| match &c.inference {
            InferenceConfig::Gemini(_) => InferenceProvider::Gemini,
            InferenceConfig::OpenAi(_) => InferenceProvider::OpenAi,
            InferenceConfig::Anthropic(_) => InferenceProvider::Anthropic,
            InferenceConfig::Grok(_) => InferenceProvider::Grok,
            InferenceConfig::Stub | InferenceConfig::Oci(_) => InferenceProvider::Disabled,
        })
        .unwrap_or(InferenceProvider::Gemini);

    let default_str = match default {
        InferenceProvider::Gemini => "gemini",
        InferenceProvider::OpenAi => "openai",
        InferenceProvider::Anthropic => "anthropic",
        InferenceProvider::Grok => "grok",
        InferenceProvider::Disabled => "disabled",
    };

    print!(
        "Enrichment provider (gemini/openai/anthropic/grok/disabled) [{}]: ",
        default_str
    );
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let input = input.trim();

    let provider = if input.is_empty() {
        default
    } else {
        match input {
            "gemini" => InferenceProvider::Gemini,
            "openai" => InferenceProvider::OpenAi,
            "anthropic" => InferenceProvider::Anthropic,
            "grok" => InferenceProvider::Grok,
            "disabled" => InferenceProvider::Disabled,
            _ => return Err(anyhow::anyhow!("invalid provider: {}", input)),
        }
    };

    // Prompt for API key
    let inference_key = if provider != InferenceProvider::Disabled {
        let key_name = match provider {
            InferenceProvider::Gemini => "OA_GEMINI_API_KEY",
            InferenceProvider::OpenAi => "OA_OPENAI_API_KEY",
            InferenceProvider::Anthropic => "OA_ANTHROPIC_API_KEY",
            InferenceProvider::Grok => "OA_GROK_API_KEY",
            InferenceProvider::Disabled => unreachable!(),
        };

        // Check if already in keyring
        let existing_key = secret_store.get(key_name);
        let prompt_text = if existing_key.is_some() {
            format!("{} API key [already set — press enter to keep]: ", key_name)
        } else {
            format!(
                "{} API key: ",
                match provider {
                    InferenceProvider::Gemini => "Gemini",
                    InferenceProvider::OpenAi => "OpenAI",
                    InferenceProvider::Anthropic => "Anthropic",
                    InferenceProvider::Grok => "Grok",
                    InferenceProvider::Disabled => unreachable!(),
                }
            )
        };

        print!("{}", prompt_text);
        io::stdout().flush()?;

        let key_input = read_password()?;
        if key_input.is_empty() {
            match existing_key {
                Some(key) => key,
                None => return Err(anyhow::anyhow!("API key is required")),
            }
        } else {
            key_input
        }
    } else {
        String::new()
    };

    // Build inference config with default models
    let config = match provider {
        InferenceProvider::Gemini => InferenceConfig::Gemini(GeminiConfig {
            api_key: inference_key.clone(),
            base_url: "https://generativelanguage.googleapis.com/v1beta".to_string(),
            max_output_tokens: 4000,
            repair_max_output_tokens: 8000,
            heavy_model: "gemini-2.0-flash".to_string(),
            fast_model: "gemini-2.0-flash-lite".to_string(),
            batch_enabled: false,
            batch_max_jobs: 16,
            batch_max_bytes: 1_500_000,
            batch_poll_interval: std::time::Duration::from_secs(5),
        }),
        InferenceProvider::OpenAi => InferenceConfig::OpenAi(OpenAiConfig {
            api_key: inference_key.clone(),
            base_url: "https://api.openai.com/v1".to_string(),
            max_output_tokens: 4000,
            repair_max_output_tokens: 8000,
            reasoning_effort_override: OpenAiReasoningEffort::Auto,
            heavy_model: "gpt-4.1".to_string(),
            fast_model: "gpt-4.1-mini".to_string(),
        }),
        InferenceProvider::Anthropic => InferenceConfig::Anthropic(AnthropicConfig {
            api_key: inference_key.clone(),
            base_url: "https://api.anthropic.com/v1".to_string(),
            max_output_tokens: 4000,
            heavy_model: "claude-sonnet-4-5".to_string(),
            fast_model: "claude-haiku-4-5".to_string(),
        }),
        InferenceProvider::Grok => InferenceConfig::Grok(GrokConfig {
            api_key: inference_key.clone(),
            base_url: "https://api.x.ai/v1".to_string(),
            max_output_tokens: 4000,
            repair_max_output_tokens: 8000,
            heavy_model: "grok-4-1-fast-reasoning".to_string(),
            fast_model: "grok-4-1-fast-non-reasoning".to_string(),
        }),
        InferenceProvider::Disabled => InferenceConfig::Stub,
    };

    Ok((config, provider))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EmbeddingProvider {
    Gemini,
    OpenAi,
    OpenAiCompatible,
    Disabled,
}

fn prompt_embedding_provider(
    secret_store: &SecretStore,
    inference_provider: &InferenceProvider,
    existing: Option<&AppConfig>,
) -> anyhow::Result<(EmbeddingConfig, EmbeddingProvider)> {
    // Determine default based on inference provider
    let default = match inference_provider {
        InferenceProvider::Gemini | InferenceProvider::Anthropic | InferenceProvider::Grok => {
            EmbeddingProvider::Gemini
        }
        InferenceProvider::OpenAi => EmbeddingProvider::OpenAi,
        InferenceProvider::Disabled => EmbeddingProvider::Disabled,
    };

    // Check existing config for override
    let existing_embedding = existing.map(|c| match &c.embeddings {
        EmbeddingConfig::Gemini(_) => EmbeddingProvider::Gemini,
        EmbeddingConfig::OpenAi(_) => EmbeddingProvider::OpenAi,
        EmbeddingConfig::OpenAiCompatible(_) => EmbeddingProvider::OpenAiCompatible,
        EmbeddingConfig::Stub(_) | EmbeddingConfig::Disabled => EmbeddingProvider::Disabled,
    });

    let effective_default = existing_embedding.unwrap_or(default);

    let default_str = match effective_default {
        EmbeddingProvider::Gemini => "gemini",
        EmbeddingProvider::OpenAi => "openai",
        EmbeddingProvider::OpenAiCompatible => "openai-compatible",
        EmbeddingProvider::Disabled => "disabled",
    };

    print!(
        "Embedding provider (gemini/openai/openai-compatible/disabled) [{}]: ",
        default_str
    );
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let input = input.trim();

    let provider = if input.is_empty() {
        effective_default
    } else {
        match input {
            "gemini" => EmbeddingProvider::Gemini,
            "openai" => EmbeddingProvider::OpenAi,
            "openai-compatible" => EmbeddingProvider::OpenAiCompatible,
            "disabled" => EmbeddingProvider::Disabled,
            _ => return Err(anyhow::anyhow!("invalid embedding provider: {}", input)),
        }
    };

    let config = match provider {
        EmbeddingProvider::Gemini => {
            // If inference is also gemini, use same key
            let api_key = if *inference_provider == InferenceProvider::Gemini {
                None // Will be stored under OA_GEMINI_API_KEY already
            } else {
                // Need separate key
                let key_name = "OA_GEMINI_API_KEY";
                let existing_key = secret_store.get(key_name);
                let prompt_text = if existing_key.is_some() {
                    "Gemini API key for embeddings [already set — press enter to keep]: "
                        .to_string()
                } else {
                    "Gemini API key for embeddings: ".to_string()
                };

                print!("{}", prompt_text);
                io::stdout().flush()?;

                let key_input = read_password()?;
                if key_input.is_empty() && existing_key.is_some() {
                    existing_key
                } else if key_input.is_empty() {
                    return Err(anyhow::anyhow!("API key is required for gemini embeddings"));
                } else {
                    Some(key_input)
                }
            };

            EmbeddingConfig::Gemini(GeminiEmbeddingConfig {
                api_key: api_key.unwrap_or_default(),
                base_url: "https://generativelanguage.googleapis.com/v1beta".to_string(),
                embedding_model: "gemini-embedding-001".to_string(),
                embedding_dimensions: 3072,
            })
        }
        EmbeddingProvider::OpenAi => {
            // If inference is also openai, use same key
            let api_key = if *inference_provider == InferenceProvider::OpenAi {
                None // Will be stored under OA_OPENAI_API_KEY already
            } else {
                // Need separate key
                let key_name = "OA_OPENAI_API_KEY";
                let existing_key = secret_store.get(key_name);
                let prompt_text = if existing_key.is_some() {
                    "OpenAI API key for embeddings [already set — press enter to keep]: "
                        .to_string()
                } else {
                    "OpenAI API key for embeddings: ".to_string()
                };

                print!("{}", prompt_text);
                io::stdout().flush()?;

                let key_input = read_password()?;
                if key_input.is_empty() && existing_key.is_some() {
                    existing_key
                } else if key_input.is_empty() {
                    return Err(anyhow::anyhow!("API key is required for openai embeddings"));
                } else {
                    Some(key_input)
                }
            };

            EmbeddingConfig::OpenAi(OpenAiEmbeddingConfig {
                api_key: api_key.unwrap_or_default(),
                base_url: "https://api.openai.com/v1".to_string(),
                embedding_model: "text-embedding-3-large".to_string(),
                embedding_dimensions: 3072,
            })
        }
        EmbeddingProvider::OpenAiCompatible => {
            print!("  Base URL: ");
            io::stdout().flush()?;
            let mut base_url = String::new();
            io::stdin().read_line(&mut base_url)?;
            let base_url = base_url.trim();
            if base_url.is_empty() {
                return Err(anyhow::anyhow!("base URL is required"));
            }

            print!("  API key (leave blank if not required): ");
            io::stdout().flush()?;
            let api_key = read_password()?;
            let api_key = if api_key.is_empty() {
                None
            } else {
                Some(api_key)
            };

            print!("  Embedding model: ");
            io::stdout().flush()?;
            let mut model = String::new();
            io::stdin().read_line(&mut model)?;
            let model = model.trim();
            if model.is_empty() {
                return Err(anyhow::anyhow!("model name is required"));
            }

            print!("  Embedding dimensions: ");
            io::stdout().flush()?;
            let mut dims = String::new();
            io::stdin().read_line(&mut dims)?;
            let dims: usize = dims
                .trim()
                .parse()
                .map_err(|_| anyhow::anyhow!("dimensions must be a positive integer"))?;

            EmbeddingConfig::OpenAiCompatible(OpenAiCompatibleEmbeddingConfig {
                api_key,
                base_url: base_url.to_string(),
                embedding_model: model.to_string(),
                embedding_dimensions: dims,
            })
        }
        EmbeddingProvider::Disabled => EmbeddingConfig::Disabled,
    };

    Ok((config, provider))
}

fn build_init_config(
    data_dir: &Path,
    inference: InferenceConfig,
    embeddings: EmbeddingConfig,
) -> anyhow::Result<AppConfig> {
    let sqlite_path = data_dir.join("open_archive.db");
    let objects_path = data_dir.join("objects");
    let qdrant_root = data_dir.join("qdrant");

    let relational_store = RelationalStoreConfig::Sqlite(SqliteConfig {
        path: sqlite_path,
        busy_timeout: std::time::Duration::from_secs(30),
    });

    let object_store = ObjectStoreConfig::LocalFs(LocalFsObjectStoreConfig { root: objects_path });

    let vector_store = VectorStoreConfig::Qdrant(Box::new(QdrantConfig {
        url: "http://127.0.0.1:6333".to_string(),
        collection_name: "open_archive".to_string(),
        request_timeout: std::time::Duration::from_secs(30),
        exact: true,
        managed: ManagedQdrantConfig {
            enabled: true,
            version: "1.17.1".to_string(),
            install_root: qdrant_root.clone(),
            storage_path: qdrant_root.join("storage"),
            log_path: qdrant_root.join("qdrant.log"),
            startup_timeout: std::time::Duration::from_secs(30),
            binary_path: None,
        },
    }));

    Ok(AppConfig {
        http: open_archive::config::HttpConfig {
            bind_addr: "127.0.0.1:8080".to_string(),
            request_worker_count: 4,
            enrichment_worker_count: 2,
            enrichment_poll_interval_ms: 500,
        },
        relational_store,
        vector_store,
        object_store,
        inference,
        embeddings,
        inference_mode: InferenceExecutionMode::Batch,
    })
}

fn create_init_directories(config: &AppConfig) -> anyhow::Result<String> {
    // Create object store directory
    if let ObjectStoreConfig::LocalFs(l) = &config.object_store {
        if !l.root.exists() {
            std::fs::create_dir_all(&l.root)?;
        }
    }

    // Create qdrant directories
    if let VectorStoreConfig::Qdrant(q) = &config.vector_store {
        if !q.managed.install_root.exists() {
            std::fs::create_dir_all(&q.managed.install_root)?;
        }
        if !q.managed.storage_path.exists() {
            std::fs::create_dir_all(&q.managed.storage_path)?;
        }
    }

    Ok("created".to_string())
}

fn store_inference_key(
    secret_store: &SecretStore,
    provider: &InferenceProvider,
    config: &InferenceConfig,
) -> anyhow::Result<String> {
    let key_name = match provider {
        InferenceProvider::Gemini => "OA_GEMINI_API_KEY",
        InferenceProvider::OpenAi => "OA_OPENAI_API_KEY",
        InferenceProvider::Anthropic => "OA_ANTHROPIC_API_KEY",
        InferenceProvider::Grok => "OA_GROK_API_KEY",
        InferenceProvider::Disabled => return Ok("disabled".to_string()),
    };

    let key_value = match config {
        InferenceConfig::Gemini(g) => &g.api_key,
        InferenceConfig::OpenAi(o) => &o.api_key,
        InferenceConfig::Anthropic(a) => &a.api_key,
        InferenceConfig::Grok(g) => &g.api_key,
        _ => return Ok("disabled".to_string()),
    };

    secret_store.set(key_name, key_value)?;
    Ok(format!("stored in {}", secret_store.backend().as_str()))
}

fn store_embedding_key(
    secret_store: &SecretStore,
    provider: &EmbeddingProvider,
    config: &EmbeddingConfig,
) -> anyhow::Result<String> {
    let (key_name, key_value) = match (provider, config) {
        (EmbeddingProvider::Disabled, _) => return Ok("disabled".to_string()),
        (EmbeddingProvider::Gemini, EmbeddingConfig::Gemini(g)) => {
            if g.api_key.is_empty() {
                // Same as inference, already stored
                return Ok("same as inference".to_string());
            }
            ("OA_GEMINI_API_KEY", &g.api_key)
        }
        (EmbeddingProvider::OpenAi, EmbeddingConfig::OpenAi(o)) => {
            if o.api_key.is_empty() {
                return Ok("same as inference".to_string());
            }
            ("OA_OPENAI_API_KEY", &o.api_key)
        }
        (EmbeddingProvider::OpenAiCompatible, EmbeddingConfig::OpenAiCompatible(o)) => {
            if let Some(ref key) = o.api_key {
                ("OA_OPENAI_COMPATIBLE_API_KEY", key)
            } else {
                return Ok("no key required".to_string());
            }
        }
        _ => return Err(anyhow::anyhow!("embedding config mismatch")),
    };

    secret_store.set(key_name, key_value)?;
    Ok(format!("stored in {}", secret_store.backend().as_str()))
}

fn verify_qdrant_binary(config: &AppConfig) -> anyhow::Result<String> {
    if let VectorStoreConfig::Qdrant(q) = &config.vector_store {
        if q.managed.enabled {
            match open_archive::qdrant_sidecar::qdrant_binary_exists(config) {
                Ok(true) => Ok("binary found".to_string()),
                Ok(false) => Err(anyhow::anyhow!(
                    "binary not found — reinstall OpenArchive if missing"
                )),
                Err(e) => Err(anyhow::anyhow!("failed to check binary: {}", e)),
            }
        } else {
            Ok("unmanaged".to_string())
        }
    } else {
        Ok("disabled".to_string())
    }
}

fn print_init_result(name: &str, result: &anyhow::Result<String>, failures: &mut usize) {
    match result {
        Ok(detail) => println!("[ok] {}: {}", name, detail),
        Err(err) => {
            *failures += 1;
            println!("[fail] {}: {}", name, err);
        }
    }
}

fn init_non_interactive(
    secret_store: &SecretStore,
    existing: Option<AppConfig>,
) -> anyhow::Result<()> {
    // Non-interactive mode: read everything from existing config + keyring/env
    let config = existing.ok_or_else(|| {
        anyhow::anyhow!("inference API key not found — run 'open_archive init' to configure")
    })?;

    // Validate required API keys are present
    let has_inference_key = match &config.inference {
        InferenceConfig::Stub => true,
        InferenceConfig::Gemini(_) => secret_store.get("OA_GEMINI_API_KEY").is_some(),
        InferenceConfig::OpenAi(_) => secret_store.get("OA_OPENAI_API_KEY").is_some(),
        InferenceConfig::Anthropic(_) => secret_store.get("OA_ANTHROPIC_API_KEY").is_some(),
        InferenceConfig::Grok(_) => secret_store.get("OA_GROK_API_KEY").is_some(),
        InferenceConfig::Oci(_) => true, // OCI doesn't use API keys
    };

    if !has_inference_key {
        return Err(anyhow::anyhow!(
            "inference API key not found — run 'open_archive init' to configure"
        ));
    }

    println!("Setting up...");
    let mut failures = 0usize;

    // Steps 8-10 only
    let dir_result = create_init_directories(&config);
    print_init_result("data directory", &dir_result, &mut failures);

    let qdrant_result = verify_qdrant_binary(&config);
    print_init_result("qdrant", &qdrant_result, &mut failures);

    let migration_result = migrations::migrate(&config).map(|_| "applied".to_string());
    print_init_result(
        "migrations",
        &migration_result.map_err(anyhow::Error::new),
        &mut failures,
    );

    if failures == 0 {
        println!("\nReady. Try: open_archive import auto <path>");
        Ok(())
    } else {
        Err(anyhow::anyhow!("init completed with {failures} failure(s)"))
    }
}
