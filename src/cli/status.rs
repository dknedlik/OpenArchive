use anyhow::Context;
use open_archive::config::{
    AppConfig, ObjectStoreConfig, RelationalStoreConfig, VectorStoreConfig,
};
use open_archive::error::StorageError;

pub(crate) fn status_command() -> anyhow::Result<()> {
    let runtime = crate::cli::load_local_runtime(true)?;
    let snapshot = runtime
        .services
        .operator_store
        .load_archive_status()
        .context("failed to load archive status")?;
    let recent_artifacts = match runtime.services.app.artifacts.list_artifacts_filtered(
        &open_archive::storage::ArtifactListFilters::default(),
        5,
        0,
    ) {
        Ok(items) => items,
        Err(StorageError::UnsupportedOperation { .. }) => runtime
            .services
            .app
            .artifacts
            .list_artifacts()
            .context("failed to load recent artifacts")?
            .into_iter()
            .take(5)
            .collect(),
        Err(err) => return Err(anyhow::Error::new(err).context("failed to load recent artifacts")),
    };

    println!(
        "relational_store={}",
        relational_store_label(&runtime.config)
    );
    println!("vector_store={}", vector_store_label(&runtime.config));
    println!("object_store={}", object_store_label(&runtime.config));
    println!("artifacts={}", snapshot.artifact_count);
    println!(
        "artifact_sources={}",
        format_source_counts(&snapshot.artifacts_by_source)
    );
    println!(
        "artifact_enrichment={}",
        format_enrichment_counts(&snapshot.artifacts_by_enrichment_status)
    );
    {
        use open_archive::storage::JobStatus;
        let active_jobs: usize = snapshot
            .jobs_by_status
            .iter()
            .filter(|c| {
                matches!(
                    c.job_status,
                    JobStatus::Pending | JobStatus::Running | JobStatus::Retryable
                )
            })
            .map(|c| c.count)
            .sum();
        if active_jobs > 0 {
            println!(
                "jobs={}  (run 'open_archive enrich' to process)",
                format_job_counts(&snapshot.jobs_by_status)
            );
        } else {
            println!("jobs={}", format_job_counts(&snapshot.jobs_by_status));
        }
    }
    if recent_artifacts.is_empty() {
        println!("recent_artifacts=none");
    } else {
        println!("recent_artifacts={}", recent_artifacts.len());
        for artifact in recent_artifacts {
            println!(
                "  {} {} {} {}",
                artifact.artifact_id,
                artifact.source_type,
                artifact.enrichment_status.as_str(),
                artifact
                    .title
                    .or(artifact.note_path)
                    .unwrap_or_else(|| "(untitled)".to_string())
            );
        }
    }
    Ok(())
}

fn relational_store_label(config: &AppConfig) -> &'static str {
    match config.relational_store {
        RelationalStoreConfig::Sqlite(_) => "sqlite",
        RelationalStoreConfig::Postgres(_) => "postgres",
        RelationalStoreConfig::Oracle(_) => "oracle",
    }
}

fn vector_store_label(config: &AppConfig) -> String {
    match &config.vector_store {
        VectorStoreConfig::Disabled => "disabled".to_string(),
        VectorStoreConfig::PostgresPgVector => "postgres_pgvector".to_string(),
        VectorStoreConfig::Qdrant(qdrant) => format!("qdrant {}", qdrant.url),
    }
}

fn object_store_label(config: &AppConfig) -> String {
    match &config.object_store {
        ObjectStoreConfig::LocalFs(local_fs) => format!("local_fs {}", local_fs.root.display()),
        ObjectStoreConfig::S3Compatible(s3) => format!("s3 {} {}", s3.endpoint, s3.bucket),
    }
}

fn format_source_counts(counts: &[open_archive::storage::ArtifactSourceCount]) -> String {
    if counts.is_empty() {
        return "none".to_string();
    }
    counts
        .iter()
        .map(|count| format!("{}={}", count.source_type.as_str(), count.count))
        .collect::<Vec<_>>()
        .join(",")
}

fn format_enrichment_counts(counts: &[open_archive::storage::ArtifactEnrichmentCount]) -> String {
    if counts.is_empty() {
        return "none".to_string();
    }
    counts
        .iter()
        .map(|count| format!("{}={}", count.enrichment_status.as_str(), count.count))
        .collect::<Vec<_>>()
        .join(",")
}

fn format_job_counts(counts: &[open_archive::storage::EnrichmentJobCount]) -> String {
    if counts.is_empty() {
        return "none".to_string();
    }
    counts
        .iter()
        .map(|count| format!("{}={}", count.job_status.as_str(), count.count))
        .collect::<Vec<_>>()
        .join(",")
}
