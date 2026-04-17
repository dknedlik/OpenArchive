pub mod doctor;
pub mod import;
pub mod init;
pub mod query;
pub mod status;

use anyhow::Context;
use open_archive::bootstrap::{build_service_bundle, ServiceBundle};
use open_archive::config::AppConfig;
use open_archive::migrations;
use open_archive::qdrant_sidecar::ManagedQdrantHandle;

pub(crate) struct LocalRuntime {
    pub config: AppConfig,
    pub services: ServiceBundle,
    pub _managed_qdrant: Option<ManagedQdrantHandle>,
}

pub(crate) fn load_local_runtime(apply_migrations: bool) -> anyhow::Result<LocalRuntime> {
    let mut config = AppConfig::load().context("failed to load application configuration")?;
    let managed_qdrant = open_archive::qdrant_sidecar::ensure_managed_qdrant(&mut config)
        .context("failed to prepare managed Qdrant")?;
    if apply_migrations {
        migrations::migrate(&config).context("failed to apply database migrations")?;
    }
    let services = build_service_bundle(&config)
        .context("failed to construct configured service providers")?;
    Ok(LocalRuntime {
        config,
        services,
        _managed_qdrant: managed_qdrant,
    })
}
