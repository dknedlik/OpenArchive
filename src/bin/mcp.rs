#![deny(warnings)]

use anyhow::Context;
use log::info;
use open_archive::bootstrap::build_service_bundle;
use open_archive::config::AppConfig;

fn main() -> Result<(), anyhow::Error> {
    env_logger::init();
    info!("[open-archive-mcp] process starting");

    // Load .env file relative to the binary location (not cwd), so that
    // spawned processes (e.g. from Claude Desktop) find the project .env
    // regardless of their working directory.
    if let Ok(exe) = std::env::current_exe() {
        if let Some(project_dir) = exe
            .parent()
            .and_then(|p| p.parent())
            .and_then(|p| p.parent())
        {
            let env_path = project_dir.join(".env");
            if env_path.exists() {
                info!(
                    "[open-archive-mcp] loading .env from {}",
                    env_path.display()
                );
                let _ = dotenvy::from_path(&env_path);
            }
        }
    }
    // Fallback: try cwd-relative .env as well.
    let _ = dotenvy::dotenv();

    let config = AppConfig::from_env().context("failed to load application configuration")?;
    info!("[open-archive-mcp] configuration loaded");

    let services = build_service_bundle(&config)
        .context("failed to construct configured service providers")?;
    info!("[open-archive-mcp] service bundle constructed");

    if services.app.search.is_none()
        || services.app.artifact_detail.is_none()
        || services.app.context_pack.is_none()
    {
        anyhow::bail!(
            "configured provider does not support the MVP retrieval MCP tools; use the Postgres retrieval path"
        );
    }

    info!("[open-archive-mcp] stdio server ready");
    open_archive::mcp::run_stdio_server(services.app).context("MCP stdio server failed")
}
