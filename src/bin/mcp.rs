use anyhow::Context;
use open_archive::bootstrap::build_service_bundle;
use open_archive::config::AppConfig;

fn main() -> Result<(), anyhow::Error> {
    eprintln!("[open-archive-mcp] process starting");

    let config = AppConfig::from_env().context("failed to load application configuration")?;
    eprintln!("[open-archive-mcp] configuration loaded");

    let services = build_service_bundle(&config)
        .context("failed to construct configured service providers")?;
    eprintln!("[open-archive-mcp] service bundle constructed");

    if services.app.search.is_none()
        || services.app.artifact_detail.is_none()
        || services.app.context_pack.is_none()
    {
        anyhow::bail!(
            "configured provider does not support the MVP retrieval MCP tools; use the Postgres retrieval path"
        );
    }

    eprintln!("[open-archive-mcp] stdio server ready");
    open_archive::mcp::run_stdio_server(services.app).context("MCP stdio server failed")
}
