#![deny(warnings)]

use anyhow::Context;
use log::info;
use open_archive::bootstrap::build_service_bundle;
use open_archive::config::AppConfig;
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;
use url::Url;

fn main() -> Result<(), anyhow::Error> {
    env_logger::init();
    info!("[open-archive-mcp] process starting");
    let explicit_postgres_url = std::env::var_os("OA_POSTGRES_URL").is_some();

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
    normalize_stdio_postgres_url(explicit_postgres_url);

    let mut config = AppConfig::load().context("failed to load application configuration")?;
    let _managed_qdrant = open_archive::qdrant_sidecar::ensure_managed_qdrant(&mut config)
        .context("failed to prepare managed Qdrant")?;
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

fn normalize_stdio_postgres_url(explicit_postgres_url: bool) {
    if explicit_postgres_url {
        return;
    }

    let Ok(connection_string) = std::env::var("OA_POSTGRES_URL") else {
        return;
    };

    let Some(normalized) = normalized_stdio_postgres_url(&connection_string) else {
        return;
    };

    info!(
        "[open-archive-mcp] rewriting OA_POSTGRES_URL host from postgres to localhost for host stdio startup"
    );
    std::env::set_var("OA_POSTGRES_URL", normalized);
}

fn normalized_stdio_postgres_url(connection_string: &str) -> Option<String> {
    let port = Url::parse(connection_string).ok()?.port().unwrap_or(5432);
    rewrite_postgres_host_to_localhost(connection_string, localhost_postgres_reachable(port))
}

fn localhost_postgres_reachable(port: u16) -> bool {
    TcpStream::connect_timeout(
        &SocketAddr::from(([127, 0, 0, 1], port)),
        Duration::from_millis(200),
    )
    .is_ok()
}

fn rewrite_postgres_host_to_localhost(
    connection_string: &str,
    localhost_reachable: bool,
) -> Option<String> {
    let mut url = Url::parse(connection_string).ok()?;
    if !matches!(url.scheme(), "postgres" | "postgresql") {
        return None;
    }
    if url.host_str()? != "postgres" || !localhost_reachable {
        return None;
    }
    url.set_host(Some("localhost")).ok()?;
    Some(url.into())
}

#[cfg(test)]
mod tests {
    use super::rewrite_postgres_host_to_localhost;

    #[test]
    fn rewrites_docker_postgres_host_for_stdio() {
        let connection_string =
            "postgres://openarchive:openarchive@postgres:5432/openarchive?sslmode=disable";
        let normalized =
            rewrite_postgres_host_to_localhost(connection_string, true).expect("rewrite");
        assert_eq!(
            normalized,
            "postgres://openarchive:openarchive@localhost:5432/openarchive?sslmode=disable"
        );
    }

    #[test]
    fn leaves_non_docker_host_unchanged() {
        let connection_string = "postgres://openarchive:openarchive@localhost:5432/openarchive";
        assert!(rewrite_postgres_host_to_localhost(connection_string, true).is_none());
    }

    #[test]
    fn leaves_docker_host_unchanged_when_local_postgres_is_down() {
        let connection_string = "postgres://openarchive:openarchive@postgres:5432/openarchive";
        assert!(rewrite_postgres_host_to_localhost(connection_string, false).is_none());
    }
}
