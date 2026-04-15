use crate::config::{AppConfig, ManagedQdrantConfig, QdrantConfig, VectorStoreConfig};
use crate::error::{ConfigError, ConfigResult};
use flate2::read::GzDecoder;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{self, Cursor, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use tar::Archive;
use url::Url;
use zip::ZipArchive;

const USER_AGENT: &str = "open_archive/qdrant-sidecar";
const QDRANT_BINARY_NAME: &str = if cfg!(windows) {
    "qdrant.exe"
} else {
    "qdrant"
};
const LOCALHOST_HOSTS: &[&str] = &["127.0.0.1", "localhost"];

#[derive(Debug)]
pub struct ManagedQdrantHandle {
    child: Option<Child>,
}

impl Drop for ManagedQdrantHandle {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        match child.try_wait() {
            Ok(Some(_)) => {}
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
            }
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ManagedQdrantState {
    url: String,
}

pub fn install_managed_qdrant(config: &AppConfig) -> ConfigResult<PathBuf> {
    let managed = managed_config(config)?;
    resolve_binary(managed)
}

pub fn ensure_managed_qdrant(config: &mut AppConfig) -> ConfigResult<Option<ManagedQdrantHandle>> {
    let VectorStoreConfig::Qdrant(qdrant) = &mut config.vector_store else {
        return Ok(None);
    };
    if !qdrant.managed.enabled || !should_manage_url(&qdrant.url)? {
        return Ok(None);
    }
    if qdrant_healthy(&qdrant.url, qdrant.request_timeout) {
        return Ok(None);
    }

    let state_path = managed_state_path(&qdrant.managed);
    if let Some(state_url) = healthy_state_url(&state_path, qdrant.request_timeout) {
        qdrant.url = state_url;
        return Ok(None);
    }

    let _lock = StartupLock::acquire(
        &managed_lock_path(&qdrant.managed),
        qdrant.managed.startup_timeout,
    )?;

    if qdrant_healthy(&qdrant.url, qdrant.request_timeout) {
        return Ok(None);
    }
    if let Some(state_url) = healthy_state_url(&state_path, qdrant.request_timeout) {
        qdrant.url = state_url;
        return Ok(None);
    }

    let runtime_url = select_runtime_url(qdrant)?;
    if qdrant_healthy(&runtime_url, qdrant.request_timeout) {
        write_state(&state_path, &runtime_url)?;
        qdrant.url = runtime_url;
        return Ok(None);
    }

    let binary_path = resolve_binary(&qdrant.managed)?;
    let child = spawn_qdrant(&binary_path, qdrant, &runtime_url)?;
    let child = wait_until_healthy(&runtime_url, qdrant.managed.startup_timeout, child)?;
    write_state(&state_path, &runtime_url)?;
    qdrant.url = runtime_url;
    Ok(Some(ManagedQdrantHandle { child: Some(child) }))
}

fn managed_config(config: &AppConfig) -> ConfigResult<&ManagedQdrantConfig> {
    let VectorStoreConfig::Qdrant(qdrant) = &config.vector_store else {
        return Err(invalid_vector_store(
            "managed Qdrant requires OA_VECTOR_STORE=qdrant",
        ));
    };
    Ok(&qdrant.managed)
}

fn should_manage_url(url: &str) -> ConfigResult<bool> {
    let parsed = Url::parse(url).map_err(|source| {
        invalid_vector_store(&format!("invalid OA_QDRANT_URL {url}: {source}"))
    })?;
    Ok(parsed.scheme() == "http"
        && parsed
            .host_str()
            .is_some_and(|host| LOCALHOST_HOSTS.contains(&host)))
}

fn resolve_binary(config: &ManagedQdrantConfig) -> ConfigResult<PathBuf> {
    if let Some(path) = config.binary_path.clone() {
        if path.exists() {
            return Ok(path);
        }
        return Err(invalid_vector_store(&format!(
            "managed Qdrant binary path {} does not exist",
            path.display()
        )));
    }

    if let Some(path) = bundled_binary_path()? {
        return Ok(path);
    }

    let version_dir = version_install_dir(config);
    let binary_path = version_dir.join(QDRANT_BINARY_NAME);
    if binary_path.exists() {
        return Ok(binary_path);
    }

    download_and_extract_release(config, &binary_path)?;
    Ok(binary_path)
}

fn bundled_binary_path() -> ConfigResult<Option<PathBuf>> {
    let Ok(exe) = std::env::current_exe() else {
        return Ok(None);
    };
    let Some(parent) = exe.parent() else {
        return Ok(None);
    };
    for candidate in [
        parent.join(QDRANT_BINARY_NAME),
        parent.join("bin").join(QDRANT_BINARY_NAME),
        parent.join("qdrant").join(QDRANT_BINARY_NAME),
    ] {
        if candidate.exists() {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

fn download_and_extract_release(
    config: &ManagedQdrantConfig,
    binary_path: &Path,
) -> ConfigResult<()> {
    fs::create_dir_all(version_install_dir(config)).map_err(|source| {
        invalid_vector_store(&format!(
            "failed to create Qdrant install directory: {source}"
        ))
    })?;

    let asset = platform_asset_name()?;
    let url = format!(
        "https://github.com/qdrant/qdrant/releases/download/v{}/{}",
        config.version, asset
    );
    let client = Client::builder()
        .timeout(config.startup_timeout)
        .build()
        .map_err(|source| {
            invalid_vector_store(&format!("failed to build Qdrant download client: {source}"))
        })?;
    let response = client
        .get(&url)
        .header(reqwest::header::USER_AGENT, USER_AGENT)
        .send()
        .and_then(|response| response.error_for_status())
        .map_err(|source| {
            invalid_vector_store(&format!(
                "failed to download Qdrant release asset {url}: {source}"
            ))
        })?;
    let archive_bytes = response.bytes().map_err(|source| {
        invalid_vector_store(&format!(
            "failed to read Qdrant release asset body: {source}"
        ))
    })?;

    let tmp_dir = config.install_root.join("tmp");
    fs::create_dir_all(&tmp_dir).map_err(|source| {
        invalid_vector_store(&format!("failed to create Qdrant temp directory: {source}"))
    })?;
    let extract_dir = tmp_dir.join(format!("extract-{}", std::process::id()));
    if extract_dir.exists() {
        fs::remove_dir_all(&extract_dir).map_err(|source| {
            invalid_vector_store(&format!("failed to clear Qdrant temp directory: {source}"))
        })?;
    }
    fs::create_dir_all(&extract_dir).map_err(|source| {
        invalid_vector_store(&format!(
            "failed to create Qdrant extract directory: {source}"
        ))
    })?;

    if asset.ends_with(".tar.gz") {
        let decoder = GzDecoder::new(Cursor::new(archive_bytes));
        let mut archive = Archive::new(decoder);
        archive.unpack(&extract_dir).map_err(|source| {
            invalid_vector_store(&format!(
                "failed to extract Qdrant tarball {asset}: {source}"
            ))
        })?;
    } else if asset.ends_with(".zip") {
        let mut archive = ZipArchive::new(Cursor::new(archive_bytes)).map_err(|source| {
            invalid_vector_store(&format!("failed to open Qdrant zip {asset}: {source}"))
        })?;
        archive.extract(&extract_dir).map_err(|source| {
            invalid_vector_store(&format!("failed to extract Qdrant zip {asset}: {source}"))
        })?;
    } else {
        return Err(invalid_vector_store(&format!(
            "unsupported Qdrant release asset format {asset}"
        )));
    }

    let extracted_binary = extract_dir.join(QDRANT_BINARY_NAME);
    if !extracted_binary.exists() {
        return Err(invalid_vector_store(&format!(
            "Qdrant release asset {asset} did not contain {}",
            QDRANT_BINARY_NAME
        )));
    }

    fs::copy(&extracted_binary, binary_path).map_err(|source| {
        invalid_vector_store(&format!(
            "failed to place managed Qdrant binary at {}: {source}",
            binary_path.display()
        ))
    })?;
    set_executable(binary_path)?;
    fs::remove_dir_all(&extract_dir).map_err(|source| {
        invalid_vector_store(&format!("failed to remove Qdrant temp directory: {source}"))
    })?;
    Ok(())
}

fn set_executable(path: &Path) -> ConfigResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(path)
            .map_err(|source| {
                invalid_vector_store(&format!("failed to read Qdrant binary metadata: {source}"))
            })?
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).map_err(|source| {
            invalid_vector_store(&format!(
                "failed to mark Qdrant binary executable: {source}"
            ))
        })?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

fn select_runtime_url(config: &QdrantConfig) -> ConfigResult<String> {
    let parsed = Url::parse(&config.url).map_err(|source| {
        invalid_vector_store(&format!("invalid OA_QDRANT_URL {}: {source}", config.url))
    })?;
    let requested_port = parsed.port().unwrap_or(6333);
    select_runtime_url_from(
        requested_port,
        |port| qdrant_healthy(&format!("http://127.0.0.1:{port}"), config.request_timeout),
        port_available,
    )
}

fn port_available(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).is_ok()
}

fn spawn_qdrant(
    binary_path: &Path,
    config: &QdrantConfig,
    runtime_url: &str,
) -> ConfigResult<Child> {
    let parsed = Url::parse(runtime_url).map_err(|source| {
        invalid_vector_store(&format!(
            "invalid managed Qdrant url {runtime_url}: {source}"
        ))
    })?;
    let http_port = parsed.port().unwrap_or(6333);
    let grpc_port = http_port
        .checked_add(1)
        .ok_or_else(|| invalid_vector_store("managed Qdrant gRPC port overflowed"))?;
    let host = parsed.host_str().unwrap_or("127.0.0.1");

    let log_path = &config.managed.log_path;
    if let Some(parent) = log_path.parent() {
        fs::create_dir_all(parent).map_err(|source| {
            invalid_vector_store(&format!("failed to create Qdrant log directory: {source}"))
        })?;
    }
    fs::create_dir_all(&config.managed.storage_path).map_err(|source| {
        invalid_vector_store(&format!(
            "failed to create Qdrant storage directory: {source}"
        ))
    })?;
    let config_path = write_runtime_config(&config.managed, host, http_port, grpc_port)?;

    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .map_err(|source| {
            invalid_vector_store(&format!(
                "failed to open Qdrant log file {}: {source}",
                log_path.display()
            ))
        })?;
    let stderr = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .map_err(|source| {
            invalid_vector_store(&format!(
                "failed to open Qdrant log file {}: {source}",
                log_path.display()
            ))
        })?;

    Command::new(binary_path)
        .arg("--config-path")
        .arg(&config_path)
        .current_dir(&config.managed.install_root)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .map_err(|source| {
            invalid_vector_store(&format!(
                "failed to start managed Qdrant from {}: {source}",
                binary_path.display()
            ))
        })
}

fn write_runtime_config(
    config: &ManagedQdrantConfig,
    host: &str,
    http_port: u16,
    grpc_port: u16,
) -> ConfigResult<PathBuf> {
    let path = config.install_root.join("runtime-config.yaml");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| {
            invalid_vector_store(&format!(
                "failed to create Qdrant config directory: {source}"
            ))
        })?;
    }
    let yaml = format!(
        "log_level: INFO\nstorage:\n  storage_path: {}\nservice:\n  host: {}\n  http_port: {}\n  grpc_port: {}\ncluster:\n  enabled: false\ntelemetry_disabled: true\n",
        yaml_string(&config.storage_path.to_string_lossy()),
        yaml_string(host),
        http_port,
        grpc_port
    );
    fs::write(&path, yaml).map_err(|source| {
        invalid_vector_store(&format!(
            "failed to write managed Qdrant config {}: {source}",
            path.display()
        ))
    })?;
    Ok(path)
}

fn yaml_string(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn wait_until_healthy(
    runtime_url: &str,
    timeout: Duration,
    mut child: Child,
) -> ConfigResult<Child> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if qdrant_healthy(runtime_url, Duration::from_millis(250)) {
            return Ok(child);
        }
        if let Some(status) = child.try_wait().map_err(|source| {
            invalid_vector_store(&format!(
                "failed to query managed Qdrant process status: {source}"
            ))
        })? {
            return Err(invalid_vector_store(&format!(
                "managed Qdrant exited early with status {status}"
            )));
        }
        thread::sleep(Duration::from_millis(200));
    }
    let _ = child.kill();
    let _ = child.wait();
    Err(invalid_vector_store(&format!(
        "managed Qdrant did not become healthy within {} ms",
        timeout.as_millis()
    )))
}

fn qdrant_healthy(url: &str, timeout: Duration) -> bool {
    let Ok(client) = Client::builder().timeout(timeout).build() else {
        return false;
    };
    client
        .get(format!("{}/collections", url.trim_end_matches('/')))
        .header(reqwest::header::USER_AGENT, USER_AGENT)
        .send()
        .and_then(|response| response.error_for_status())
        .is_ok()
}

fn version_install_dir(config: &ManagedQdrantConfig) -> PathBuf {
    config
        .install_root
        .join("bin")
        .join(format!("v{}", config.version))
}

fn managed_state_path(config: &ManagedQdrantConfig) -> PathBuf {
    config.install_root.join("runtime-state.json")
}

fn managed_lock_path(config: &ManagedQdrantConfig) -> PathBuf {
    config.install_root.join("runtime.lock")
}

fn write_state(path: &Path, url: &str) -> ConfigResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| {
            invalid_vector_store(&format!(
                "failed to create Qdrant runtime directory: {source}"
            ))
        })?;
    }
    let state = serde_json::to_vec(&ManagedQdrantState {
        url: url.to_string(),
    })
    .map_err(|source| {
        invalid_vector_store(&format!(
            "failed to serialize Qdrant runtime state: {source}"
        ))
    })?;
    fs::write(path, state).map_err(|source| {
        invalid_vector_store(&format!(
            "failed to write Qdrant runtime state {}: {source}",
            path.display()
        ))
    })
}

fn healthy_state_url(path: &Path, timeout: Duration) -> Option<String> {
    let state = fs::read(path).ok()?;
    let state: ManagedQdrantState = serde_json::from_slice(&state).ok()?;
    if qdrant_healthy(&state.url, timeout) {
        Some(state.url)
    } else {
        None
    }
}

fn platform_asset_name() -> ConfigResult<&'static str> {
    platform_asset_name_for(std::env::consts::OS, std::env::consts::ARCH)
}

fn invalid_vector_store(message: &str) -> ConfigError {
    ConfigError::InvalidVectorStoreConfig {
        message: message.to_string(),
    }
}

struct StartupLock {
    path: PathBuf,
}

impl StartupLock {
    fn acquire(path: &Path, timeout: Duration) -> ConfigResult<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| {
                invalid_vector_store(&format!(
                    "failed to create Qdrant runtime directory: {source}"
                ))
            })?;
        }

        let deadline = Instant::now() + timeout;
        loop {
            match OpenOptions::new().write(true).create_new(true).open(path) {
                Ok(mut file) => {
                    let _ = writeln!(file, "pid={}", std::process::id());
                    return Ok(Self {
                        path: path.to_path_buf(),
                    });
                }
                Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                    if Instant::now() >= deadline {
                        return Err(invalid_vector_store(&format!(
                            "timed out waiting for Qdrant startup lock {}",
                            path.display()
                        )));
                    }
                    thread::sleep(Duration::from_millis(200));
                }
                Err(source) => {
                    return Err(invalid_vector_store(&format!(
                        "failed to create Qdrant startup lock {}: {source}",
                        path.display()
                    )));
                }
            }
        }
    }
}

impl Drop for StartupLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn platform_asset_name_for(os: &str, arch: &str) -> ConfigResult<&'static str> {
    match (os, arch) {
        ("macos", "aarch64") => Ok("qdrant-aarch64-apple-darwin.tar.gz"),
        ("macos", "x86_64") => Ok("qdrant-x86_64-apple-darwin.tar.gz"),
        ("linux", "x86_64") => Ok("qdrant-x86_64-unknown-linux-gnu.tar.gz"),
        ("linux", "aarch64") => Ok("qdrant-aarch64-unknown-linux-musl.tar.gz"),
        ("windows", "x86_64") => Ok("qdrant-x86_64-pc-windows-msvc.zip"),
        (os, arch) => Err(invalid_vector_store(&format!(
            "managed Qdrant does not support platform {os}/{arch}"
        ))),
    }
}

fn select_runtime_url_from<F, G>(
    requested_port: u16,
    healthy: F,
    available: G,
) -> ConfigResult<String>
where
    F: Fn(u16) -> bool,
    G: Fn(u16) -> bool,
{
    for offset in 0..64 {
        let candidate_port = requested_port.saturating_add(offset);
        if healthy(candidate_port) || available(candidate_port) {
            return Ok(format!("http://127.0.0.1:{candidate_port}"));
        }
    }
    Err(invalid_vector_store(&format!(
        "failed to find an available managed Qdrant port starting at {requested_port}"
    )))
}

#[cfg(test)]
mod tests {
    use super::{platform_asset_name_for, select_runtime_url_from};
    use crate::config::{ManagedQdrantConfig, QdrantConfig};
    use std::path::PathBuf;
    use std::time::Duration;

    #[test]
    fn platform_asset_selection_matches_supported_targets() {
        assert_eq!(
            platform_asset_name_for("macos", "aarch64").unwrap(),
            "qdrant-aarch64-apple-darwin.tar.gz"
        );
        assert_eq!(
            platform_asset_name_for("macos", "x86_64").unwrap(),
            "qdrant-x86_64-apple-darwin.tar.gz"
        );
        assert_eq!(
            platform_asset_name_for("linux", "x86_64").unwrap(),
            "qdrant-x86_64-unknown-linux-gnu.tar.gz"
        );
        assert_eq!(
            platform_asset_name_for("windows", "x86_64").unwrap(),
            "qdrant-x86_64-pc-windows-msvc.zip"
        );
        assert!(platform_asset_name_for("linux", "riscv64").is_err());
    }

    #[test]
    fn runtime_url_selection_reuses_requested_port_when_free() {
        let url = select_runtime_url_from(6333, |_| false, |_| true).expect("url");
        assert_eq!(url, "http://127.0.0.1:6333");
    }

    #[test]
    fn runtime_url_selection_skips_busy_ports() {
        let url = select_runtime_url_from(6333, |_| false, |port| port == 6335).expect("url");
        assert_eq!(url, "http://127.0.0.1:6335");
    }

    #[allow(dead_code)]
    fn _sample_config() -> QdrantConfig {
        QdrantConfig {
            url: "http://127.0.0.1:6333".to_string(),
            collection_name: "oa_derived_object_embeddings".to_string(),
            request_timeout: Duration::from_secs(1),
            exact: true,
            managed: ManagedQdrantConfig {
                enabled: true,
                version: "1.17.1".to_string(),
                install_root: PathBuf::from("/tmp/open_archive_qdrant"),
                storage_path: PathBuf::from("/tmp/open_archive_qdrant/storage"),
                log_path: PathBuf::from("/tmp/open_archive_qdrant/qdrant.log"),
                startup_timeout: Duration::from_secs(5),
                binary_path: None,
            },
        }
    }
}
