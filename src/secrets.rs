//! Secret storage for API keys and sensitive configuration values.
//!
//! Provides OS-native keyring integration with a plain-file fallback for
//! environments where keyring is unavailable. Environment variables always
//! take precedence for CI/scripting compatibility.

use crate::error::SecretStoreError;
use std::env;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

const SERVICE_NAME: &str = "open-archive";
const SECRETS_FILE_NAME: &str = "secrets";

/// Secret storage backend indicator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretBackend {
    /// OS-native keyring (Keychain on Mac, Credential Manager on Windows,
    /// libsecret on Linux).
    Keyring,
    /// Plain file fallback at ~/.open_archive/secrets with 0600 permissions.
    PlainFile,
}

impl SecretBackend {
    /// Returns the backend name as a lowercase string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Keyring => "keyring",
            Self::PlainFile => "plain_file",
        }
    }
}

/// Unified secret storage that tries keyring first, falls back to plain file.
pub struct SecretStore {
    backend: SecretBackend,
    fallback_path: PathBuf,
}

impl SecretStore {
    /// Create a new secret store, detecting the best available backend.
    ///
    /// Attempts to use the OS keyring first. If keyring operations fail
    /// during initialization, falls back to a plain file at
    /// ~/.open_archive/secrets.
    pub fn new() -> Self {
        let fallback_path = default_secrets_path();

        // Try keyring first - create a test entry to verify it works
        match Self::probe_keyring() {
            Ok(()) => Self {
                backend: SecretBackend::Keyring,
                fallback_path,
            },
            Err(_) => Self {
                backend: SecretBackend::PlainFile,
                fallback_path,
            },
        }
    }

    /// Get a secret value by key name.
    ///
    /// Lookup order:
    /// 1. Environment variable (key name) - always wins for CI/scripting
    /// 2. Keyring entry (service: "open-archive", account: key)
    /// 3. Plain file fallback (if keyring unavailable)
    ///
    /// Returns `None` if the secret is not found in any location.
    /// Errors are logged but not returned - callers only care about presence.
    pub fn get(&self, key: &str) -> Option<String> {
        // 1. Env var always wins
        if let Ok(value) = env::var(key) {
            return Some(value);
        }

        // 2. Try the configured backend
        match self.backend {
            SecretBackend::Keyring => self.get_from_keyring(key),
            SecretBackend::PlainFile => self.get_from_file(key),
        }
    }

    /// Store a secret value.
    ///
    /// Uses the configured backend (keyring if available, otherwise plain file).
    /// Creates the secrets directory if needed.
    pub fn set(&self, key: &str, value: &str) -> Result<(), SecretStoreError> {
        match self.backend {
            SecretBackend::Keyring => self.set_in_keyring(key, value),
            SecretBackend::PlainFile => self.set_in_file(key, value),
        }
    }

    /// Delete a secret from storage.
    pub fn delete(&self, key: &str) -> Result<(), SecretStoreError> {
        match self.backend {
            SecretBackend::Keyring => self.delete_from_keyring(key),
            SecretBackend::PlainFile => self.delete_from_file(key),
        }
    }

    /// Return which backend is in use.
    pub fn backend(&self) -> SecretBackend {
        self.backend
    }

    // Keyring operations

    fn probe_keyring() -> Result<(), SecretStoreError> {
        // Try to access the keyring by getting a dummy entry
        // We don't care if it exists, just that the keyring is accessible
        let entry = keyring::Entry::new(SERVICE_NAME, "__probe__")
            .map_err(|e| SecretStoreError::KeyringUnavailable(e.to_string()))?;
        match entry.get_password() {
            Ok(_) => Ok(()),
            Err(keyring::Error::NoEntry) => Ok(()), // Keyring works, just no entry
            Err(e) => Err(SecretStoreError::KeyringUnavailable(e.to_string())),
        }
    }

    fn get_from_keyring(&self, key: &str) -> Option<String> {
        let entry = match keyring::Entry::new(SERVICE_NAME, key) {
            Ok(e) => e,
            Err(e) => {
                log::debug!("Failed to create keyring entry for '{}': {}", key, e);
                return None;
            }
        };
        match entry.get_password() {
            Ok(value) => Some(value),
            Err(keyring::Error::NoEntry) => None,
            Err(e) => {
                log::debug!("Failed to get secret '{}' from keyring: {}", key, e);
                None
            }
        }
    }

    fn set_in_keyring(&self, key: &str, value: &str) -> Result<(), SecretStoreError> {
        let entry = keyring::Entry::new(SERVICE_NAME, key)
            .map_err(|e| SecretStoreError::KeyringUnavailable(e.to_string()))?;
        entry
            .set_password(value)
            .map_err(|e| SecretStoreError::KeyringUnavailable(e.to_string()))
    }

    fn delete_from_keyring(&self, key: &str) -> Result<(), SecretStoreError> {
        let entry = keyring::Entry::new(SERVICE_NAME, key)
            .map_err(|e| SecretStoreError::KeyringUnavailable(e.to_string()))?;
        match entry.delete_credential() {
            Ok(_) => Ok(()),
            Err(keyring::Error::NoEntry) => Err(SecretStoreError::KeyNotFound),
            Err(e) => Err(SecretStoreError::KeyringUnavailable(e.to_string())),
        }
    }

    // Plain file operations

    fn get_from_file(&self, key: &str) -> Option<String> {
        let content = match fs::read_to_string(&self.fallback_path) {
            Ok(content) => content,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
            Err(e) => {
                log::debug!(
                    "Failed to read secrets file '{}': {}",
                    self.fallback_path.display(),
                    e
                );
                return None;
            }
        };

        parse_secret_value(&content, key)
    }

    fn set_in_file(&self, key: &str, value: &str) -> Result<(), SecretStoreError> {
        // Ensure parent directory exists
        if let Some(parent) = self.fallback_path.parent() {
            fs::create_dir_all(parent).map_err(|e| SecretStoreError::Io { source: e })?;
        }

        // Read existing content or start fresh
        let mut secrets: Vec<(String, String)> = match fs::read_to_string(&self.fallback_path) {
            Ok(content) => parse_all_secrets(&content),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(SecretStoreError::Io { source: e }),
        };

        // Update or insert
        let pos = secrets.iter().position(|(k, _)| k == key);
        match pos {
            Some(idx) => secrets[idx].1 = value.to_string(),
            None => secrets.push((key.to_string(), value.to_string())),
        }

        // Write back with atomic permission setting on Unix
        #[cfg(unix)]
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&self.fallback_path)
            .map_err(|e| SecretStoreError::Io { source: e })?;

        #[cfg(not(unix))]
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.fallback_path)
            .map_err(|e| SecretStoreError::Io { source: e })?;

        for (k, v) in secrets {
            writeln!(file, "{}={}", k, v).map_err(|e| SecretStoreError::Io { source: e })?;
        }

        Ok(())
    }

    fn delete_from_file(&self, key: &str) -> Result<(), SecretStoreError> {
        let content = match fs::read_to_string(&self.fallback_path) {
            Ok(content) => content,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(SecretStoreError::KeyNotFound);
            }
            Err(e) => return Err(SecretStoreError::Io { source: e }),
        };

        let mut secrets = parse_all_secrets(&content);
        let pos = secrets.iter().position(|(k, _)| k == key);

        match pos {
            Some(idx) => {
                secrets.remove(idx);

                // Write back remaining secrets with atomic permission setting on Unix
                #[cfg(unix)]
                let mut file = fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .mode(0o600)
                    .open(&self.fallback_path)
                    .map_err(|e| SecretStoreError::Io { source: e })?;

                #[cfg(not(unix))]
                let mut file = fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&self.fallback_path)
                    .map_err(|e| SecretStoreError::Io { source: e })?;

                for (k, v) in secrets {
                    writeln!(file, "{}={}", k, v)
                        .map_err(|e| SecretStoreError::Io { source: e })?;
                }

                Ok(())
            }
            None => Err(SecretStoreError::KeyNotFound),
        }
    }
}

impl Default for SecretStore {
    fn default() -> Self {
        Self::new()
    }
}

fn default_secrets_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".open_archive")
        .join(SECRETS_FILE_NAME)
}

/// Parse a single secret value from KEY=value format.
fn parse_secret_value(content: &str, key: &str) -> Option<String> {
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some(eq_pos) = line.find('=') {
            let k = line[..eq_pos].trim();
            if k == key {
                let v = line[eq_pos + 1..].trim();
                return Some(v.to_string());
            }
        }
    }
    None
}

/// Parse all secrets from KEY=value format.
fn parse_all_secrets(content: &str) -> Vec<(String, String)> {
    let mut secrets = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some(eq_pos) = line.find('=') {
            let k = line[..eq_pos].trim().to_string();
            let v = line[eq_pos + 1..].trim().to_string();
            secrets.push((k, v));
        }
    }
    secrets
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_secret() {
        let content = "OA_GEMINI_API_KEY=sk-test123\n";
        assert_eq!(
            parse_secret_value(content, "OA_GEMINI_API_KEY"),
            Some("sk-test123".to_string())
        );
        assert_eq!(parse_secret_value(content, "OA_OPENAI_API_KEY"), None);
    }

    #[test]
    fn parse_multiple_secrets() {
        let content = "OA_GEMINI_API_KEY=sk-gemini\nOA_OPENAI_API_KEY=sk-openai\n";
        let secrets = parse_all_secrets(content);
        assert_eq!(secrets.len(), 2);
        assert_eq!(secrets[0].0, "OA_GEMINI_API_KEY");
        assert_eq!(secrets[0].1, "sk-gemini");
        assert_eq!(secrets[1].0, "OA_OPENAI_API_KEY");
        assert_eq!(secrets[1].1, "sk-openai");
    }

    #[test]
    fn parse_skips_comments_and_empty() {
        let content = "# This is a comment\n\nOA_KEY=value\n  \n";
        assert_eq!(
            parse_secret_value(content, "OA_KEY"),
            Some("value".to_string())
        );
    }

    #[test]
    fn parse_handles_values_with_equals() {
        let content = "OA_KEY=val=ue=with=equals\n";
        assert_eq!(
            parse_secret_value(content, "OA_KEY"),
            Some("val=ue=with=equals".to_string())
        );
    }

    #[test]
    fn plain_file_set_and_get() {
        let temp_dir = tempfile::tempdir().unwrap();
        let secrets_path = temp_dir.path().join("secrets");

        let store = SecretStore {
            backend: SecretBackend::PlainFile,
            fallback_path: secrets_path.clone(),
        };

        // Set a secret
        store.set("OA_TEST_KEY", "test-value").unwrap();

        // Read it back
        let value = store.get_from_file("OA_TEST_KEY");
        assert_eq!(value, Some("test-value".to_string()));

        // Update it
        store.set("OA_TEST_KEY", "new-value").unwrap();
        let value = store.get_from_file("OA_TEST_KEY");
        assert_eq!(value, Some("new-value".to_string()));

        // Add another
        store.set("OA_OTHER_KEY", "other-value").unwrap();
        let value = store.get_from_file("OA_OTHER_KEY");
        assert_eq!(value, Some("other-value".to_string()));

        // First key should still exist
        let value = store.get_from_file("OA_TEST_KEY");
        assert_eq!(value, Some("new-value".to_string()));
    }

    #[test]
    fn plain_file_delete() {
        let temp_dir = tempfile::tempdir().unwrap();
        let secrets_path = temp_dir.path().join("secrets");

        let store = SecretStore {
            backend: SecretBackend::PlainFile,
            fallback_path: secrets_path.clone(),
        };

        store.set("OA_KEY1", "value1").unwrap();
        store.set("OA_KEY2", "value2").unwrap();

        // Delete first key
        store.delete("OA_KEY1").unwrap();
        assert_eq!(store.get_from_file("OA_KEY1"), None);
        assert_eq!(store.get_from_file("OA_KEY2"), Some("value2".to_string()));

        // Delete non-existent key
        assert!(matches!(
            store.delete("OA_NONEXISTENT"),
            Err(SecretStoreError::KeyNotFound)
        ));
    }

    #[test]
    fn env_var_wins() {
        // This test relies on env vars, so we need to be careful
        // We'll skip if the env var is already set
        let key = "OA_TEST_SECRET_ENV_VAR";
        if env::var(key).is_ok() {
            return; // Skip if already set
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let store = SecretStore {
            backend: SecretBackend::PlainFile,
            fallback_path: temp_dir.path().join("secrets"),
        };

        // Set in file first
        store.set(key, "file-value").unwrap();
        assert_eq!(store.get(key), Some("file-value".to_string()));

        // Set env var and verify it wins
        env::set_var(key, "env-value");
        assert_eq!(store.get(key), Some("env-value".to_string()));

        // Clean up
        env::remove_var(key);
    }
}
