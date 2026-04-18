use crate::error::{ConfigError, ConfigResult};
use std::env;
use std::path::PathBuf;
use std::time::Duration;

pub(super) fn default_local_archive_root() -> ConfigResult<PathBuf> {
    Ok(home_dir()?.join(".open_archive"))
}

fn home_dir() -> ConfigResult<PathBuf> {
    Ok(PathBuf::from(env::var("HOME").map_err(|_| {
        ConfigError::MissingEnvWithDependency { key: "HOME" }
    })?))
}

pub(super) fn optional_path_env(key: &'static str) -> ConfigResult<Option<PathBuf>> {
    optional_trimmed_env(key)
        .map(|value| expand_home_path(&value))
        .transpose()
}

pub fn expand_home_path(value: &str) -> ConfigResult<PathBuf> {
    let Some(expanded) = expand_with_home(value)? else {
        return Ok(PathBuf::from(value));
    };
    Ok(PathBuf::from(expanded))
}

fn expand_with_home(value: &str) -> ConfigResult<Option<String>> {
    let Some(home) = home_dir_string_if_needed(value)? else {
        return Ok(None);
    };
    Ok(Some(
        if value == "~" || value == "$HOME" || value == "${HOME}" {
            home
        } else if let Some(rest) = value.strip_prefix("~/") {
            format!("{home}/{rest}")
        } else if let Some(rest) = value.strip_prefix("$HOME/") {
            format!("{home}/{rest}")
        } else if let Some(rest) = value.strip_prefix("${HOME}/") {
            format!("{home}/{rest}")
        } else {
            home
        },
    ))
}

fn home_dir_string_if_needed(value: &str) -> ConfigResult<Option<String>> {
    if matches!(value, "~" | "$HOME" | "${HOME}")
        || value.starts_with("~/")
        || value.starts_with("$HOME/")
        || value.starts_with("${HOME}/")
    {
        let home = dirs::home_dir().ok_or(ConfigError::MissingEnvWithDependency { key: "HOME" })?;
        Ok(Some(home.to_string_lossy().to_string()))
    } else {
        Ok(None)
    }
}

pub(super) fn positive_u32_env(key: &'static str) -> ConfigResult<Option<u32>> {
    match env::var(key) {
        Ok(raw) => {
            let value = raw
                .parse::<u32>()
                .map_err(|_| ConfigError::InvalidPositiveIntegerEnv {
                    key,
                    value: raw.clone(),
                })?;
            if value == 0 {
                return Err(ConfigError::InvalidPositiveIntegerEnv { key, value: raw });
            }
            Ok(Some(value))
        }
        Err(_) => Ok(None),
    }
}

pub(super) fn required_env(key: &'static str) -> ConfigResult<String> {
    env::var(key).map_err(|_| ConfigError::MissingEnv { key })
}

pub(super) fn optional_trimmed_env(key: &'static str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(super) fn optional_duration_env_ms(key: &'static str) -> ConfigResult<Option<Duration>> {
    match env::var(key) {
        Ok(raw) => {
            let value = raw
                .parse::<u64>()
                .map_err(|_| ConfigError::InvalidPositiveIntegerEnv {
                    key,
                    value: raw.clone(),
                })?;
            if value == 0 {
                return Err(ConfigError::InvalidPositiveIntegerEnv { key, value: raw });
            }
            Ok(Some(Duration::from_millis(value)))
        }
        Err(_) => Ok(None),
    }
}

pub(super) fn bool_env(key: &'static str) -> ConfigResult<Option<bool>> {
    match env::var(key) {
        Ok(raw) => match raw.as_str() {
            "true" | "1" => Ok(Some(true)),
            "false" | "0" => Ok(Some(false)),
            _ => Err(ConfigError::InvalidEnumEnv {
                key,
                value: raw,
                expected: "true, false, 1, 0",
            }),
        },
        Err(_) => Ok(None),
    }
}

pub(super) fn positive_usize_env(key: &'static str) -> ConfigResult<Option<usize>> {
    match env::var(key) {
        Ok(raw) => {
            let value =
                raw.parse::<usize>()
                    .map_err(|_| ConfigError::InvalidPositiveIntegerEnv {
                        key,
                        value: raw.clone(),
                    })?;
            if value == 0 {
                return Err(ConfigError::InvalidPositiveIntegerEnv { key, value: raw });
            }
            Ok(Some(value))
        }
        Err(_) => Ok(None),
    }
}

pub(super) fn optional_usize_env(key: &'static str) -> ConfigResult<Option<usize>> {
    match env::var(key) {
        Ok(raw) => {
            let value =
                raw.parse::<usize>()
                    .map_err(|_| ConfigError::InvalidPositiveIntegerEnv {
                        key,
                        value: raw.clone(),
                    })?;
            Ok(Some(value))
        }
        Err(_) => Ok(None),
    }
}
