use crate::error::{ParserError, ParserResult};
use crate::storage::{ImportedNotePropertyValueKind, ImportedNoteTagSourceKind};

use super::{normalize_tag, ParsedVaultAlias, ParsedVaultProperty, ParsedVaultTag};

#[derive(Debug)]
pub struct ParsedFrontmatter {
    pub body: String,
    pub title: Option<String>,
    pub properties: Vec<ParsedVaultProperty>,
    pub tags: Vec<ParsedVaultTag>,
    pub aliases: Vec<ParsedVaultAlias>,
}

pub fn parse_frontmatter(note_path: &str, raw_text: &str) -> ParserResult<ParsedFrontmatter> {
    let normalized = raw_text.replace("\r\n", "\n").replace('\r', "\n");
    let Some(frontmatter) = split_frontmatter(&normalized) else {
        return Ok(ParsedFrontmatter {
            body: normalized,
            title: None,
            properties: Vec::new(),
            tags: Vec::new(),
            aliases: Vec::new(),
        });
    };

    let yaml_value: serde_yaml::Value =
        serde_yaml::from_str(frontmatter.yaml).map_err(|err| ParserError::InvalidFrontmatter {
            note_path: note_path.to_string(),
            detail: err.to_string(),
        })?;
    let mapping = yaml_value
        .as_mapping()
        .ok_or_else(|| ParserError::InvalidFrontmatter {
            note_path: note_path.to_string(),
            detail: "frontmatter must be a YAML mapping".to_string(),
        })?;

    let mut properties = Vec::new();
    let mut tags = Vec::new();
    let mut aliases = Vec::new();
    let mut title = None;

    for (key, value) in mapping {
        let Some(property_key) = key.as_str() else {
            continue;
        };
        let normalized_key = property_key.trim().to_string();
        if normalized_key.is_empty() {
            continue;
        }

        let value_json = serde_json::to_value(value).unwrap_or(serde_json::Value::Null);
        let value_kind = detect_value_kind(value);
        let value_text = scalar_text(value);
        if normalized_key.eq_ignore_ascii_case("title") && title.is_none() {
            title = value_text.clone();
        }
        if normalized_key.eq_ignore_ascii_case("tags") || normalized_key.eq_ignore_ascii_case("tag")
        {
            tags.extend(extract_tags_from_value(value));
        }
        if normalized_key.eq_ignore_ascii_case("aliases")
            || normalized_key.eq_ignore_ascii_case("alias")
        {
            aliases.extend(extract_aliases_from_value(value));
        }

        properties.push(ParsedVaultProperty {
            property_key: normalized_key,
            value_kind,
            value_text,
            value_json,
        });
    }

    Ok(ParsedFrontmatter {
        body: frontmatter.body.to_string(),
        title,
        properties,
        tags,
        aliases,
    })
}

struct SplitFrontmatter<'a> {
    yaml: &'a str,
    body: &'a str,
}

fn split_frontmatter(raw_text: &str) -> Option<SplitFrontmatter<'_>> {
    if !raw_text.starts_with("---\n") {
        return None;
    }

    let remaining = &raw_text[4..];
    if let Some(end) = remaining.find("\n---\n") {
        let yaml = &remaining[..end];
        let body = &remaining[end + 5..];
        return Some(SplitFrontmatter { yaml, body });
    }

    let yaml = remaining.strip_suffix("\n---")?;
    Some(SplitFrontmatter { yaml, body: "" })
}

fn detect_value_kind(value: &serde_yaml::Value) -> ImportedNotePropertyValueKind {
    match value {
        serde_yaml::Value::Null => ImportedNotePropertyValueKind::Null,
        serde_yaml::Value::Bool(_) => ImportedNotePropertyValueKind::Boolean,
        serde_yaml::Value::Number(_) => ImportedNotePropertyValueKind::Number,
        serde_yaml::Value::Sequence(_) => ImportedNotePropertyValueKind::List,
        serde_yaml::Value::Mapping(_) => ImportedNotePropertyValueKind::Json,
        serde_yaml::Value::String(text) => detect_string_kind(text),
        _ => ImportedNotePropertyValueKind::Json,
    }
}

fn detect_string_kind(text: &str) -> ImportedNotePropertyValueKind {
    let trimmed = text.trim();
    if is_date(trimmed) {
        ImportedNotePropertyValueKind::Date
    } else if is_datetime(trimmed) {
        ImportedNotePropertyValueKind::DateTime
    } else {
        ImportedNotePropertyValueKind::String
    }
}

fn scalar_text(value: &serde_yaml::Value) -> Option<String> {
    match value {
        serde_yaml::Value::Null => None,
        serde_yaml::Value::Bool(v) => Some(v.to_string()),
        serde_yaml::Value::Number(v) => Some(v.to_string()),
        serde_yaml::Value::String(v) => Some(v.trim().to_string()),
        _ => None,
    }
}

fn extract_tags_from_value(value: &serde_yaml::Value) -> Vec<ParsedVaultTag> {
    flatten_tag_strings(value)
        .into_iter()
        .filter_map(|raw| {
            normalize_tag(&raw).map(|normalized| ParsedVaultTag {
                raw_tag: raw,
                tag_path: normalized.clone(),
                normalized_tag: normalized,
                source_kind: ImportedNoteTagSourceKind::Frontmatter,
            })
        })
        .collect()
}

fn extract_aliases_from_value(value: &serde_yaml::Value) -> Vec<ParsedVaultAlias> {
    flatten_scalar_strings(value)
        .into_iter()
        .filter_map(|alias_text| {
            let normalized_alias = normalize_alias(&alias_text);
            if normalized_alias.is_empty() {
                return None;
            }
            Some(ParsedVaultAlias {
                alias_text,
                normalized_alias,
            })
        })
        .collect()
}

fn flatten_tag_strings(value: &serde_yaml::Value) -> Vec<String> {
    match value {
        serde_yaml::Value::String(text) => split_tag_string(text),
        serde_yaml::Value::Sequence(values) => {
            values.iter().flat_map(flatten_tag_strings).collect()
        }
        _ => scalar_text(value)
            .into_iter()
            .flat_map(|value| split_tag_string(&value))
            .collect(),
    }
}

fn split_tag_string(text: &str) -> Vec<String> {
    text.split(|ch: char| ch == ',' || ch.is_whitespace())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn flatten_scalar_strings(value: &serde_yaml::Value) -> Vec<String> {
    match value {
        serde_yaml::Value::String(text) => text
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect(),
        serde_yaml::Value::Sequence(values) => {
            values.iter().flat_map(flatten_scalar_strings).collect()
        }
        _ => scalar_text(value).into_iter().collect(),
    }
}

pub fn normalize_alias(raw: &str) -> String {
    raw.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_lowercase()
}

fn is_date(value: &str) -> bool {
    if value.len() != 10 {
        return false;
    }
    let bytes = value.as_bytes();
    bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes.iter().enumerate().all(|(index, ch)| match index {
            4 | 7 => true,
            _ => ch.is_ascii_digit(),
        })
}

fn is_datetime(value: &str) -> bool {
    value.contains('T') && value.chars().any(|ch| ch == ':' || ch == 'Z' || ch == '+')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_frontmatter_properties_tags_and_aliases() {
        let parsed = parse_frontmatter(
            "note.md",
            "---\n\
             title: Sample\n\
             tags:\n\
               - project/Acme\n\
               - inbox\n\
             aliases: [OA, Open Archive]\n\
             score: 3\n\
             ---\n\
             body",
        )
        .expect("frontmatter should parse");

        assert_eq!(parsed.title.as_deref(), Some("Sample"));
        assert_eq!(parsed.tags.len(), 2);
        assert_eq!(parsed.tags[0].normalized_tag, "project/acme");
        assert_eq!(parsed.aliases[0].normalized_alias, "oa");
        assert_eq!(parsed.properties.len(), 4);
        assert_eq!(parsed.body.trim(), "body");
    }

    #[test]
    fn parses_frontmatter_when_closing_delimiter_is_at_eof() {
        let parsed = parse_frontmatter("note.md", "---\n title: Sample\n---")
            .expect("frontmatter should parse");

        assert_eq!(parsed.title.as_deref(), Some("Sample"));
        assert_eq!(parsed.body, "");
        assert_eq!(parsed.properties.len(), 1);
    }

    #[test]
    fn splits_space_delimited_frontmatter_tags() {
        let parsed = parse_frontmatter(
            "note.md",
            "---\n\
             tags: dashboard project top-secret-project\n\
             ---\n\
             body",
        )
        .expect("frontmatter should parse");

        let tags = parsed
            .tags
            .iter()
            .map(|tag| tag.normalized_tag.as_str())
            .collect::<Vec<_>>();
        assert_eq!(tags, vec!["dashboard", "project", "top-secret-project"]);
    }
}
