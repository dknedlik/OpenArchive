use std::collections::BTreeSet;

use crate::storage::ImportedNoteTagSourceKind;

use super::{normalize_tag, ParsedVaultTag};

pub fn extract_inline_tags(body: &str) -> Vec<ParsedVaultTag> {
    let mut seen = BTreeSet::new();
    let mut tags = Vec::new();
    let mut in_code_fence = false;

    for line in body.lines() {
        let trimmed = line.trim_start();
        if is_code_fence_delimiter(trimmed) {
            in_code_fence = !in_code_fence;
            continue;
        }
        if in_code_fence || should_skip_inline_tag_scan(trimmed) {
            continue;
        }

        for raw in scan_line_for_tags(line) {
            let Some(normalized) = normalize_tag(&raw) else {
                continue;
            };
            if !seen.insert(normalized.clone()) {
                continue;
            }
            tags.push(ParsedVaultTag {
                raw_tag: raw,
                normalized_tag: normalized.clone(),
                tag_path: normalized,
                source_kind: ImportedNoteTagSourceKind::Inline,
            });
        }
    }

    tags
}

fn is_code_fence_delimiter(trimmed: &str) -> bool {
    trimmed.starts_with("```") || trimmed.starts_with("~~~")
}

fn should_skip_inline_tag_scan(trimmed: &str) -> bool {
    let lowered = trimmed.to_ascii_lowercase();
    trimmed.contains("${")
        || trimmed.contains("<%")
        || trimmed.contains("%>")
        || trimmed.contains("{{")
        || lowered.contains("dataview")
        || lowered.contains("dv.")
        || lowered.contains("tp.")
        || lowered.contains("tag=")
        || lowered.starts_with("const ")
        || lowered.starts_with("let ")
        || lowered.starts_with("var ")
        || lowered.starts_with("function ")
        || lowered.starts_with("return ")
        || lowered.starts_with("await ")
}

fn scan_line_for_tags(line: &str) -> Vec<String> {
    let bytes = line.as_bytes();
    let mut tags = Vec::new();
    let mut index = 0usize;

    while index < bytes.len() {
        if bytes[index] != b'#' {
            index += 1;
            continue;
        }

        if index > 0 {
            let prev = bytes[index - 1];
            if !prev.is_ascii_whitespace() && prev != b'(' && prev != b'[' && prev != b'{' {
                index += 1;
                continue;
            }
        }

        let start = index + 1;
        let mut end = start;
        while end < bytes.len() {
            let ch = bytes[end];
            if ch.is_ascii_alphanumeric() || ch == b'_' || ch == b'-' || ch == b'/' {
                end += 1;
                continue;
            }
            break;
        }

        if end > start {
            tags.push(line[index..end].to_string());
            index = end;
        } else {
            index += 1;
        }
    }

    tags
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_inline_tags_outside_code_fences() {
        let tags = extract_inline_tags(
            "hello #Project/Acme\n\
             ```\n\
             #skip/me\n\
             ```\n\
             trailing #todo",
        );

        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0].normalized_tag, "project/acme");
        assert_eq!(tags[1].normalized_tag, "todo");
    }

    #[test]
    fn skips_tags_inside_tilde_fences() {
        let tags = extract_inline_tags(
            "before #one\n\
             ~~~md\n\
             #skip/me\n\
             ~~~\n\
             after #two",
        );

        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0].normalized_tag, "one");
        assert_eq!(tags[1].normalized_tag, "two");
    }

    #[test]
    fn skips_dataview_and_template_lines() {
        let tags = extract_inline_tags(
            "real prose #project\n\
             const pages = dv.pages('#dashboard')\n\
             <%* const nav = '#templated' %>\n\
             tag=#project\n\
             trailing #todo",
        );

        let normalized = tags
            .iter()
            .map(|tag| tag.normalized_tag.as_str())
            .collect::<Vec<_>>();
        assert_eq!(normalized, vec!["project", "todo"]);
    }
}
