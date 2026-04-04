use std::collections::{BTreeMap, BTreeSet};

use url::Url;

use crate::storage::{
    ImportedNoteLinkKind, ImportedNoteLinkResolutionStatus, ImportedNoteLinkTargetKind,
};

use super::ParsedVaultLink;

#[derive(Debug, Default)]
pub struct VaultLinkResolver {
    exact_paths: BTreeSet<String>,
    stem_index: BTreeMap<String, Vec<String>>,
}

impl VaultLinkResolver {
    pub fn new(note_paths: &[String]) -> Self {
        let mut resolver = Self::default();
        for path in note_paths {
            let normalized = normalize_note_path(path);
            resolver.exact_paths.insert(normalized.clone());
            resolver
                .stem_index
                .entry(note_stem(&normalized))
                .or_default()
                .push(normalized);
        }
        resolver
    }

    pub fn parse_links(&self, note_path: &str, body: &str) -> Vec<ParsedVaultLink> {
        let mut links = Vec::new();
        let wikilinks = parse_wikilinks(note_path, body, self);
        let spans = wikilinks
            .iter()
            .map(|parsed| parsed.span.clone())
            .collect::<Vec<_>>();
        links.extend(wikilinks.into_iter().map(|parsed| parsed.link));
        links.extend(parse_markdown_links(note_path, body, self, &spans));
        links
    }
}

#[derive(Debug)]
struct ParsedLinkMatch {
    span: std::ops::Range<usize>,
    link: ParsedVaultLink,
}

fn parse_wikilinks(
    note_path: &str,
    body: &str,
    resolver: &VaultLinkResolver,
) -> Vec<ParsedLinkMatch> {
    let mut links = Vec::new();
    let bytes = body.as_bytes();
    let mut index = 0usize;

    while index + 1 < bytes.len() {
        let is_embed = bytes[index] == b'!' && bytes[index + 1] == b'[';
        let open_index = if is_embed {
            if index + 2 >= bytes.len() || bytes[index + 2] != b'[' {
                index += 1;
                continue;
            }
            index + 1
        } else if bytes[index] == b'[' && bytes[index + 1] == b'[' {
            index
        } else {
            index += 1;
            continue;
        };

        let start = open_index + 2;
        let mut end = start;
        while end + 1 < bytes.len() && !(bytes[end] == b']' && bytes[end + 1] == b']') {
            end += 1;
        }
        if end + 1 >= bytes.len() {
            break;
        }
        let raw_target = body[start..end].trim();
        if !raw_target.is_empty() && !is_dynamic_template_target(raw_target) {
            links.push(ParsedLinkMatch {
                span: index..(end + 2),
                link: resolve_internal_link(
                    note_path,
                    raw_target,
                    if is_embed {
                        ImportedNoteLinkKind::Embed
                    } else {
                        ImportedNoteLinkKind::Link
                    },
                    resolver,
                ),
            });
        }
        index = end + 2;
    }

    links
}

fn parse_markdown_links(
    note_path: &str,
    body: &str,
    resolver: &VaultLinkResolver,
    blocked_spans: &[std::ops::Range<usize>],
) -> Vec<ParsedVaultLink> {
    let mut links = Vec::new();
    let bytes = body.as_bytes();
    let mut index = 0usize;

    while index < bytes.len() {
        if let Some(span_end) = blocked_spans
            .iter()
            .find(|span| span.contains(&index))
            .map(|span| span.end)
        {
            index = span_end;
            continue;
        }

        let is_embed = bytes[index] == b'!';
        let open_index = if is_embed {
            if index + 1 >= bytes.len() || bytes[index + 1] != b'[' {
                index += 1;
                continue;
            }
            index + 1
        } else if bytes[index] == b'[' {
            index
        } else {
            index += 1;
            continue;
        };

        let Some(label_end) = find_markdown_label_end(bytes, open_index) else {
            index += 1;
            continue;
        };
        let target_start = label_end + 2;
        let Some(target_end) = find_markdown_target_end(bytes, target_start) else {
            index += 1;
            continue;
        };
        let label = body[open_index + 1..label_end].trim();
        let raw_target = body[target_start..target_end].trim();
        if !raw_target.is_empty() && !is_dynamic_template_target(raw_target) {
            let link_kind = if is_embed {
                ImportedNoteLinkKind::Embed
            } else {
                ImportedNoteLinkKind::Link
            };
            let mut link = resolve_uri_link(note_path, raw_target, label, link_kind, resolver)
                .unwrap_or_else(|| {
                    resolve_internal_link(note_path, raw_target, link_kind, resolver)
                });
            if !label.is_empty() {
                link.display_text = Some(label.to_string());
            }
            links.push(link);
        }
        index = target_end + 1;
    }

    links
}

fn find_markdown_label_end(bytes: &[u8], open_index: usize) -> Option<usize> {
    let mut depth = 1usize;
    let mut index = open_index + 1;

    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index += 2,
            b'[' => {
                depth += 1;
                index += 1;
            }
            b']' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return (index + 1 < bytes.len() && bytes[index + 1] == b'(').then_some(index);
                }
                index += 1;
            }
            _ => index += 1,
        }
    }

    None
}

fn find_markdown_target_end(bytes: &[u8], target_start: usize) -> Option<usize> {
    let mut depth = 1usize;
    let mut index = target_start;

    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index += 2,
            b'(' => {
                depth += 1;
                index += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(index);
                }
                index += 1;
            }
            _ => index += 1,
        }
    }

    None
}

fn is_dynamic_template_target(raw_target: &str) -> bool {
    raw_target.contains("${")
        || raw_target.contains("<%")
        || raw_target.contains("%>")
        || raw_target.contains("{{")
}

fn resolve_uri_link(
    note_path: &str,
    raw_target: &str,
    label: &str,
    link_kind: ImportedNoteLinkKind,
    resolver: &VaultLinkResolver,
) -> Option<ParsedVaultLink> {
    let url = Url::parse(raw_target).ok()?;
    let mut link = match url.scheme() {
        "obsidian" => resolve_obsidian_uri(note_path, raw_target, link_kind, resolver)
            .unwrap_or_else(|| external_link(raw_target, label, link_kind)),
        "file" => resolve_file_uri(note_path, raw_target, link_kind, resolver)
            .unwrap_or_else(|| external_link(raw_target, label, link_kind)),
        _ if is_uri_scheme_target(raw_target) => external_link(raw_target, label, link_kind),
        _ => return None,
    };
    if !label.is_empty() {
        link.display_text = Some(label.to_string());
    }
    Some(link)
}

fn resolve_internal_link(
    note_path: &str,
    raw_target: &str,
    link_kind: ImportedNoteLinkKind,
    resolver: &VaultLinkResolver,
) -> ParsedVaultLink {
    let (path_part, heading, block, display_text) = split_target(raw_target);
    let normalized_target = normalize_target(raw_target);

    let (target_kind, target_path, resolution_status) = if path_part.is_empty() && heading.is_some()
    {
        (
            if block.is_some() {
                ImportedNoteLinkTargetKind::Block
            } else {
                ImportedNoteLinkTargetKind::Heading
            },
            Some(normalize_note_path(note_path)),
            ImportedNoteLinkResolutionStatus::Resolved,
        )
    } else if let Some(resolved_path) = resolver.resolve_note_target(note_path, &path_part) {
        (
            if block.is_some() {
                ImportedNoteLinkTargetKind::Block
            } else if heading.is_some() {
                ImportedNoteLinkTargetKind::Heading
            } else {
                ImportedNoteLinkTargetKind::Note
            },
            Some(resolved_path),
            ImportedNoteLinkResolutionStatus::Resolved,
        )
    } else if looks_like_attachment(&path_part) {
        (
            ImportedNoteLinkTargetKind::Attachment,
            Some(resolve_relative_attachment_target(note_path, &path_part)),
            ImportedNoteLinkResolutionStatus::Unresolved,
        )
    } else {
        (
            if block.is_some() {
                ImportedNoteLinkTargetKind::Block
            } else if heading.is_some() {
                ImportedNoteLinkTargetKind::Heading
            } else {
                ImportedNoteLinkTargetKind::Note
            },
            if path_part.is_empty() {
                Some(normalize_note_path(note_path))
            } else {
                Some(resolve_relative_target(note_path, &path_part))
            },
            ImportedNoteLinkResolutionStatus::Unresolved,
        )
    };

    ParsedVaultLink {
        source_block_index: None,
        link_kind,
        target_kind,
        raw_target: raw_target.to_string(),
        normalized_target,
        display_text,
        target_path,
        target_heading: heading,
        target_block: block,
        external_url: None,
        resolution_status,
        locator_json: None,
    }
}

fn resolve_obsidian_uri(
    note_path: &str,
    raw_target: &str,
    link_kind: ImportedNoteLinkKind,
    resolver: &VaultLinkResolver,
) -> Option<ParsedVaultLink> {
    let url = Url::parse(raw_target).ok()?;
    let action = url.host_str().unwrap_or_default().to_lowercase();
    let params = url.query_pairs().collect::<BTreeMap<_, _>>();

    match action.as_str() {
        "open" | "advanced-uri" => {
            for key in ["file", "filepath", "path", "note"] {
                if let Some(value) = params.get(key) {
                    if let Some(resolved) = resolve_uri_note_reference(
                        raw_target, note_path, value, link_kind, resolver,
                    ) {
                        return Some(resolved);
                    }
                }
            }
            None
        }
        _ => None,
    }
}

fn resolve_file_uri(
    note_path: &str,
    raw_target: &str,
    link_kind: ImportedNoteLinkKind,
    resolver: &VaultLinkResolver,
) -> Option<ParsedVaultLink> {
    let url = Url::parse(raw_target).ok()?;
    let uri_path = url.path();
    if uri_path.is_empty() {
        return None;
    }

    resolver
        .resolve_uri_path(uri_path)
        .map(|resolved_path| build_resolved_note_link(raw_target, link_kind, resolved_path))
        .or_else(|| {
            if is_markdown_note_path(uri_path) {
                resolve_uri_note_reference(raw_target, note_path, uri_path, link_kind, resolver)
            } else {
                None
            }
        })
}

fn resolve_uri_note_reference(
    raw_target: &str,
    note_path: &str,
    target: &str,
    link_kind: ImportedNoteLinkKind,
    resolver: &VaultLinkResolver,
) -> Option<ParsedVaultLink> {
    let (path_part, heading, block, display_text) = split_target(target);

    if let Some(resolved_path) = resolver
        .resolve_uri_path(&path_part)
        .or_else(|| resolver.resolve_note_target(note_path, &path_part))
    {
        return Some(ParsedVaultLink {
            source_block_index: None,
            link_kind,
            target_kind: if block.is_some() {
                ImportedNoteLinkTargetKind::Block
            } else if heading.is_some() {
                ImportedNoteLinkTargetKind::Heading
            } else {
                ImportedNoteLinkTargetKind::Note
            },
            raw_target: raw_target.to_string(),
            normalized_target: normalize_target(raw_target),
            display_text,
            target_path: Some(resolved_path),
            target_heading: heading,
            target_block: block,
            external_url: None,
            resolution_status: ImportedNoteLinkResolutionStatus::Resolved,
            locator_json: None,
        });
    }

    None
}

fn build_resolved_note_link(
    raw_target: &str,
    link_kind: ImportedNoteLinkKind,
    resolved_path: String,
) -> ParsedVaultLink {
    ParsedVaultLink {
        source_block_index: None,
        link_kind,
        target_kind: ImportedNoteLinkTargetKind::Note,
        raw_target: raw_target.to_string(),
        normalized_target: normalize_target(raw_target),
        display_text: None,
        target_path: Some(resolved_path),
        target_heading: None,
        target_block: None,
        external_url: None,
        resolution_status: ImportedNoteLinkResolutionStatus::Resolved,
        locator_json: None,
    }
}

fn external_link(
    raw_target: &str,
    label: &str,
    link_kind: ImportedNoteLinkKind,
) -> ParsedVaultLink {
    ParsedVaultLink {
        source_block_index: None,
        link_kind,
        target_kind: ImportedNoteLinkTargetKind::External,
        raw_target: raw_target.to_string(),
        normalized_target: Some(raw_target.trim().to_lowercase()),
        display_text: (!label.is_empty()).then(|| label.to_string()),
        target_path: None,
        target_heading: None,
        target_block: None,
        external_url: Some(raw_target.to_string()),
        resolution_status: ImportedNoteLinkResolutionStatus::External,
        locator_json: None,
    }
}

impl VaultLinkResolver {
    fn resolve_note_target(&self, current_note_path: &str, target: &str) -> Option<String> {
        if target.trim().is_empty() {
            return Some(normalize_note_path(current_note_path));
        }

        let candidate = resolve_relative_target(current_note_path, target);
        if self.exact_paths.contains(&candidate) {
            return Some(candidate);
        }

        let stem = note_stem(&normalize_note_path(target));
        let matches = self.stem_index.get(&stem)?;
        if matches.len() == 1 {
            matches.first().cloned()
        } else {
            None
        }
    }

    fn resolve_uri_path(&self, target: &str) -> Option<String> {
        let normalized = normalize_note_path(target);
        if self.exact_paths.contains(&normalized) {
            return Some(normalized);
        }

        let suffix_matches = self
            .exact_paths
            .iter()
            .filter(|path| normalized == **path || normalized.ends_with(&format!("/{path}")))
            .cloned()
            .collect::<Vec<_>>();
        if suffix_matches.len() == 1 {
            return suffix_matches.first().cloned();
        }

        if is_markdown_note_path(&normalized) {
            let stem = note_stem(&normalized);
            let matches = self.stem_index.get(&stem)?;
            if matches.len() == 1 {
                return matches.first().cloned();
            }
        }

        None
    }
}

fn split_target(raw_target: &str) -> (String, Option<String>, Option<String>, Option<String>) {
    let (target_without_display, display_text) = raw_target
        .split_once('|')
        .map(|(target, display)| {
            (
                target.trim().to_string(),
                Some(display.trim().to_string()).filter(|value| !value.is_empty()),
            )
        })
        .unwrap_or_else(|| (raw_target.trim().to_string(), None));

    let (target_without_block, block) = target_without_display
        .split_once("#^")
        .map(|(target, block)| {
            (
                target.trim().to_string(),
                Some(block.trim().to_string()).filter(|value| !value.is_empty()),
            )
        })
        .unwrap_or_else(|| (target_without_display, None));

    let (path_part, heading) = target_without_block
        .split_once('#')
        .map(|(path, heading)| {
            (
                path.trim().to_string(),
                Some(heading.trim().to_string()).filter(|value| !value.is_empty()),
            )
        })
        .unwrap_or_else(|| (target_without_block, None));

    (path_part, heading, block, display_text)
}

fn resolve_relative_target(current_note_path: &str, target: &str) -> String {
    let target = target.trim().replace('\\', "/");
    let target = ensure_markdown_suffix(&target);
    if target.contains('/') {
        let current_dir = current_note_path
            .rsplit_once('/')
            .map(|(dir, _)| dir)
            .unwrap_or("");
        normalize_note_path(&join_paths(current_dir, &target))
    } else {
        normalize_note_path(&target)
    }
}

fn resolve_relative_attachment_target(current_note_path: &str, target: &str) -> String {
    let target = target.trim().replace('\\', "/");
    if target.contains('/') {
        let current_dir = current_note_path
            .rsplit_once('/')
            .map(|(dir, _)| dir)
            .unwrap_or("");
        normalize_note_path(&join_paths(current_dir, &target))
    } else {
        normalize_note_path(&target)
    }
}

fn join_paths(base: &str, child: &str) -> String {
    let mut parts = Vec::new();
    let joined = format!("{base}/{child}");
    for piece in joined.split('/') {
        match piece {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            other => parts.push(other),
        }
    }
    parts.join("/")
}

fn ensure_markdown_suffix(path: &str) -> String {
    if path.is_empty()
        || path.contains("://")
        || path.ends_with(".md")
        || path.ends_with(".markdown")
    {
        path.to_string()
    } else {
        format!("{path}.md")
    }
}

fn note_stem(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(_, tail)| tail)
        .unwrap_or(path)
        .trim_end_matches(".md")
        .to_lowercase()
}

pub fn normalize_note_path(path: &str) -> String {
    path.replace('\\', "/")
        .trim_start_matches("./")
        .trim_matches('/')
        .to_string()
}

fn normalize_target(target: &str) -> Option<String> {
    let trimmed = target.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_lowercase())
    }
}

fn looks_like_attachment(target: &str) -> bool {
    let trimmed = target.trim();
    if trimmed.is_empty() || trimmed.ends_with('/') {
        return false;
    }

    let last_segment = trimmed.rsplit('/').next().unwrap_or(trimmed);
    let Some((base, extension)) = last_segment.rsplit_once('.') else {
        return false;
    };
    if base.is_empty() || extension.is_empty() {
        return false;
    }

    !matches!(extension.to_ascii_lowercase().as_str(), "md" | "markdown")
        && matches!(
            extension.to_ascii_lowercase().as_str(),
            "avif"
                | "base"
                | "bmp"
                | "canvas"
                | "csv"
                | "doc"
                | "docx"
                | "epub"
                | "gif"
                | "heic"
                | "jpeg"
                | "jpg"
                | "m4a"
                | "mdx"
                | "mov"
                | "mp3"
                | "mp4"
                | "ogg"
                | "pdf"
                | "png"
                | "ppt"
                | "pptx"
                | "svg"
                | "txt"
                | "wav"
                | "webm"
                | "webp"
                | "xls"
                | "xlsx"
                | "zip"
        )
}

fn is_markdown_note_path(target: &str) -> bool {
    let trimmed = target.trim().to_lowercase();
    trimmed.ends_with(".md") || trimmed.ends_with(".markdown")
}

fn is_uri_scheme_target(target: &str) -> bool {
    let trimmed = target.trim();
    let Some((scheme, _)) = trimmed.split_once(':') else {
        return false;
    };
    !scheme.is_empty()
        && scheme.chars().enumerate().all(|(idx, ch)| match idx {
            0 => ch.is_ascii_alphabetic(),
            _ => ch.is_ascii_alphanumeric() || matches!(ch, '+' | '.' | '-'),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_unique_note_targets_and_external_urls() {
        let resolver = VaultLinkResolver::new(&[
            "Projects/Acme.md".to_string(),
            "People/Alice.md".to_string(),
        ]);
        let links = resolver.parse_links(
            "Inbox.md",
            "[[Projects/Acme#Roadmap]] and [site](https://example.com)",
        );

        assert_eq!(links.len(), 2);
        assert_eq!(links[0].target_path.as_deref(), Some("Projects/Acme.md"));
        assert_eq!(links[0].target_heading.as_deref(), Some("Roadmap"));
        assert_eq!(links[1].target_kind, ImportedNoteLinkTargetKind::External);
    }

    #[test]
    fn classifies_custom_uri_schemes_as_external() {
        let resolver = VaultLinkResolver::new(&["Importer.md".to_string()]);
        let links = resolver.parse_links(
            "Import notes/Import from Notion.md",
            "[Importer](obsidian://show-plugin?id=obsidian-importer)",
        );

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target_kind, ImportedNoteLinkTargetKind::External);
        assert_eq!(
            links[0].external_url.as_deref(),
            Some("obsidian://show-plugin?id=obsidian-importer")
        );
        assert_eq!(
            links[0].resolution_status,
            ImportedNoteLinkResolutionStatus::External
        );
    }

    #[test]
    fn resolves_obsidian_open_uri_to_note() {
        let resolver = VaultLinkResolver::new(&["Projects/Acme.md".to_string()]);
        let links = resolver.parse_links(
            "Inbox.md",
            "[Acme](obsidian://open?file=Projects%2FAcme.md)",
        );

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target_kind, ImportedNoteLinkTargetKind::Note);
        assert_eq!(links[0].target_path.as_deref(), Some("Projects/Acme.md"));
        assert_eq!(
            links[0].resolution_status,
            ImportedNoteLinkResolutionStatus::Resolved
        );
    }

    #[test]
    fn resolves_file_uri_to_note() {
        let resolver = VaultLinkResolver::new(&["Projects/Acme.md".to_string()]);
        let links = resolver.parse_links(
            "Inbox.md",
            "[Acme](file:///Users/david/Vault/Projects/Acme.md)",
        );

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target_kind, ImportedNoteLinkTargetKind::Note);
        assert_eq!(links[0].target_path.as_deref(), Some("Projects/Acme.md"));
        assert_eq!(
            links[0].resolution_status,
            ImportedNoteLinkResolutionStatus::Resolved
        );
    }

    #[test]
    fn keeps_attachment_paths_without_markdown_suffix() {
        let resolver = VaultLinkResolver::new(&["Guides/Start.md".to_string()]);
        let links = resolver.parse_links("Guides/Start.md", "[[lucide-menu.svg#icon]]");

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target_kind, ImportedNoteLinkTargetKind::Attachment);
        assert_eq!(links[0].target_path.as_deref(), Some("lucide-menu.svg"));
    }

    #[test]
    fn prefers_note_resolution_for_note_names_with_dots() {
        let resolver = VaultLinkResolver::new(&["Projects/some.note.with.dots.md".to_string()]);
        let links = resolver.parse_links("Inbox.md", "[[Projects/some.note.with.dots]]");

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target_kind, ImportedNoteLinkTargetKind::Note);
        assert_eq!(
            links[0].target_path.as_deref(),
            Some("Projects/some.note.with.dots.md")
        );
        assert_eq!(
            links[0].resolution_status,
            ImportedNoteLinkResolutionStatus::Resolved
        );
    }

    #[test]
    fn does_not_parse_markdown_links_inside_wikilinks() {
        let resolver = VaultLinkResolver::new(&["Projects/Acme.md".to_string()]);
        let links = resolver.parse_links(
            "Inbox.md",
            "[[Projects/Acme|Docs [site](https://example.com)]] [outside](https://openarchive.dev)",
        );

        assert_eq!(links.len(), 2);
        assert_eq!(links[0].target_kind, ImportedNoteLinkTargetKind::Note);
        assert_eq!(links[1].target_kind, ImportedNoteLinkTargetKind::External);
        assert_eq!(
            links[1].external_url.as_deref(),
            Some("https://openarchive.dev")
        );
    }

    #[test]
    fn does_not_treat_callout_markers_as_markdown_link_labels() {
        let resolver = VaultLinkResolver::new(&[]);
        let links = resolver.parse_links(
            "Tabs.md",
            "> [!note] Mobile tabs\n> On mobile, tabs appear in a tray.\nSee [Plugins](https://example.com/plugins).",
        );

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].display_text.as_deref(), Some("Plugins"));
        assert_eq!(
            links[0].external_url.as_deref(),
            Some("https://example.com/plugins")
        );
    }

    #[test]
    fn skips_dynamic_template_targets() {
        let resolver = VaultLinkResolver::new(&["Projects/Home.md".to_string()]);
        let links = resolver.parse_links(
            "Projects/Home.md",
            "[[${navLink}]]\n[Link](<% tp.file.path() %>)\n[[Projects/Home]]",
        );

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target_kind, ImportedNoteLinkTargetKind::Note);
        assert_eq!(links[0].target_path.as_deref(), Some("Projects/Home.md"));
    }
}
