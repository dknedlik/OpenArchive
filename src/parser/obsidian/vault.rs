use std::collections::BTreeSet;
use std::io::{Cursor, Read};

use zip::read::ZipArchive;

use crate::error::{ParserError, ParserResult};
use crate::parser::document::{parse_document, DocumentBlockKind, DocumentFormat, ParsedDocument};

use super::frontmatter::{normalize_alias, parse_frontmatter};
use super::links::{normalize_note_path, VaultLinkResolver};
use super::tags::extract_inline_tags;
use super::{ParsedVault, ParsedVaultAlias, ParsedVaultNote, ParsedVaultTag};

const MAX_VAULT_NOTES: usize = 10_000;
const MAX_VAULT_NOTE_BYTES: usize = 64 * 1024 * 1024;

pub fn parse_vault_zip(input: &[u8]) -> ParserResult<ParsedVault> {
    let reader = Cursor::new(input);
    let mut archive = ZipArchive::new(reader).map_err(|err| ParserError::InvalidObsidianVault {
        detail: format!("failed to open zip archive: {err}"),
    })?;

    let mut notes = Vec::new();
    let mut total_note_bytes = 0usize;
    for index in 0..archive.len() {
        let mut file =
            archive
                .by_index(index)
                .map_err(|err| ParserError::InvalidObsidianVault {
                    detail: format!("failed to read zip entry {index}: {err}"),
                })?;
        let path = normalize_archive_path(file.name());
        if file.is_dir() || should_skip_path(&path) || !path.ends_with(".md") {
            continue;
        }
        if notes.len() >= MAX_VAULT_NOTES {
            return Err(ParserError::InvalidObsidianVault {
                detail: format!("vault exceeds maximum supported note count of {MAX_VAULT_NOTES}"),
            });
        }
        let file_size =
            usize::try_from(file.size()).map_err(|_| ParserError::InvalidObsidianVault {
                detail: format!("note {path} exceeds supported size limits"),
            })?;
        total_note_bytes = total_note_bytes.saturating_add(file_size);
        if total_note_bytes > MAX_VAULT_NOTE_BYTES {
            return Err(ParserError::InvalidObsidianVault {
                detail: format!(
                    "vault exceeds maximum supported markdown payload size of {MAX_VAULT_NOTE_BYTES} bytes"
                ),
            });
        }

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|err| ParserError::InvalidObsidianVault {
                detail: format!("failed to read note {path}: {err}"),
            })?;
        let raw_text =
            String::from_utf8(bytes).map_err(|err| ParserError::InvalidObsidianVault {
                detail: format!("note {path} is not valid UTF-8: {err}"),
            })?;
        if raw_text.trim().is_empty() {
            continue;
        }
        notes.push((path, raw_text));
    }

    if notes.is_empty() {
        return Err(ParserError::EmptyVault);
    }

    let note_paths = notes
        .iter()
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    let resolver = VaultLinkResolver::new(&note_paths);

    let parsed_notes = notes
        .into_iter()
        .map(|(note_path, raw_text)| parse_note(&note_path, &raw_text, &resolver))
        .collect::<ParserResult<Vec<_>>>()?;

    Ok(ParsedVault {
        notes: parsed_notes,
    })
}

fn parse_note(
    note_path: &str,
    raw_text: &str,
    resolver: &VaultLinkResolver,
) -> ParserResult<ParsedVaultNote> {
    let frontmatter = parse_frontmatter(note_path, raw_text)?;
    let document = parse_note_document(raw_text, &frontmatter.body)?;
    let mut tags = frontmatter.tags;
    tags.extend(extract_inline_tags(&frontmatter.body));
    let tags = dedupe_tags(tags);
    let aliases = dedupe_aliases(frontmatter.aliases);
    let links = resolver.parse_links(note_path, &frontmatter.body);

    Ok(ParsedVaultNote {
        note_path: normalize_note_path(note_path),
        title: frontmatter
            .title
            .or_else(|| first_heading_title(&document))
            .or_else(|| filename_stem(note_path)),
        document,
        properties: frontmatter.properties,
        tags,
        aliases,
        links,
    })
}

fn parse_note_document(raw_text: &str, stripped_body: &str) -> ParserResult<ParsedDocument> {
    match parse_document(stripped_body.as_bytes(), DocumentFormat::Markdown) {
        Ok(document) => Ok(document),
        Err(ParserError::EmptyDocument) => {
            parse_document(raw_text.as_bytes(), DocumentFormat::Markdown)
        }
        Err(err) => Err(err),
    }
}

fn dedupe_tags(tags: Vec<ParsedVaultTag>) -> Vec<ParsedVaultTag> {
    let mut seen = BTreeSet::new();
    let mut deduped = Vec::new();
    for tag in tags {
        let key = format!("{}:{}", tag.source_kind.as_str(), tag.normalized_tag);
        if seen.insert(key) {
            deduped.push(tag);
        }
    }
    deduped
}

fn dedupe_aliases(aliases: Vec<ParsedVaultAlias>) -> Vec<ParsedVaultAlias> {
    let mut seen = BTreeSet::new();
    let mut deduped = Vec::new();
    for alias in aliases {
        let normalized_alias = normalize_alias(&alias.alias_text);
        if normalized_alias.is_empty() || !seen.insert(normalized_alias.clone()) {
            continue;
        }
        deduped.push(ParsedVaultAlias {
            alias_text: alias.alias_text,
            normalized_alias,
        });
    }
    deduped
}

fn normalize_archive_path(path: &str) -> String {
    normalize_note_path(path)
}

fn should_skip_path(path: &str) -> bool {
    path.starts_with(".obsidian/") || path.starts_with(".git/")
}

fn filename_stem(path: &str) -> Option<String> {
    path.rsplit_once('/')
        .map(|(_, tail)| tail)
        .unwrap_or(path)
        .strip_suffix(".md")
        .map(ToOwned::to_owned)
}

fn first_heading_title(document: &ParsedDocument) -> Option<String> {
    document
        .blocks
        .iter()
        .find(|block| block.block_kind == DocumentBlockKind::Heading)
        .map(|block| block.text_content.clone())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use zip::write::SimpleFileOptions;

    use super::*;

    fn vault_zip() -> Vec<u8> {
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file("Inbox.md", SimpleFileOptions::default())
            .expect("file should start");
        writer
            .write_all(
                b"---\n\
                  tags: [project/acme]\n\
                  aliases: [Inbox Note]\n\
                  ---\n\
                  # Inbox\n\
                  Link to [[Projects/Acme|Acme]].",
            )
            .expect("note should write");
        writer
            .start_file("Projects/Acme.md", SimpleFileOptions::default())
            .expect("file should start");
        writer
            .write_all(b"# Acme\nContent")
            .expect("note should write");
        writer.finish().expect("zip should finish").into_inner()
    }

    #[test]
    fn parses_markdown_notes_from_zip_vault() {
        let parsed = parse_vault_zip(&vault_zip()).expect("vault should parse");
        assert_eq!(parsed.notes.len(), 2);
        assert_eq!(parsed.notes[0].tags[0].normalized_tag, "project/acme");
        assert_eq!(parsed.notes[0].aliases[0].normalized_alias, "inbox note");
        assert_eq!(
            parsed.notes[0].links[0].target_path.as_deref(),
            Some("Projects/Acme.md")
        );
    }

    #[test]
    fn falls_back_to_filename_when_note_has_no_heading_or_frontmatter_title() {
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file("Sandbox/Plain note.md", SimpleFileOptions::default())
            .expect("file should start");
        writer
            .write_all(b"First paragraph.\n\nSecond paragraph.")
            .expect("note should write");
        let bytes = writer.finish().expect("zip should finish").into_inner();

        let parsed = parse_vault_zip(&bytes).expect("vault should parse");
        assert_eq!(parsed.notes[0].title.as_deref(), Some("Plain note"));
    }

    #[test]
    fn rejects_vaults_that_exceed_note_count_limit() {
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = zip::ZipWriter::new(cursor);
        for index in 0..=MAX_VAULT_NOTES {
            writer
                .start_file(format!("note-{index}.md"), SimpleFileOptions::default())
                .expect("file should start");
            writer.write_all(b"# note").expect("note should write");
        }
        let bytes = writer.finish().expect("zip should finish").into_inner();

        let err = parse_vault_zip(&bytes).expect_err("vault should be rejected");
        assert!(matches!(err, ParserError::InvalidObsidianVault { .. }));
    }

    #[test]
    fn skips_blank_markdown_notes() {
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file("empty.md", SimpleFileOptions::default())
            .expect("file should start");
        writer.write_all(b"   \n").expect("note should write");
        writer
            .start_file("Inbox.md", SimpleFileOptions::default())
            .expect("file should start");
        writer.write_all(b"# Inbox").expect("note should write");
        let bytes = writer.finish().expect("zip should finish").into_inner();

        let parsed = parse_vault_zip(&bytes).expect("vault should parse");
        assert_eq!(parsed.notes.len(), 1);
        assert_eq!(parsed.notes[0].note_path, "Inbox.md");
    }
}
