use std::collections::BTreeSet;
use std::fs;
use std::io::{Cursor, Read};
use std::path::Path;

use zip::read::ZipArchive;

use crate::error::{ParserError, ParserResult};
use crate::parser::document::{parse_document, DocumentBlockKind, DocumentFormat, ParsedDocument};

use super::frontmatter::{normalize_alias, parse_frontmatter};
use super::links::{normalize_note_path, VaultLinkResolver};
use super::tags::extract_inline_tags;
use super::{ParsedVault, ParsedVaultAlias, ParsedVaultNote, ParsedVaultTag};

/// Metadata for a file discovered in a vault directory.
#[derive(Debug, Clone)]
pub struct VaultDirectoryFile {
    pub relative_path: String,
    pub absolute_path: std::path::PathBuf,
    pub content: String,
    pub mtime_millis: Option<i64>,
}

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

/// Parse an Obsidian vault from a directory on disk.
/// Returns the parsed vault and metadata about the files found.
/// Files are skipped if they exceed size limits or are not valid UTF-8.
pub fn parse_vault_directory(root: &Path) -> ParserResult<(ParsedVault, Vec<VaultDirectoryFile>)> {
    let mut files = Vec::new();
    let mut total_note_bytes = 0usize;

    collect_vault_files(root, root, &mut files, &mut total_note_bytes)?;

    if files.is_empty() {
        return Err(ParserError::EmptyVault);
    }

    // Build resolver from file paths
    let note_paths: Vec<String> = files.iter().map(|f| f.relative_path.clone()).collect();
    let resolver = VaultLinkResolver::new(&note_paths);

    // Parse all notes
    let parsed_notes: ParserResult<Vec<_>> = files
        .iter()
        .map(|file| parse_note(&file.relative_path, &file.content, &resolver))
        .collect();
    let parsed_notes = parsed_notes?;

    Ok((
        ParsedVault {
            notes: parsed_notes,
        },
        files,
    ))
}

fn collect_vault_files(
    root: &Path,
    current: &Path,
    files: &mut Vec<VaultDirectoryFile>,
    total_bytes: &mut usize,
) -> ParserResult<()> {
    let entries = fs::read_dir(current).map_err(|err| ParserError::InvalidObsidianVault {
        detail: format!("failed to read directory {}: {err}", current.display()),
    })?;

    for entry in entries {
        let entry = entry.map_err(|err| ParserError::InvalidObsidianVault {
            detail: format!("failed to read directory entry: {err}"),
        })?;
        let path = entry.path();

        if path.is_dir() {
            // Skip hidden directories
            if entry
                .file_name()
                .to_str()
                .map(|name| name.starts_with('.'))
                .unwrap_or(false)
            {
                continue;
            }
            collect_vault_files(root, &path, files, total_bytes)?;
            continue;
        }

        let relative = path
            .strip_prefix(root)
            .map_err(|_| ParserError::InvalidObsidianVault {
                detail: format!("failed to compute relative path for {}", path.display()),
            })?;
        let relative_path = relative.to_str().map(normalize_note_path).ok_or_else(|| {
            ParserError::InvalidObsidianVault {
                detail: format!("invalid path encoding for {}", path.display()),
            }
        })?;

        if should_skip_path(&relative_path) || !relative_path.ends_with(".md") {
            continue;
        }

        if files.len() >= MAX_VAULT_NOTES {
            return Err(ParserError::InvalidObsidianVault {
                detail: format!("vault exceeds maximum supported note count of {MAX_VAULT_NOTES}"),
            });
        }

        let metadata = fs::metadata(&path).map_err(|err| ParserError::InvalidObsidianVault {
            detail: format!("failed to read metadata for {}: {err}", path.display()),
        })?;
        let file_size = metadata.len() as usize;

        *total_bytes = total_bytes.saturating_add(file_size);
        if *total_bytes > MAX_VAULT_NOTE_BYTES {
            return Err(ParserError::InvalidObsidianVault {
                detail: format!(
                    "vault exceeds maximum supported markdown payload size of {MAX_VAULT_NOTE_BYTES} bytes"
                ),
            });
        }

        let mtime_millis = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|dur| dur.as_millis() as i64);

        let bytes = fs::read(&path).map_err(|err| ParserError::InvalidObsidianVault {
            detail: format!("failed to read note {}: {err}", path.display()),
        })?;
        let content =
            String::from_utf8(bytes).map_err(|err| ParserError::InvalidObsidianVault {
                detail: format!("note {} is not valid UTF-8: {err}", path.display()),
            })?;

        if content.trim().is_empty() {
            continue;
        }

        files.push(VaultDirectoryFile {
            relative_path,
            absolute_path: path,
            content,
            mtime_millis,
        });
    }

    Ok(())
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

/// Check if a path should be skipped during vault parsing.
/// Skips hidden directories (starting with '.') and their contents.
pub fn should_skip_path(path: &str) -> bool {
    // Skip any path component starting with '.' (hidden directories/files)
    path.split('/').any(|component| component.starts_with('.'))
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

    // =========================================================================
    // Directory Parsing Tests
    // =========================================================================

    use std::fs;
    use tempfile::TempDir;

    fn create_test_vault_dir() -> (TempDir, Vec<VaultDirectoryFile>) {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let root = temp_dir.path();

        // Create nested structure
        fs::create_dir(root.join("Projects")).expect("dir should create");
        fs::create_dir(root.join("Projects").join("Acme")).expect("dir should create");
        fs::create_dir(root.join(".obsidian")).expect("hidden dir should create");
        fs::create_dir(root.join(".private")).expect("hidden dir should create");

        // Create notes
        fs::write(
            root.join("Inbox.md"),
            "---\ntags: [project/acme]\naliases: [Inbox Note]\n---\n# Inbox\nLink to [[Projects/Acme|Acme]].",
        )
        .expect("note should write");

        fs::write(root.join("Projects").join("Acme.md"), "# Acme\nContent")
            .expect("note should write");

        fs::write(
            root.join("Projects").join("Acme").join("Tasks.md"),
            "# Tasks\n- [ ] Do something",
        )
        .expect("note should write");

        // Hidden notes (should be skipped)
        fs::write(root.join(".obsidian").join("config.md"), "# Config").ok();
        fs::write(root.join(".private").join("secret.md"), "# Secret").ok();

        let (_vault, files) = parse_vault_directory(root).expect("vault should parse");
        (temp_dir, files)
    }

    #[test]
    fn parse_vault_directory_finds_all_markdown_files() {
        let (_temp, files) = create_test_vault_dir();

        // Should find exactly 3 markdown files
        assert_eq!(files.len(), 3);

        let paths: Vec<&str> = files.iter().map(|f| f.relative_path.as_str()).collect();
        assert!(paths.contains(&"Inbox.md"));
        assert!(paths.contains(&"Projects/Acme.md"));
        assert!(paths.contains(&"Projects/Acme/Tasks.md"));
    }

    #[test]
    fn parse_vault_directory_skips_hidden_directories() {
        let (_temp, files) = create_test_vault_dir();

        // Should NOT include files from .obsidian or .private
        let paths: Vec<&str> = files.iter().map(|f| f.relative_path.as_str()).collect();
        assert!(!paths.iter().any(|p| p.contains(".obsidian")));
        assert!(!paths.iter().any(|p| p.contains(".private")));
        assert!(!paths.contains(&".obsidian/config.md"));
        assert!(!paths.contains(&".private/secret.md"));
    }

    #[test]
    fn parse_vault_directory_preserves_content() {
        let (_temp, files) = create_test_vault_dir();

        let inbox = files
            .iter()
            .find(|f| f.relative_path == "Inbox.md")
            .expect("Inbox.md should exist");
        assert!(inbox.content.contains("# Inbox"));
        assert!(inbox.content.contains("project/acme"));
    }

    #[test]
    fn parse_vault_directory_captures_mtime() {
        let (_temp, files) = create_test_vault_dir();

        // All files should have mtime captured
        for file in &files {
            assert!(
                file.mtime_millis.is_some(),
                "{} should have mtime",
                file.relative_path
            );
            assert!(file.mtime_millis.unwrap() > 0);
        }
    }

    #[test]
    fn parse_vault_directory_produces_equivalent_vault_to_zip() {
        let (temp_dir, _files) = create_test_vault_dir();

        // Parse directory
        let (dir_vault, _) = parse_vault_directory(temp_dir.path()).expect("dir should parse");

        // Create equivalent zip
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
        writer
            .start_file("Projects/Acme/Tasks.md", SimpleFileOptions::default())
            .expect("file should start");
        writer
            .write_all(b"# Tasks\n- [ ] Do something")
            .expect("note should write");
        let zip_bytes = writer.finish().expect("zip should finish").into_inner();

        let zip_vault = parse_vault_zip(&zip_bytes).expect("zip should parse");

        // Both should have same number of notes
        assert_eq!(dir_vault.notes.len(), zip_vault.notes.len());

        // Both should have same note paths
        let dir_paths: std::collections::BTreeSet<_> =
            dir_vault.notes.iter().map(|n| &n.note_path).collect();
        let zip_paths: std::collections::BTreeSet<_> =
            zip_vault.notes.iter().map(|n| &n.note_path).collect();
        assert_eq!(dir_paths, zip_paths);
    }

    #[test]
    fn should_skip_path_skips_all_hidden() {
        assert!(should_skip_path(".obsidian/config"));
        assert!(should_skip_path(".git/objects"));
        assert!(should_skip_path(".private/notes.md"));
        assert!(should_skip_path("notes/.hidden/file.md"));
        assert!(!should_skip_path("Projects/Acme.md"));
        assert!(!should_skip_path("Inbox.md"));
    }
}
