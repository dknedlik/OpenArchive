#![deny(warnings)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use open_archive::parser::{self, ParsedConversation};
use open_archive::storage::{ImportedNoteLinkResolutionStatus, ImportedNoteLinkTargetKind};

#[derive(Debug, Parser)]
#[command(name = "probe_import_parse")]
#[command(about = "Parse an export file into normalized conversations without touching storage")]
struct Args {
    #[arg(long, value_enum)]
    source: ImportSource,

    #[arg(long)]
    path: PathBuf,

    #[arg(long, default_value_t = 3)]
    sample: usize,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ImportSource {
    Chatgpt,
    Claude,
    Grok,
    Gemini,
    Obsidian,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let bytes = fs::read(&args.path)
        .with_context(|| format!("failed to read export file {}", args.path.display()))?;
    if matches!(args.source, ImportSource::Obsidian) {
        return probe_obsidian(&args.path, &bytes, args.sample);
    }
    let conversations = parse_source(args.source, &bytes)?;

    let total_messages: usize = conversations.iter().map(|c| c.messages.len()).sum();
    let total_participants: usize = conversations.iter().map(|c| c.participants.len()).sum();
    let total_unsupported: usize = conversations
        .iter()
        .map(|c| {
            c.orphaned_skipped_content.len()
                + c.messages
                    .iter()
                    .map(|m| m.unsupported_content.len())
                    .sum::<usize>()
        })
        .sum();

    println!("Parse probe");
    println!("Source: {}", source_label(args.source));
    println!("Path: {}", args.path.display());
    println!("Conversations: {}", conversations.len());
    println!("Messages: {}", total_messages);
    println!("Participants: {}", total_participants);
    println!("Skipped/unsupported items: {}", total_unsupported);
    println!();

    for conversation in conversations.iter().take(args.sample) {
        let orphaned = conversation.orphaned_skipped_content.len();
        let skipped = conversation
            .messages
            .iter()
            .map(|m| m.unsupported_content.len())
            .sum::<usize>();
        println!("Artifact: {}", conversation.source_id);
        println!(
            "  title: {}",
            conversation.title.as_deref().unwrap_or("(untitled)")
        );
        println!("  messages: {}", conversation.messages.len());
        println!("  participants: {}", conversation.participants.len());
        println!("  skipped: {} attached + {} orphaned", skipped, orphaned);
        if let Some(first) = conversation.messages.first() {
            println!("  first message: {}", preview(&first.text_content));
        }
        if let Some(last) = conversation.messages.last() {
            println!("  last message: {}", preview(&last.text_content));
        }
        println!();
    }

    Ok(())
}

fn parse_source(source: ImportSource, bytes: &[u8]) -> Result<Vec<ParsedConversation>> {
    let parsed = match source {
        ImportSource::Chatgpt => parser::chatgpt::parse_conversations(bytes),
        ImportSource::Claude => parser::claude::parse_conversations(bytes),
        ImportSource::Grok => parser::grok::parse_conversations(bytes),
        ImportSource::Gemini => parser::gemini::parse_conversations(bytes),
        ImportSource::Obsidian => unreachable!("obsidian is handled separately"),
    };
    parsed.map_err(anyhow::Error::from)
}

fn source_label(source: ImportSource) -> &'static str {
    match source {
        ImportSource::Chatgpt => "chatgpt",
        ImportSource::Claude => "claude",
        ImportSource::Grok => "grok",
        ImportSource::Gemini => "gemini",
        ImportSource::Obsidian => "obsidian",
    }
}

fn probe_obsidian(path: &Path, bytes: &[u8], sample: usize) -> Result<()> {
    let vault = parser::obsidian::parse_vault_zip(bytes)?;

    let total_blocks: usize = vault
        .notes
        .iter()
        .map(|note| note.document.blocks.len())
        .sum();
    let total_properties: usize = vault.notes.iter().map(|note| note.properties.len()).sum();
    let total_tags: usize = vault.notes.iter().map(|note| note.tags.len()).sum();
    let total_aliases: usize = vault.notes.iter().map(|note| note.aliases.len()).sum();
    let total_links: usize = vault.notes.iter().map(|note| note.links.len()).sum();
    let resolved_links: usize = vault
        .notes
        .iter()
        .flat_map(|note| note.links.iter())
        .filter(|link| link.resolution_status == ImportedNoteLinkResolutionStatus::Resolved)
        .count();
    let unresolved_links: usize = vault
        .notes
        .iter()
        .flat_map(|note| note.links.iter())
        .filter(|link| link.resolution_status == ImportedNoteLinkResolutionStatus::Unresolved)
        .count();
    let external_links: usize = vault
        .notes
        .iter()
        .flat_map(|note| note.links.iter())
        .filter(|link| link.resolution_status == ImportedNoteLinkResolutionStatus::External)
        .count();

    println!("Parse probe");
    println!("Source: obsidian");
    println!("Path: {}", path.display());
    println!("Notes: {}", vault.notes.len());
    println!("Blocks: {}", total_blocks);
    println!("Properties: {}", total_properties);
    println!("Tags: {}", total_tags);
    println!("Aliases: {}", total_aliases);
    println!("Links: {}", total_links);
    println!("Resolved links: {}", resolved_links);
    println!("Unresolved links: {}", unresolved_links);
    println!("External links: {}", external_links);
    println!();

    let unresolved = vault
        .notes
        .iter()
        .flat_map(|note| {
            note.links.iter().filter_map(|link| {
                (link.resolution_status == ImportedNoteLinkResolutionStatus::Unresolved)
                    .then_some((note.note_path.as_str(), link))
            })
        })
        .collect::<Vec<_>>();
    if !unresolved.is_empty() {
        print_unresolved_breakdown(&unresolved);
    }

    for note in vault.notes.iter().take(sample) {
        println!("Note: {}", note.note_path);
        println!("  title: {}", note.title.as_deref().unwrap_or("(untitled)"));
        println!("  blocks: {}", note.document.blocks.len());
        println!("  properties: {}", note.properties.len());
        println!("  tags: {}", note.tags.len());
        println!("  aliases: {}", note.aliases.len());
        println!("  links: {}", note.links.len());
        if let Some(first_link) = note.links.first() {
            println!("  first link: {}", preview(&first_link.raw_target));
        }
        println!();
    }

    Ok(())
}

fn print_unresolved_breakdown(
    unresolved: &[(&str, &open_archive::parser::obsidian::ParsedVaultLink)],
) {
    let mut by_target_kind = BTreeMap::<&'static str, usize>::new();
    let mut attachment_exts = BTreeMap::<String, usize>::new();
    let mut raw_target_counts = BTreeMap::<String, usize>::new();

    for (_, link) in unresolved {
        *by_target_kind
            .entry(match link.target_kind {
                ImportedNoteLinkTargetKind::Attachment => "attachment",
                ImportedNoteLinkTargetKind::Note => "note",
                ImportedNoteLinkTargetKind::Heading => "heading",
                ImportedNoteLinkTargetKind::Block => "block",
                ImportedNoteLinkTargetKind::External => "external",
            })
            .or_default() += 1;
        *raw_target_counts
            .entry(link.raw_target.clone())
            .or_default() += 1;

        if link.target_kind == ImportedNoteLinkTargetKind::Attachment {
            let ext = link
                .target_path
                .as_deref()
                .and_then(|path| path.rsplit_once('.').map(|(_, ext)| ext.to_lowercase()))
                .unwrap_or_else(|| "(no extension)".to_string());
            *attachment_exts.entry(ext).or_default() += 1;
        }
    }

    println!("Unresolved breakdown:");
    for (kind, count) in by_target_kind {
        println!("  {kind}: {count}");
    }

    let mut top_targets = raw_target_counts.into_iter().collect::<Vec<_>>();
    top_targets.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    println!("Top unresolved targets:");
    for (raw_target, count) in top_targets.into_iter().take(12) {
        println!("  {count:>3}  {raw_target}");
    }

    if !attachment_exts.is_empty() {
        let mut exts = attachment_exts.into_iter().collect::<Vec<_>>();
        exts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        println!("Unresolved attachment extensions:");
        for (ext, count) in exts.into_iter().take(10) {
            println!("  {count:>3}  .{ext}");
        }
    }

    println!("Sample unresolved links:");
    for (note_path, link) in unresolved.iter().take(12) {
        println!(
            "  {} -> {} ({})",
            note_path,
            link.raw_target,
            match link.target_kind {
                ImportedNoteLinkTargetKind::Attachment => "attachment",
                ImportedNoteLinkTargetKind::Note => "note",
                ImportedNoteLinkTargetKind::Heading => "heading",
                ImportedNoteLinkTargetKind::Block => "block",
                ImportedNoteLinkTargetKind::External => "external",
            }
        );
    }
    println!();
}

fn preview(text: &str) -> String {
    const MAX_CHARS: usize = 96;
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let count = compact.chars().count();
    if count <= MAX_CHARS {
        compact
    } else {
        let trimmed = compact.chars().take(MAX_CHARS).collect::<String>();
        format!("{trimmed}...")
    }
}
