use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use open_archive::parser::{self, ParsedConversation};

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
}

fn main() -> Result<()> {
    let args = Args::parse();
    let bytes = fs::read(&args.path)
        .with_context(|| format!("failed to read export file {}", args.path.display()))?;
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
    };
    parsed.map_err(anyhow::Error::from)
}

fn source_label(source: ImportSource) -> &'static str {
    match source {
        ImportSource::Chatgpt => "chatgpt",
        ImportSource::Claude => "claude",
        ImportSource::Grok => "grok",
        ImportSource::Gemini => "gemini",
    }
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
