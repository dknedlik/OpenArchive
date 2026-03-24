use sha2::{Digest, Sha256};

use crate::error::{ParserError, ParserResult};

pub const TEXT_NORMALIZATION_VERSION: &str = "text-v1";
pub const TEXT_CONTENT_HASH_VERSION: &str = "text-v1";
pub const TEXT_CONTENT_FACETS: &[&str] = &["blocks", "text"];

pub const MARKDOWN_NORMALIZATION_VERSION: &str = "markdown-v1";
pub const MARKDOWN_CONTENT_HASH_VERSION: &str = "markdown-v1";
pub const MARKDOWN_CONTENT_FACETS: &[&str] = &["blocks", "text", "headings", "lists", "code"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentFormat {
    PlainText,
    Markdown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedDocument {
    pub title: Option<String>,
    pub blocks: Vec<ParsedDocumentBlock>,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedDocumentBlock {
    pub block_kind: DocumentBlockKind,
    pub text_content: String,
    pub locator_json: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentBlockKind {
    Paragraph,
    Heading,
    List,
    Code,
}

impl DocumentBlockKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Paragraph => "paragraph",
            Self::Heading => "heading",
            Self::List => "list",
            Self::Code => "code",
        }
    }
}

pub fn parse_document(input: &[u8], format: DocumentFormat) -> ParserResult<ParsedDocument> {
    let raw_text = std::str::from_utf8(input)
        .map_err(|err| ParserError::InvalidJson {
            detail: format!("document input is not valid UTF-8: {err}"),
        })?
        .replace("\r\n", "\n")
        .replace('\r', "\n");

    let blocks = match format {
        DocumentFormat::PlainText => parse_plain_text_blocks(&raw_text),
        DocumentFormat::Markdown => parse_markdown_blocks(&raw_text),
    };

    if blocks.is_empty() {
        return Err(ParserError::EmptyDocument);
    }

    let title = match format {
        DocumentFormat::Markdown => blocks
            .iter()
            .find(|block| block.block_kind == DocumentBlockKind::Heading)
            .map(|block| block.text_content.clone())
            .or_else(|| infer_title_from_blocks(&blocks)),
        DocumentFormat::PlainText => infer_title_from_blocks(&blocks),
    };

    Ok(ParsedDocument {
        content_hash: compute_content_hash(&blocks),
        title,
        blocks,
    })
}

fn parse_plain_text_blocks(raw_text: &str) -> Vec<ParsedDocumentBlock> {
    raw_text
        .split("\n\n")
        .map(normalize_block_text)
        .filter(|text| !text.is_empty())
        .map(|text_content| ParsedDocumentBlock {
            block_kind: DocumentBlockKind::Paragraph,
            text_content,
            locator_json: None,
        })
        .collect()
}

fn parse_markdown_blocks(raw_text: &str) -> Vec<ParsedDocumentBlock> {
    let mut blocks = Vec::new();
    let mut current_heading_path: Vec<String> = Vec::new();
    let mut paragraph_lines = Vec::new();
    let mut list_lines = Vec::new();
    let mut code_lines = Vec::new();
    let mut in_code_fence = false;

    for line in raw_text.lines() {
        let trimmed = line.trim();

        if trimmed.starts_with("```") {
            if in_code_fence {
                push_block(
                    &mut blocks,
                    DocumentBlockKind::Code,
                    &mut code_lines,
                    Some(locator_json(
                        DocumentBlockKind::Code,
                        None,
                        &current_heading_path,
                    )),
                );
                in_code_fence = false;
            } else {
                push_block(
                    &mut blocks,
                    DocumentBlockKind::Paragraph,
                    &mut paragraph_lines,
                    Some(locator_json(
                        DocumentBlockKind::Paragraph,
                        None,
                        &current_heading_path,
                    )),
                );
                push_block(
                    &mut blocks,
                    DocumentBlockKind::List,
                    &mut list_lines,
                    Some(locator_json(
                        DocumentBlockKind::List,
                        None,
                        &current_heading_path,
                    )),
                );
                in_code_fence = true;
            }
            continue;
        }

        if in_code_fence {
            code_lines.push(line.to_string());
            continue;
        }

        if trimmed.is_empty() {
            push_block(
                &mut blocks,
                DocumentBlockKind::Paragraph,
                &mut paragraph_lines,
                Some(locator_json(
                    DocumentBlockKind::Paragraph,
                    None,
                    &current_heading_path,
                )),
            );
            push_block(
                &mut blocks,
                DocumentBlockKind::List,
                &mut list_lines,
                Some(locator_json(
                    DocumentBlockKind::List,
                    None,
                    &current_heading_path,
                )),
            );
            continue;
        }

        if let Some((level, heading_text)) = parse_markdown_heading(trimmed) {
            push_block(
                &mut blocks,
                DocumentBlockKind::Paragraph,
                &mut paragraph_lines,
                Some(locator_json(
                    DocumentBlockKind::Paragraph,
                    None,
                    &current_heading_path,
                )),
            );
            push_block(
                &mut blocks,
                DocumentBlockKind::List,
                &mut list_lines,
                Some(locator_json(
                    DocumentBlockKind::List,
                    None,
                    &current_heading_path,
                )),
            );
            if current_heading_path.len() >= level {
                current_heading_path.truncate(level.saturating_sub(1));
            }
            current_heading_path.push(heading_text.to_string());
            blocks.push(ParsedDocumentBlock {
                block_kind: DocumentBlockKind::Heading,
                text_content: heading_text.to_string(),
                locator_json: Some(locator_json(
                    DocumentBlockKind::Heading,
                    Some(level),
                    &current_heading_path,
                )),
            });
            continue;
        }

        if is_markdown_list_item(trimmed) {
            push_block(
                &mut blocks,
                DocumentBlockKind::Paragraph,
                &mut paragraph_lines,
                Some(locator_json(
                    DocumentBlockKind::Paragraph,
                    None,
                    &current_heading_path,
                )),
            );
            list_lines.push(trimmed.to_string());
            continue;
        }

        push_block(
            &mut blocks,
            DocumentBlockKind::List,
            &mut list_lines,
            Some(locator_json(
                DocumentBlockKind::List,
                None,
                &current_heading_path,
            )),
        );
        paragraph_lines.push(line.to_string());
    }

    if in_code_fence {
        push_block(
            &mut blocks,
            DocumentBlockKind::Code,
            &mut code_lines,
            Some(locator_json(
                DocumentBlockKind::Code,
                None,
                &current_heading_path,
            )),
        );
    }
    push_block(
        &mut blocks,
        DocumentBlockKind::Paragraph,
        &mut paragraph_lines,
        Some(locator_json(
            DocumentBlockKind::Paragraph,
            None,
            &current_heading_path,
        )),
    );
    push_block(
        &mut blocks,
        DocumentBlockKind::List,
        &mut list_lines,
        Some(locator_json(
            DocumentBlockKind::List,
            None,
            &current_heading_path,
        )),
    );

    blocks
}

fn parse_markdown_heading(line: &str) -> Option<(usize, &str)> {
    let hashes = line.chars().take_while(|&ch| ch == '#').count();
    if hashes == 0 || hashes > 6 {
        return None;
    }
    let rest = line[hashes..].trim_start();
    if rest.is_empty() {
        return None;
    }
    Some((hashes, rest))
}

fn is_markdown_list_item(line: &str) -> bool {
    line.starts_with("- ")
        || line.starts_with("* ")
        || line.starts_with("+ ")
        || line
            .split_once(". ")
            .is_some_and(|(prefix, _)| prefix.chars().all(|ch| ch.is_ascii_digit()))
}

fn push_block(
    blocks: &mut Vec<ParsedDocumentBlock>,
    block_kind: DocumentBlockKind,
    lines: &mut Vec<String>,
    locator_json: Option<String>,
) {
    if lines.is_empty() {
        return;
    }
    let text_content = normalize_block_text(&lines.join("\n"));
    lines.clear();
    if text_content.is_empty() {
        return;
    }
    blocks.push(ParsedDocumentBlock {
        block_kind,
        text_content,
        locator_json,
    });
}

fn normalize_block_text(block: &str) -> String {
    block
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string()
}

fn locator_json(
    block_kind: DocumentBlockKind,
    heading_level: Option<usize>,
    heading_path: &[String],
) -> String {
    serde_json::json!({
        "block_kind": block_kind.as_str(),
        "heading_level": heading_level,
        "heading_path": heading_path,
    })
    .to_string()
}

fn infer_title_from_blocks(blocks: &[ParsedDocumentBlock]) -> Option<String> {
    blocks
        .iter()
        .map(|block| block.text_content.trim())
        .find(|text| !text.is_empty())
        .map(truncate_title)
}

fn truncate_title(text: &str) -> String {
    const MAX_TITLE_CHARS: usize = 80;
    let mut truncated = text.chars().take(MAX_TITLE_CHARS).collect::<String>();
    if text.chars().count() > MAX_TITLE_CHARS {
        truncated.push_str("...");
    }
    truncated
}

fn compute_content_hash(blocks: &[ParsedDocumentBlock]) -> String {
    let mut hasher = Sha256::new();
    for block in blocks {
        hasher.update(block.block_kind.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(block.text_content.as_bytes());
        hasher.update([0]);
    }
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_text_parser_splits_paragraphs() {
        let parsed = parse_document(b"One.\n\nTwo.", DocumentFormat::PlainText).unwrap();
        assert_eq!(parsed.blocks.len(), 2);
        assert_eq!(parsed.blocks[0].block_kind, DocumentBlockKind::Paragraph);
        assert_eq!(parsed.title.as_deref(), Some("One."));
    }

    #[test]
    fn markdown_parser_preserves_headings_and_lists() {
        let parsed = parse_document(
            b"# Title\n\nParagraph text.\n\n- one\n- two\n",
            DocumentFormat::Markdown,
        )
        .unwrap();
        assert_eq!(parsed.blocks.len(), 3);
        assert_eq!(parsed.blocks[0].block_kind, DocumentBlockKind::Heading);
        assert_eq!(parsed.blocks[1].block_kind, DocumentBlockKind::Paragraph);
        assert_eq!(parsed.blocks[2].block_kind, DocumentBlockKind::List);
        assert_eq!(parsed.title.as_deref(), Some("Title"));
    }

    #[test]
    fn empty_document_is_rejected() {
        let err = parse_document(b" \n \n", DocumentFormat::PlainText).unwrap_err();
        assert!(matches!(err, ParserError::EmptyDocument));
    }
}
