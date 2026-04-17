use std::io::{Cursor, Read};

use crate::error::{OpenArchiveError, ParserError};

#[derive(Debug)]
pub(super) enum ChatGptPayloadShape<'a> {
    RawJson {
        bytes: &'a [u8],
    },
    Zip {
        zip_bytes: &'a [u8],
        conversations_json: Vec<u8>,
    },
}

pub fn looks_like_chatgpt_zip(bytes: &[u8]) -> bool {
    if bytes.len() < 4 || bytes[0..4] != *b"PK\x03\x04" {
        return false;
    }

    let reader = Cursor::new(bytes);
    let Ok(mut archive) = zip::ZipArchive::new(reader) else {
        return false;
    };

    for index in 0..archive.len() {
        let Ok(file) = archive.by_index(index) else {
            continue;
        };
        let name = file.name();
        if name.eq_ignore_ascii_case("conversations.json") {
            return true;
        }
        if std::path::Path::new(name)
            .file_name()
            .map(|n| n.eq_ignore_ascii_case("conversations.json"))
            .unwrap_or(false)
        {
            return true;
        }
    }

    false
}

pub(super) fn sniff_chatgpt_payload(
    bytes: &[u8],
) -> Result<ChatGptPayloadShape<'_>, OpenArchiveError> {
    if bytes.len() < 4 || bytes[0..4] != *b"PK\x03\x04" {
        return Ok(ChatGptPayloadShape::RawJson { bytes });
    }

    let reader = Cursor::new(bytes);
    let mut archive =
        zip::ZipArchive::new(reader).map_err(|e| ParserError::UnsupportedPayload {
            detail: format!("corrupt zip archive: {e}"),
        })?;

    for index in 0..archive.len() {
        let mut file = archive
            .by_index(index)
            .map_err(|e| ParserError::UnsupportedPayload {
                detail: format!("failed to read zip entry {index}: {e}"),
            })?;
        if file.name().eq_ignore_ascii_case("conversations.json") {
            let mut conversations_json = Vec::new();
            file.read_to_end(&mut conversations_json).map_err(|e| {
                ParserError::UnsupportedPayload {
                    detail: format!("failed to read conversations.json from zip: {e}"),
                }
            })?;
            return Ok(ChatGptPayloadShape::Zip {
                zip_bytes: bytes,
                conversations_json,
            });
        }
    }

    for index in 0..archive.len() {
        let mut file = archive
            .by_index(index)
            .map_err(|e| ParserError::UnsupportedPayload {
                detail: format!("failed to read zip entry {index}: {e}"),
            })?;
        let basename_matches = std::path::Path::new(file.name())
            .file_name()
            .map(|name| name.eq_ignore_ascii_case("conversations.json"))
            .unwrap_or(false);
        if basename_matches {
            let mut conversations_json = Vec::new();
            file.read_to_end(&mut conversations_json).map_err(|e| {
                ParserError::UnsupportedPayload {
                    detail: format!("failed to read conversations.json from zip: {e}"),
                }
            })?;
            return Ok(ChatGptPayloadShape::Zip {
                zip_bytes: bytes,
                conversations_json,
            });
        }
    }

    Err(ParserError::UnsupportedPayload {
        detail: "chatgpt zip missing conversations.json".to_string(),
    }
    .into())
}
