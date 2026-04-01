use oracle::Connection;

use crate::error::{StorageError, StorageResult};
use crate::storage::types::{
    ImportedNoteAliasRecord, ImportedNoteLinkKind, ImportedNoteLinkRecord,
    ImportedNoteLinkResolutionStatus, ImportedNoteLinkTargetKind, ImportedNoteMetadata,
    ImportedNotePropertyRecord, ImportedNotePropertyValueKind, ImportedNoteTagRecord,
    ImportedNoteTagSourceKind, NewImportedNoteAlias, NewImportedNoteLink, NewImportedNoteProperty,
    NewImportedNoteTag,
};

pub fn insert_imported_note_property(
    conn: &Connection,
    property: &NewImportedNoteProperty,
) -> StorageResult<()> {
    conn.execute(
        "INSERT INTO oa_artifact_note_property \
         (artifact_note_property_id, artifact_id, property_key, value_kind, value_text, value_json, sequence_no) \
         VALUES (:1, :2, :3, :4, :5, :6, :7)",
        &[
            &property.imported_note_property_id,
            &property.artifact_id,
            &property.property_key,
            &property.value_kind.as_str(),
            &property.value_text,
            &property.value_json,
            &property.sequence_no,
        ],
    )
    .map_err(|source| StorageError::InsertImportedNoteProperty {
        artifact_id: property.artifact_id.clone(),
        property_key: property.property_key.clone(),
        source: Box::new(source),
    })?;
    Ok(())
}

pub fn insert_imported_note_tag(conn: &Connection, tag: &NewImportedNoteTag) -> StorageResult<()> {
    conn.execute(
        "INSERT INTO oa_artifact_note_tag \
         (artifact_note_tag_id, artifact_id, raw_tag, normalized_tag, tag_path, source_kind, sequence_no) \
         VALUES (:1, :2, :3, :4, :5, :6, :7)",
        &[
            &tag.imported_note_tag_id,
            &tag.artifact_id,
            &tag.raw_tag,
            &tag.normalized_tag,
            &tag.tag_path,
            &tag.source_kind.as_str(),
            &tag.sequence_no,
        ],
    )
    .map_err(|source| StorageError::InsertImportedNoteTag {
        artifact_id: tag.artifact_id.clone(),
        tag_value: tag.normalized_tag.clone(),
        source: Box::new(source),
    })?;
    Ok(())
}

pub fn insert_imported_note_alias(
    conn: &Connection,
    alias: &NewImportedNoteAlias,
) -> StorageResult<()> {
    conn.execute(
        "INSERT INTO oa_artifact_note_alias \
         (artifact_note_alias_id, artifact_id, alias_text, normalized_alias, sequence_no) \
         VALUES (:1, :2, :3, :4, :5)",
        &[
            &alias.imported_note_alias_id,
            &alias.artifact_id,
            &alias.alias_text,
            &alias.normalized_alias,
            &alias.sequence_no,
        ],
    )
    .map_err(|source| StorageError::InsertImportedNoteAlias {
        artifact_id: alias.artifact_id.clone(),
        alias_text: alias.alias_text.clone(),
        source: Box::new(source),
    })?;
    Ok(())
}

pub fn insert_imported_note_link(
    conn: &Connection,
    link: &NewImportedNoteLink,
) -> StorageResult<()> {
    conn.execute(
        "INSERT INTO oa_artifact_note_link \
         (artifact_note_link_id, artifact_id, source_segment_id, link_kind, target_kind, raw_target, \
          normalized_target, display_text, target_path, target_heading, target_block, external_url, \
          resolved_artifact_id, resolution_status, locator_json, sequence_no) \
         VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16)",
        &[
            &link.imported_note_link_id,
            &link.artifact_id,
            &link.source_segment_id,
            &link.link_kind.as_str(),
            &link.target_kind.as_str(),
            &link.raw_target,
            &link.normalized_target,
            &link.display_text,
            &link.target_path,
            &link.target_heading,
            &link.target_block,
            &link.external_url,
            &link.resolved_artifact_id,
            &link.resolution_status.as_str(),
            &link.locator_json,
            &link.sequence_no,
        ],
    )
    .map_err(|source| StorageError::InsertImportedNoteLink {
        artifact_id: link.artifact_id.clone(),
        link_id: link.imported_note_link_id.clone(),
        source: Box::new(source),
    })?;
    Ok(())
}

pub fn load_imported_note_metadata(
    conn: &Connection,
    artifact_id: &str,
    note_path: Option<String>,
) -> StorageResult<ImportedNoteMetadata> {
    let property_rows = conn
        .query(
            "SELECT property_key, value_kind, value_text, value_json \
             FROM oa_artifact_note_property \
             WHERE artifact_id = :1 \
             ORDER BY sequence_no ASC, artifact_note_property_id ASC",
            &[&artifact_id],
        )
        .map_err(|source| StorageError::ListArtifacts {
            source: Box::new(source),
        })?;
    let mut properties = Vec::new();
    for row_result in property_rows {
        let row = row_result.map_err(|source| StorageError::ListArtifacts {
            source: Box::new(source),
        })?;
        let value_kind: String = row.get(1).map_err(|source| StorageError::ListArtifacts {
            source: Box::new(source),
        })?;
        let value_json_text: String = row.get(3).map_err(|source| StorageError::ListArtifacts {
            source: Box::new(source),
        })?;
        properties.push(ImportedNotePropertyRecord {
            property_key: row.get(0).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?,
            value_kind: ImportedNotePropertyValueKind::parse(&value_kind).ok_or_else(|| {
                StorageError::InvalidDerivationWrite {
                    detail: format!("unknown imported note property kind {value_kind}"),
                }
            })?,
            value_text: row.get(2).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?,
            value_json: serde_json::from_str(&value_json_text).map_err(|err| {
                StorageError::InvalidDerivationWrite {
                    detail: format!("invalid imported note property JSON for {artifact_id}: {err}"),
                }
            })?,
        });
    }

    let tag_rows = conn
        .query(
            "SELECT raw_tag, normalized_tag, tag_path, source_kind \
             FROM oa_artifact_note_tag \
             WHERE artifact_id = :1 \
             ORDER BY sequence_no ASC, artifact_note_tag_id ASC",
            &[&artifact_id],
        )
        .map_err(|source| StorageError::ListArtifacts {
            source: Box::new(source),
        })?;
    let mut tags = Vec::new();
    for row_result in tag_rows {
        let row = row_result.map_err(|source| StorageError::ListArtifacts {
            source: Box::new(source),
        })?;
        let source_kind: String = row.get(3).map_err(|source| StorageError::ListArtifacts {
            source: Box::new(source),
        })?;
        tags.push(ImportedNoteTagRecord {
            raw_tag: row.get(0).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?,
            normalized_tag: row.get(1).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?,
            tag_path: row.get(2).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?,
            source_kind: ImportedNoteTagSourceKind::parse(&source_kind).ok_or_else(|| {
                StorageError::InvalidDerivationWrite {
                    detail: format!("unknown imported note tag source {source_kind}"),
                }
            })?,
        });
    }

    let alias_rows = conn
        .query(
            "SELECT alias_text, normalized_alias \
             FROM oa_artifact_note_alias \
             WHERE artifact_id = :1 \
             ORDER BY sequence_no ASC, artifact_note_alias_id ASC",
            &[&artifact_id],
        )
        .map_err(|source| StorageError::ListArtifacts {
            source: Box::new(source),
        })?;
    let mut aliases = Vec::new();
    for row_result in alias_rows {
        let row = row_result.map_err(|source| StorageError::ListArtifacts {
            source: Box::new(source),
        })?;
        aliases.push(ImportedNoteAliasRecord {
            alias_text: row.get(0).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?,
            normalized_alias: row.get(1).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?,
        });
    }

    Ok(ImportedNoteMetadata {
        note_path,
        properties,
        tags,
        aliases,
        outbound_links: load_outbound_note_links(conn, artifact_id)?,
    })
}

pub fn load_outbound_note_links(
    conn: &Connection,
    artifact_id: &str,
) -> StorageResult<Vec<ImportedNoteLinkRecord>> {
    load_note_links(
        conn,
        "SELECT artifact_note_link_id, source_segment_id, link_kind, target_kind, raw_target, \
                normalized_target, display_text, target_path, target_heading, target_block, external_url, \
                resolved_artifact_id, resolution_status, locator_json \
         FROM oa_artifact_note_link \
         WHERE artifact_id = :1 \
         ORDER BY sequence_no ASC, artifact_note_link_id ASC",
        &[&artifact_id],
    )
}

pub fn load_inbound_note_links(
    conn: &Connection,
    artifact_id: &str,
) -> StorageResult<Vec<ImportedNoteLinkRecord>> {
    load_note_links(
        conn,
        "SELECT artifact_note_link_id, source_segment_id, link_kind, target_kind, raw_target, \
                normalized_target, display_text, target_path, target_heading, target_block, external_url, \
                resolved_artifact_id, resolution_status, locator_json \
         FROM oa_artifact_note_link \
         WHERE resolved_artifact_id = :1 \
         ORDER BY sequence_no ASC, artifact_note_link_id ASC",
        &[&artifact_id],
    )
}

fn load_note_links(
    conn: &Connection,
    sql: &str,
    params: &[&dyn oracle::sql_type::ToSql],
) -> StorageResult<Vec<ImportedNoteLinkRecord>> {
    let rows = conn
        .query(sql, params)
        .map_err(|source| StorageError::ListArtifacts {
            source: Box::new(source),
        })?;
    let mut links = Vec::new();
    for row_result in rows {
        let row = row_result.map_err(|source| StorageError::ListArtifacts {
            source: Box::new(source),
        })?;
        let link_kind: String = row.get(2).map_err(|source| StorageError::ListArtifacts {
            source: Box::new(source),
        })?;
        let target_kind: String = row.get(3).map_err(|source| StorageError::ListArtifacts {
            source: Box::new(source),
        })?;
        let resolution_status: String =
            row.get(12).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?;
        links.push(ImportedNoteLinkRecord {
            imported_note_link_id: row.get(0).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?,
            source_segment_id: row.get(1).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?,
            link_kind: ImportedNoteLinkKind::parse(&link_kind).ok_or_else(|| {
                StorageError::InvalidDerivationWrite {
                    detail: format!("unknown imported note link kind {link_kind}"),
                }
            })?,
            target_kind: ImportedNoteLinkTargetKind::parse(&target_kind).ok_or_else(|| {
                StorageError::InvalidDerivationWrite {
                    detail: format!("unknown imported note link target kind {target_kind}"),
                }
            })?,
            raw_target: row.get(4).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?,
            normalized_target: row.get(5).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?,
            display_text: row.get(6).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?,
            target_path: row.get(7).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?,
            target_heading: row.get(8).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?,
            target_block: row.get(9).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?,
            external_url: row.get(10).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?,
            resolved_artifact_id: row.get(11).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?,
            resolution_status: ImportedNoteLinkResolutionStatus::parse(&resolution_status)
                .ok_or_else(|| StorageError::InvalidDerivationWrite {
                    detail: format!(
                        "unknown imported note link resolution status {resolution_status}"
                    ),
                })?,
            locator_json: row.get(13).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?,
        });
    }

    Ok(links)
}
