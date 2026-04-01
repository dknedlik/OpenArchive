use postgres::Client;

use crate::error::{DbError, StorageError, StorageResult};
use crate::storage::types::{
    ImportedNoteAliasRecord, ImportedNoteLinkKind, ImportedNoteLinkRecord,
    ImportedNoteLinkResolutionStatus, ImportedNoteLinkTargetKind, ImportedNoteMetadata,
    ImportedNotePropertyRecord, ImportedNotePropertyValueKind, ImportedNoteTagRecord,
    ImportedNoteTagSourceKind, NewImportedNoteAlias, NewImportedNoteLink, NewImportedNoteProperty,
    NewImportedNoteTag,
};

pub fn insert_imported_note_property(
    client: &mut Client,
    property: &NewImportedNoteProperty,
) -> StorageResult<()> {
    client
        .execute(
            "INSERT INTO oa_artifact_note_property \
             (artifact_note_property_id, artifact_id, property_key, value_kind, value_text, value_json, sequence_no) \
             VALUES ($1, $2, $3, $4, $5, $6::text::jsonb, $7)",
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

pub fn insert_imported_note_tag(
    client: &mut Client,
    tag: &NewImportedNoteTag,
) -> StorageResult<()> {
    client
        .execute(
            "INSERT INTO oa_artifact_note_tag \
             (artifact_note_tag_id, artifact_id, raw_tag, normalized_tag, tag_path, source_kind, sequence_no) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
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
    client: &mut Client,
    alias: &NewImportedNoteAlias,
) -> StorageResult<()> {
    client
        .execute(
            "INSERT INTO oa_artifact_note_alias \
             (artifact_note_alias_id, artifact_id, alias_text, normalized_alias, sequence_no) \
             VALUES ($1, $2, $3, $4, $5)",
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
    client: &mut Client,
    link: &NewImportedNoteLink,
) -> StorageResult<()> {
    client
        .execute(
            "INSERT INTO oa_artifact_note_link \
             (artifact_note_link_id, artifact_id, source_segment_id, link_kind, target_kind, raw_target, \
              normalized_target, display_text, target_path, target_heading, target_block, external_url, \
              resolved_artifact_id, resolution_status, locator_json, sequence_no) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15::text::jsonb, $16)",
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
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
    note_path: Option<String>,
) -> StorageResult<ImportedNoteMetadata> {
    let property_rows = client
        .query(
            "SELECT property_key, value_kind, value_text, value_json::text
             FROM oa_artifact_note_property
             WHERE artifact_id = $1
             ORDER BY sequence_no ASC, artifact_note_property_id ASC",
            &[&artifact_id],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;
    let mut properties = Vec::with_capacity(property_rows.len());
    for row in property_rows {
        let property_key: String = row.get(0);
        let value_kind: String = row.get(1);
        let value_json_text: String = row.get(3);
        properties.push(ImportedNotePropertyRecord {
            property_key,
            value_kind: ImportedNotePropertyValueKind::parse(&value_kind).ok_or_else(|| {
                StorageError::InvalidDerivationWrite {
                    detail: format!("unknown imported note property kind {value_kind}"),
                }
            })?,
            value_text: row.get(2),
            value_json: serde_json::from_str(&value_json_text).map_err(|err| {
                StorageError::InvalidDerivationWrite {
                    detail: format!("invalid imported note property JSON for {artifact_id}: {err}"),
                }
            })?,
        });
    }

    let tag_rows = client
        .query(
            "SELECT raw_tag, normalized_tag, tag_path, source_kind
             FROM oa_artifact_note_tag
             WHERE artifact_id = $1
             ORDER BY sequence_no ASC, artifact_note_tag_id ASC",
            &[&artifact_id],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;
    let mut tags = Vec::with_capacity(tag_rows.len());
    for row in tag_rows {
        let source_kind: String = row.get(3);
        tags.push(ImportedNoteTagRecord {
            raw_tag: row.get(0),
            normalized_tag: row.get(1),
            tag_path: row.get(2),
            source_kind: ImportedNoteTagSourceKind::parse(&source_kind).ok_or_else(|| {
                StorageError::InvalidDerivationWrite {
                    detail: format!("unknown imported note tag source {source_kind}"),
                }
            })?,
        });
    }

    let alias_rows = client
        .query(
            "SELECT alias_text, normalized_alias
             FROM oa_artifact_note_alias
             WHERE artifact_id = $1
             ORDER BY sequence_no ASC, artifact_note_alias_id ASC",
            &[&artifact_id],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;
    let mut aliases = Vec::with_capacity(alias_rows.len());
    for row in alias_rows {
        aliases.push(ImportedNoteAliasRecord {
            alias_text: row.get(0),
            normalized_alias: row.get(1),
        });
    }

    Ok(ImportedNoteMetadata {
        note_path,
        properties,
        tags,
        aliases,
        outbound_links: load_outbound_note_links(client, connection_string, artifact_id)?,
    })
}

pub fn load_outbound_note_links(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
) -> StorageResult<Vec<ImportedNoteLinkRecord>> {
    load_note_links(
        client,
        connection_string,
        "WHERE artifact_id = $1",
        &[&artifact_id],
    )
}

pub fn load_inbound_note_links(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
) -> StorageResult<Vec<ImportedNoteLinkRecord>> {
    load_note_links(
        client,
        connection_string,
        "WHERE resolved_artifact_id = $1",
        &[&artifact_id],
    )
}

fn load_note_links(
    client: &mut Client,
    connection_string: &str,
    where_sql: &str,
    params: &[&(dyn postgres::types::ToSql + Sync)],
) -> StorageResult<Vec<ImportedNoteLinkRecord>> {
    let sql = format!(
        "SELECT artifact_note_link_id, source_segment_id, link_kind, target_kind, raw_target, \
                normalized_target, display_text, target_path, target_heading, target_block, external_url, \
                resolved_artifact_id, resolution_status, locator_json::text \
         FROM oa_artifact_note_link
         {where_sql}
         ORDER BY sequence_no ASC, artifact_note_link_id ASC"
    );
    let rows = client
        .query(&sql, params)
        .map_err(|source| map_pg_err(connection_string, source))?;
    let mut links = Vec::with_capacity(rows.len());
    for row in rows {
        let link_kind: String = row.get(2);
        let target_kind: String = row.get(3);
        let resolution_status: String = row.get(12);
        links.push(ImportedNoteLinkRecord {
            imported_note_link_id: row.get(0),
            source_segment_id: row.get(1),
            link_kind: ImportedNoteLinkKind::parse(&link_kind).ok_or_else(|| {
                StorageError::InvalidDerivationWrite {
                    detail: format!("unknown imported note link kind {link_kind}"),
                }
            })?,
            target_kind: ImportedNoteLinkTargetKind::parse(&target_kind).ok_or_else(|| {
                StorageError::InvalidDerivationWrite {
                    detail: format!("unknown imported note target kind {target_kind}"),
                }
            })?,
            raw_target: row.get(4),
            normalized_target: row.get(5),
            display_text: row.get(6),
            target_path: row.get(7),
            target_heading: row.get(8),
            target_block: row.get(9),
            external_url: row.get(10),
            resolved_artifact_id: row.get(11),
            resolution_status: ImportedNoteLinkResolutionStatus::parse(&resolution_status)
                .ok_or_else(|| StorageError::InvalidDerivationWrite {
                    detail: format!(
                        "unknown imported note link resolution status {resolution_status}"
                    ),
                })?,
            locator_json: row.get(13),
        });
    }
    Ok(links)
}

fn map_pg_err(connection_string: &str, source: postgres::Error) -> StorageError {
    StorageError::Db(DbError::ConnectPostgres {
        connection_string: connection_string.to_string(),
        source: Box::new(source),
    })
}
