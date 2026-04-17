use rusqlite::{params, Connection, OptionalExtension, ToSql};

use crate::error::StorageError;
use crate::storage::types::{
    ArtifactLinkRecord, ArtifactLinkType, ImportedNoteAliasRecord, ImportedNoteLinkRecord,
    ImportedNoteMetadata, ImportedNotePropertyRecord, ImportedNoteTagRecord, NewImportedNoteAlias,
    NewImportedNoteLink, NewImportedNoteProperty, NewImportedNoteTag,
};
use crate::storage::writeback_store::NewArchiveLink;

use super::{
    next_id, parse_artifact_link_type, parse_imported_note_link_kind,
    parse_imported_note_property_kind, parse_imported_note_resolution_status,
    parse_imported_note_tag_source, parse_imported_note_target_kind, StorageResult,
};

pub(super) fn insert_imported_note_property(
    tx: &rusqlite::Transaction<'_>,
    property: &NewImportedNoteProperty,
) -> StorageResult<()> {
    tx.execute(
        "INSERT INTO oa_artifact_note_property
         (artifact_note_property_id, artifact_id, property_key, value_kind, value_text, value_json, sequence_no)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            property.imported_note_property_id,
            property.artifact_id,
            property.property_key,
            property.value_kind.as_str(),
            property.value_text,
            property.value_json,
            property.sequence_no
        ],
    )
    .map_err(|source| StorageError::InsertImportedNoteProperty {
        artifact_id: property.artifact_id.clone(),
        property_key: property.property_key.clone(),
        source: Box::new(source),
    })?;
    Ok(())
}

pub(super) fn insert_imported_note_tag(
    tx: &rusqlite::Transaction<'_>,
    tag: &NewImportedNoteTag,
) -> StorageResult<()> {
    tx.execute(
        "INSERT INTO oa_artifact_note_tag
         (artifact_note_tag_id, artifact_id, raw_tag, normalized_tag, tag_path, source_kind, sequence_no)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            tag.imported_note_tag_id,
            tag.artifact_id,
            tag.raw_tag,
            tag.normalized_tag,
            tag.tag_path,
            tag.source_kind.as_str(),
            tag.sequence_no
        ],
    )
    .map_err(|source| StorageError::InsertImportedNoteTag {
        artifact_id: tag.artifact_id.clone(),
        tag_value: tag.normalized_tag.clone(),
        source: Box::new(source),
    })?;
    Ok(())
}

pub(super) fn insert_imported_note_alias(
    tx: &rusqlite::Transaction<'_>,
    alias: &NewImportedNoteAlias,
) -> StorageResult<()> {
    tx.execute(
        "INSERT INTO oa_artifact_note_alias
         (artifact_note_alias_id, artifact_id, alias_text, normalized_alias, sequence_no)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            alias.imported_note_alias_id,
            alias.artifact_id,
            alias.alias_text,
            alias.normalized_alias,
            alias.sequence_no
        ],
    )
    .map_err(|source| StorageError::InsertImportedNoteAlias {
        artifact_id: alias.artifact_id.clone(),
        alias_text: alias.alias_text.clone(),
        source: Box::new(source),
    })?;
    Ok(())
}

pub(super) fn insert_imported_note_link(
    tx: &rusqlite::Transaction<'_>,
    link: &NewImportedNoteLink,
) -> StorageResult<()> {
    tx.execute(
        "INSERT INTO oa_artifact_note_link
         (artifact_note_link_id, artifact_id, source_segment_id, link_kind, target_kind, raw_target,
          normalized_target, display_text, target_path, target_heading, target_block, external_url,
          resolved_artifact_id, resolution_status, locator_json, sequence_no)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
        params![
            link.imported_note_link_id,
            link.artifact_id,
            link.source_segment_id,
            link.link_kind.as_str(),
            link.target_kind.as_str(),
            link.raw_target,
            link.normalized_target,
            link.display_text,
            link.target_path,
            link.target_heading,
            link.target_block,
            link.external_url,
            link.resolved_artifact_id,
            link.resolution_status.as_str(),
            link.locator_json,
            link.sequence_no
        ],
    )
    .map_err(|source| StorageError::InsertImportedNoteLink {
        artifact_id: link.artifact_id.clone(),
        link_id: link.imported_note_link_id.clone(),
        source: Box::new(source),
    })?;
    Ok(())
}

pub(super) fn load_note_links(
    connection: &Connection,
    sql: &str,
    params: &[&dyn ToSql],
) -> StorageResult<Vec<ImportedNoteLinkRecord>> {
    let mut stmt =
        connection
            .prepare(sql)
            .map_err(|source| StorageError::InsertImportedNoteLink {
                artifact_id: "sqlite".to_string(),
                link_id: "prepare".to_string(),
                source: Box::new(source),
            })?;
    let rows = stmt
        .query_map(params, |row| {
            Ok(ImportedNoteLinkRecord {
                imported_note_link_id: row.get(0)?,
                source_segment_id: row.get(1)?,
                link_kind: parse_imported_note_link_kind(row.get(2)?)?,
                target_kind: parse_imported_note_target_kind(row.get(3)?)?,
                raw_target: row.get(4)?,
                normalized_target: row.get(5)?,
                display_text: row.get(6)?,
                target_path: row.get(7)?,
                target_heading: row.get(8)?,
                target_block: row.get(9)?,
                external_url: row.get(10)?,
                resolved_artifact_id: row.get(11)?,
                resolution_status: parse_imported_note_resolution_status(row.get(12)?)?,
                locator_json: row.get(13)?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load imported note links: {source}"),
        })?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map imported note links: {source}"),
        })
}

pub(super) fn load_imported_note_metadata(
    connection: &Connection,
    artifact_id: &str,
    note_path: Option<String>,
) -> StorageResult<ImportedNoteMetadata> {
    use rusqlite::types::Type;

    let mut properties_stmt = connection
        .prepare(
            "SELECT property_key, value_kind, value_text, value_json
             FROM oa_artifact_note_property
             WHERE artifact_id = ?1
             ORDER BY sequence_no ASC, artifact_note_property_id ASC",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare imported note properties query: {source}"),
        })?;
    let properties = properties_stmt
        .query_map([artifact_id], |row| {
            Ok(ImportedNotePropertyRecord {
                property_key: row.get(0)?,
                value_kind: parse_imported_note_property_kind(row.get(1)?)?,
                value_text: row.get(2)?,
                value_json: serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(3)?)
                    .map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(3, Type::Text, Box::new(err))
                    })?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load imported note properties: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map imported note properties: {source}"),
        })?;

    let mut tags_stmt = connection
        .prepare(
            "SELECT raw_tag, normalized_tag, tag_path, source_kind
             FROM oa_artifact_note_tag
             WHERE artifact_id = ?1
             ORDER BY sequence_no ASC, artifact_note_tag_id ASC",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare imported note tags query: {source}"),
        })?;
    let tags = tags_stmt
        .query_map([artifact_id], |row| {
            Ok(ImportedNoteTagRecord {
                raw_tag: row.get(0)?,
                normalized_tag: row.get(1)?,
                tag_path: row.get(2)?,
                source_kind: parse_imported_note_tag_source(row.get(3)?)?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load imported note tags: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map imported note tags: {source}"),
        })?;

    let mut aliases_stmt = connection
        .prepare(
            "SELECT alias_text, normalized_alias
             FROM oa_artifact_note_alias
             WHERE artifact_id = ?1
             ORDER BY sequence_no ASC, artifact_note_alias_id ASC",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare imported note aliases query: {source}"),
        })?;
    let aliases = aliases_stmt
        .query_map([artifact_id], |row| {
            Ok(ImportedNoteAliasRecord {
                alias_text: row.get(0)?,
                normalized_alias: row.get(1)?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load imported note aliases: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map imported note aliases: {source}"),
        })?;

    let outbound_links = load_note_links(
        connection,
        "SELECT artifact_note_link_id, source_segment_id, link_kind, target_kind, raw_target,
                normalized_target, display_text, target_path, target_heading, target_block, external_url,
                resolved_artifact_id, resolution_status, locator_json
         FROM oa_artifact_note_link
         WHERE artifact_id = ?1
         ORDER BY sequence_no ASC, artifact_note_link_id ASC",
        &[&artifact_id],
    )?;

    Ok(ImportedNoteMetadata {
        note_path,
        properties,
        tags,
        aliases,
        outbound_links,
    })
}

pub(super) fn load_inbound_note_links(
    connection: &Connection,
    artifact_id: &str,
) -> StorageResult<Vec<ImportedNoteLinkRecord>> {
    load_note_links(
        connection,
        "SELECT artifact_note_link_id, source_segment_id, link_kind, target_kind, raw_target,
                normalized_target, display_text, target_path, target_heading, target_block, external_url,
                resolved_artifact_id, resolution_status, locator_json
         FROM oa_artifact_note_link
         WHERE resolved_artifact_id = ?1
         ORDER BY sequence_no ASC, artifact_note_link_id ASC",
        &[&artifact_id],
    )
}

pub(super) fn insert_artifact_link(
    tx: &rusqlite::Transaction<'_>,
    source_artifact_id: &str,
    target_artifact_id: &str,
    link_type: ArtifactLinkType,
    link_value: &str,
) -> StorageResult<()> {
    tx.execute(
        "INSERT OR IGNORE INTO oa_artifact_link
         (artifact_link_id, source_artifact_id, target_artifact_id, link_type, link_value)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            next_id("artlink"),
            source_artifact_id,
            target_artifact_id,
            link_type.as_str(),
            link_value
        ],
    )
    .map_err(|source| StorageError::InsertArtifactLink {
        source_artifact_id: source_artifact_id.to_string(),
        target_artifact_id: target_artifact_id.to_string(),
        link_type: link_type.as_str().to_string(),
        source: Box::new(source),
    })?;
    Ok(())
}

pub(super) fn sync_structural_links_for_artifact(
    tx: &rusqlite::Transaction<'_>,
    artifact_id: &str,
) -> StorageResult<()> {
    tx.execute(
        "UPDATE oa_artifact_note_link
         SET resolved_artifact_id = (
                SELECT a.artifact_id
                FROM oa_artifact a
                WHERE a.source_type = 'obsidian_vault'
                  AND a.source_conversation_key = oa_artifact_note_link.target_path
                LIMIT 1
             ),
             resolution_status = 'resolved'
         WHERE resolved_artifact_id IS NULL
           AND resolution_status = 'unresolved'
           AND target_kind IN ('note', 'heading', 'block')
           AND artifact_id <> ?1
           AND EXISTS (
                SELECT 1
                FROM oa_artifact a
                WHERE a.source_type = 'obsidian_vault'
                  AND a.source_conversation_key = oa_artifact_note_link.target_path
           )",
        [artifact_id],
    )
    .map_err(|source| StorageError::InvalidDerivationWrite {
        detail: format!("failed to resolve note links by path for {artifact_id}: {source}"),
    })?;

    tx.execute(
        "UPDATE oa_artifact_note_link
         SET resolved_artifact_id = (
                SELECT a.artifact_id
                FROM oa_artifact_note_alias aa
                JOIN oa_artifact a ON a.artifact_id = aa.artifact_id
                WHERE aa.normalized_alias = oa_artifact_note_link.normalized_target
                GROUP BY aa.normalized_alias
                HAVING COUNT(*) = 1
             ),
             resolution_status = 'resolved'
         WHERE resolved_artifact_id IS NULL
           AND resolution_status = 'unresolved'
           AND target_kind = 'note'
           AND artifact_id <> ?1",
        [artifact_id],
    )
    .map_err(|source| StorageError::InvalidDerivationWrite {
        detail: format!("failed to resolve note links by alias for {artifact_id}: {source}"),
    })?;

    let mut wikilinks_stmt = tx
        .prepare(
            "SELECT DISTINCT artifact_id, resolved_artifact_id,
                    COALESCE(target_path, normalized_target, raw_target)
             FROM oa_artifact_note_link
             WHERE resolved_artifact_id IS NOT NULL
               AND artifact_id <> resolved_artifact_id
               AND (artifact_id = ?1 OR resolved_artifact_id = ?1)",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare wikilink sync query: {source}"),
        })?;
    let wikilinks = wikilinks_stmt
        .query_map([artifact_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load wikilink edges: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map wikilink edges: {source}"),
        })?;
    for (source_artifact_id, target_artifact_id, link_value) in wikilinks {
        insert_artifact_link(
            tx,
            &source_artifact_id,
            &target_artifact_id,
            ArtifactLinkType::Wikilink,
            &link_value,
        )?;
    }

    let mut tag_stmt = tx
        .prepare(
            "SELECT DISTINCT t1.artifact_id, t2.artifact_id, t1.normalized_tag
             FROM oa_artifact_note_tag t1
             JOIN oa_artifact_note_tag t2
               ON t2.normalized_tag = t1.normalized_tag
              AND t2.artifact_id <> t1.artifact_id
             WHERE t1.artifact_id = ?1",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare shared-tag sync query: {source}"),
        })?;
    let tag_edges = tag_stmt
        .query_map([artifact_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load shared-tag edges: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map shared-tag edges: {source}"),
        })?;
    for (source_artifact_id, target_artifact_id, link_value) in tag_edges {
        insert_artifact_link(
            tx,
            &source_artifact_id,
            &target_artifact_id,
            ArtifactLinkType::SharedTag,
            &link_value,
        )?;
    }

    let mut alias_stmt = tx
        .prepare(
            "SELECT DISTINCT a1.artifact_id, a2.artifact_id, a1.normalized_alias
             FROM oa_artifact_note_alias a1
             JOIN oa_artifact_note_alias a2
               ON a2.normalized_alias = a1.normalized_alias
              AND a2.artifact_id <> a1.artifact_id
             WHERE a1.artifact_id = ?1",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare alias sync query: {source}"),
        })?;
    let alias_edges = alias_stmt
        .query_map([artifact_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load alias edges: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map alias edges: {source}"),
        })?;
    for (source_artifact_id, target_artifact_id, link_value) in alias_edges {
        insert_artifact_link(
            tx,
            &source_artifact_id,
            &target_artifact_id,
            ArtifactLinkType::Alias,
            &link_value,
        )?;
    }

    Ok(())
}

pub(super) fn sync_reconcile_links_for_archive_links(
    tx: &rusqlite::Transaction<'_>,
    archive_links: &[NewArchiveLink],
) -> StorageResult<()> {
    for archive_link in archive_links {
        let row = tx
            .query_row(
                "SELECT src.artifact_id, tgt.artifact_id, src.derived_object_type,
                        COALESCE(NULLIF(src.title, ''), NULLIF(src.candidate_key, ''), src.derived_object_id)
                 FROM oa_derived_object src
                 JOIN oa_derived_object tgt ON tgt.derived_object_id = ?2
                 WHERE src.derived_object_id = ?1",
                params![archive_link.source_object_id, archive_link.target_object_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|source| StorageError::InvalidDerivationWrite {
                detail: format!("failed to map archive-link artifact edge: {source}"),
            })?;

        let Some((source_artifact_id, target_artifact_id, derived_object_type, label)) = row else {
            continue;
        };
        if source_artifact_id == target_artifact_id {
            continue;
        }
        let link_value = format!(
            "{}:{}:{}",
            archive_link.link_type, derived_object_type, label
        );
        insert_artifact_link(
            tx,
            &source_artifact_id,
            &target_artifact_id,
            ArtifactLinkType::ReconciledObject,
            &link_value,
        )?;
    }
    Ok(())
}

pub(super) fn load_artifact_links(
    connection: &Connection,
    artifact_id: &str,
) -> StorageResult<Vec<ArtifactLinkRecord>> {
    let mut stmt = connection
        .prepare(
            "SELECT al.artifact_link_id,
                    al.source_artifact_id,
                    src.title,
                    src.source_conversation_key,
                    al.target_artifact_id,
                    tgt.title,
                    tgt.source_conversation_key,
                    al.link_type,
                    al.link_value
             FROM oa_artifact_link al
             JOIN oa_artifact src ON src.artifact_id = al.source_artifact_id
             JOIN oa_artifact tgt ON tgt.artifact_id = al.target_artifact_id
             WHERE al.source_artifact_id = ?1 OR al.target_artifact_id = ?1
             ORDER BY al.link_type ASC, al.link_value ASC, al.source_artifact_id ASC, al.target_artifact_id ASC, al.artifact_link_id ASC",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare artifact links query: {source}"),
        })?;
    let rows = stmt
        .query_map([artifact_id], |row| {
            Ok(ArtifactLinkRecord {
                artifact_link_id: row.get(0)?,
                source_artifact_id: row.get(1)?,
                source_title: row.get(2)?,
                source_note_path: row.get(3)?,
                target_artifact_id: row.get(4)?,
                target_title: row.get(5)?,
                target_note_path: row.get(6)?,
                link_type: parse_artifact_link_type(row.get(7)?)?,
                link_value: row.get(8)?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load artifact links: {source}"),
        })?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map artifact links: {source}"),
        })
}
