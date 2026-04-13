use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use oracle::Connection;

use crate::error::{StorageError, StorageResult};
use crate::storage::types::ArtifactLinkType;
use crate::storage::writeback_store::NewArchiveLink;

static ID_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn sync_structural_links_for_artifact(
    conn: &Connection,
    artifact_id: &str,
) -> StorageResult<()> {
    resolve_note_links_by_path(conn, artifact_id)?;
    resolve_note_links_by_alias(conn, artifact_id)?;
    sync_wikilink_edges(conn, artifact_id)?;
    sync_shared_tag_edges(conn, artifact_id)?;
    sync_alias_edges(conn, artifact_id)?;
    Ok(())
}

pub fn sync_reconcile_links_for_archive_links(
    conn: &Connection,
    archive_links: &[NewArchiveLink],
) -> StorageResult<()> {
    for archive_link in archive_links {
        let row = conn
            .query_row_as::<(String, String, String, String)>(
                "SELECT src.artifact_id,
                        tgt.artifact_id,
                        src.derived_object_type,
                        COALESCE(NULLIF(src.title, ''), NULLIF(src.candidate_key, ''), src.derived_object_id)
                 FROM oa_derived_object src
                 JOIN oa_derived_object tgt
                   ON tgt.derived_object_id = :2
                 WHERE src.derived_object_id = :1",
                &[&archive_link.source_object_id, &archive_link.target_object_id],
            )
            .or_else(|source| match source.kind() {
                oracle::ErrorKind::NoDataFound => Ok((
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                )),
                _ => Err(StorageError::InsertArtifactLink {
                    source_artifact_id: String::new(),
                    target_artifact_id: String::new(),
                    link_type: ArtifactLinkType::ReconciledObject.as_str().to_string(),
                    source: Box::new(source),
                }),
            })?;

        if row.0.is_empty() || row.1.is_empty() || row.0 == row.1 {
            continue;
        }

        let link_value = format!("{}:{}:{}", archive_link.link_type, row.2, row.3);
        insert_artifact_link(
            conn,
            &row.0,
            &row.1,
            ArtifactLinkType::ReconciledObject,
            &link_value,
        )?;
    }

    Ok(())
}

fn resolve_note_links_by_path(conn: &Connection, artifact_id: &str) -> StorageResult<()> {
    conn.execute(
        "UPDATE oa_artifact_note_link
         SET resolved_artifact_id = :1,
             resolution_status = 'resolved'
         WHERE resolved_artifact_id IS NULL
           AND resolution_status = 'unresolved'
           AND target_kind IN ('note', 'heading', 'block')
           AND target_path = (
                SELECT source_conversation_key
                FROM oa_artifact
                WHERE artifact_id = :1
                  AND source_type = 'obsidian_vault'
           )
           AND artifact_id <> :1",
        &[&artifact_id],
    )
    .map_err(|source| wrap_link_error(artifact_id, ArtifactLinkType::Wikilink, source))?;
    Ok(())
}

fn resolve_note_links_by_alias(conn: &Connection, artifact_id: &str) -> StorageResult<()> {
    conn.execute(
        "UPDATE oa_artifact_note_link l
         SET resolved_artifact_id = :1,
             resolution_status = 'resolved'
         WHERE l.resolved_artifact_id IS NULL
           AND l.resolution_status = 'unresolved'
           AND l.target_kind = 'note'
           AND l.artifact_id <> :1
           AND EXISTS (
                SELECT 1
                FROM oa_artifact_note_alias a
                WHERE a.artifact_id = :1
                  AND NVL(l.normalized_target, ' ') = a.normalized_alias
                  AND NOT EXISTS (
                        SELECT 1
                        FROM oa_artifact_note_alias a2
                        WHERE a2.normalized_alias = a.normalized_alias
                          AND a2.artifact_id <> a.artifact_id
                  )
           )",
        &[&artifact_id],
    )
    .map_err(|source| wrap_link_error(artifact_id, ArtifactLinkType::Alias, source))?;
    Ok(())
}

fn sync_wikilink_edges(conn: &Connection, artifact_id: &str) -> StorageResult<()> {
    let rows = conn
        .query(
            "SELECT DISTINCT artifact_id,
                    resolved_artifact_id,
                    COALESCE(target_path, normalized_target, raw_target) AS link_value
             FROM oa_artifact_note_link
             WHERE resolved_artifact_id IS NOT NULL
               AND artifact_id <> resolved_artifact_id
               AND (artifact_id = :1 OR resolved_artifact_id = :1)",
            &[&artifact_id],
        )
        .map_err(|source| wrap_link_error(artifact_id, ArtifactLinkType::Wikilink, source))?;

    for row_result in rows {
        let row = row_result
            .map_err(|source| wrap_link_error(artifact_id, ArtifactLinkType::Wikilink, source))?;
        let source_artifact_id: String = row
            .get(0)
            .map_err(|source| wrap_link_error(artifact_id, ArtifactLinkType::Wikilink, source))?;
        let target_artifact_id: String = row
            .get(1)
            .map_err(|source| wrap_link_error(artifact_id, ArtifactLinkType::Wikilink, source))?;
        let link_value: String = row
            .get(2)
            .map_err(|source| wrap_link_error(artifact_id, ArtifactLinkType::Wikilink, source))?;
        insert_artifact_link(
            conn,
            &source_artifact_id,
            &target_artifact_id,
            ArtifactLinkType::Wikilink,
            &link_value,
        )?;
    }

    Ok(())
}

fn sync_shared_tag_edges(conn: &Connection, artifact_id: &str) -> StorageResult<()> {
    let rows = conn
        .query(
            "SELECT DISTINCT LEAST(:1, other_tags.artifact_id) AS source_artifact_id,
                    GREATEST(:1, other_tags.artifact_id) AS target_artifact_id,
                    current_tags.normalized_tag
             FROM oa_artifact_note_tag current_tags
             JOIN oa_artifact_note_tag other_tags
               ON other_tags.normalized_tag = current_tags.normalized_tag
              AND other_tags.artifact_id <> current_tags.artifact_id
             JOIN oa_artifact other_artifact
               ON other_artifact.artifact_id = other_tags.artifact_id
              AND other_artifact.source_type = 'obsidian_vault'
             WHERE current_tags.artifact_id = :1
               AND current_tags.normalized_tag <> ''",
            &[&artifact_id],
        )
        .map_err(|source| wrap_link_error(artifact_id, ArtifactLinkType::SharedTag, source))?;

    for row_result in rows {
        let row = row_result
            .map_err(|source| wrap_link_error(artifact_id, ArtifactLinkType::SharedTag, source))?;
        let source_artifact_id: String = row
            .get(0)
            .map_err(|source| wrap_link_error(artifact_id, ArtifactLinkType::SharedTag, source))?;
        let target_artifact_id: String = row
            .get(1)
            .map_err(|source| wrap_link_error(artifact_id, ArtifactLinkType::SharedTag, source))?;
        let link_value: String = row
            .get(2)
            .map_err(|source| wrap_link_error(artifact_id, ArtifactLinkType::SharedTag, source))?;
        insert_artifact_link(
            conn,
            &source_artifact_id,
            &target_artifact_id,
            ArtifactLinkType::SharedTag,
            &link_value,
        )?;
    }

    Ok(())
}

fn sync_alias_edges(conn: &Connection, artifact_id: &str) -> StorageResult<()> {
    let rows = conn
        .query(
            "SELECT DISTINCT LEAST(:1, other_aliases.artifact_id) AS source_artifact_id,
                    GREATEST(:1, other_aliases.artifact_id) AS target_artifact_id,
                    current_aliases.normalized_alias
             FROM oa_artifact_note_alias current_aliases
             JOIN oa_artifact_note_alias other_aliases
               ON other_aliases.normalized_alias = current_aliases.normalized_alias
              AND other_aliases.artifact_id <> current_aliases.artifact_id
             JOIN oa_artifact other_artifact
               ON other_artifact.artifact_id = other_aliases.artifact_id
              AND other_artifact.source_type = 'obsidian_vault'
             WHERE current_aliases.artifact_id = :1
               AND current_aliases.normalized_alias <> ''",
            &[&artifact_id],
        )
        .map_err(|source| wrap_link_error(artifact_id, ArtifactLinkType::Alias, source))?;

    for row_result in rows {
        let row = row_result
            .map_err(|source| wrap_link_error(artifact_id, ArtifactLinkType::Alias, source))?;
        let source_artifact_id: String = row
            .get(0)
            .map_err(|source| wrap_link_error(artifact_id, ArtifactLinkType::Alias, source))?;
        let target_artifact_id: String = row
            .get(1)
            .map_err(|source| wrap_link_error(artifact_id, ArtifactLinkType::Alias, source))?;
        let link_value: String = row
            .get(2)
            .map_err(|source| wrap_link_error(artifact_id, ArtifactLinkType::Alias, source))?;
        insert_artifact_link(
            conn,
            &source_artifact_id,
            &target_artifact_id,
            ArtifactLinkType::Alias,
            &link_value,
        )?;
    }

    Ok(())
}

pub(crate) fn insert_artifact_link(
    conn: &Connection,
    source_artifact_id: &str,
    target_artifact_id: &str,
    link_type: ArtifactLinkType,
    link_value: &str,
) -> StorageResult<()> {
    conn.execute(
        "MERGE INTO oa_artifact_link t
         USING (
            SELECT :1 AS artifact_link_id,
                   :2 AS source_artifact_id,
                   :3 AS target_artifact_id,
                   :4 AS link_type,
                   :5 AS link_value
              FROM dual
         ) s
         ON (t.source_artifact_id = s.source_artifact_id
             AND t.target_artifact_id = s.target_artifact_id
             AND t.link_type = s.link_type
             AND t.link_value = s.link_value)
         WHEN NOT MATCHED THEN INSERT
             (artifact_link_id, source_artifact_id, target_artifact_id, link_type, link_value)
             VALUES (s.artifact_link_id, s.source_artifact_id, s.target_artifact_id, s.link_type, s.link_value)",
        &[
            &new_id("artlink"),
            &source_artifact_id,
            &target_artifact_id,
            &link_type.as_str(),
            &link_value,
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

fn wrap_link_error(
    artifact_id: &str,
    link_type: ArtifactLinkType,
    source: oracle::Error,
) -> StorageError {
    StorageError::InsertArtifactLink {
        source_artifact_id: artifact_id.to_string(),
        target_artifact_id: artifact_id.to_string(),
        link_type: link_type.as_str().to_string(),
        source: Box::new(source),
    }
}

fn new_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{nanos:x}-{counter:x}")
}
