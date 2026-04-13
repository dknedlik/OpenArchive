use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use postgres::Client;

use crate::error::{DbError, StorageError, StorageResult};
use crate::storage::types::{ArtifactLinkRecord, ArtifactLinkType};
use crate::storage::writeback_store::NewArchiveLink;

static ID_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn sync_structural_links_for_artifact(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
) -> StorageResult<()> {
    resolve_note_links_by_path(client, connection_string, artifact_id)?;
    resolve_note_links_by_alias(client, connection_string, artifact_id)?;
    sync_wikilink_edges(client, connection_string, artifact_id)?;
    sync_shared_tag_edges(client, connection_string, artifact_id)?;
    sync_alias_edges(client, connection_string, artifact_id)?;
    Ok(())
}

pub fn sync_reconcile_links_for_archive_links(
    client: &mut Client,
    connection_string: &str,
    archive_links: &[NewArchiveLink],
) -> StorageResult<()> {
    for archive_link in archive_links {
        let row = client
            .query_opt(
                "SELECT src.artifact_id,
                        tgt.artifact_id,
                        src.derived_object_type,
                        COALESCE(NULLIF(src.title, ''), NULLIF(src.candidate_key, ''), src.derived_object_id)
                 FROM oa_derived_object src
                 JOIN oa_derived_object tgt
                   ON tgt.derived_object_id = $2
                 WHERE src.derived_object_id = $1",
                &[&archive_link.source_object_id, &archive_link.target_object_id],
            )
            .map_err(|source| map_pg_err(connection_string, source))?;

        let Some(row) = row else {
            continue;
        };

        let source_artifact_id: String = row.get(0);
        let target_artifact_id: String = row.get(1);
        if source_artifact_id == target_artifact_id {
            continue;
        }

        let derived_object_type: String = row.get(2);
        let label: String = row.get(3);
        let link_value = format!(
            "{}:{}:{}",
            archive_link.link_type, derived_object_type, label
        );

        insert_artifact_link(
            client,
            &source_artifact_id,
            &target_artifact_id,
            ArtifactLinkType::ReconciledObject,
            &link_value,
        )?;
    }

    Ok(())
}

pub fn load_artifact_links(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
) -> StorageResult<Vec<ArtifactLinkRecord>> {
    let rows = client
        .query(
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
             WHERE al.source_artifact_id = $1 OR al.target_artifact_id = $1
             ORDER BY al.link_type ASC,
                      al.link_value ASC,
                      al.source_artifact_id ASC,
                      al.target_artifact_id ASC,
                      al.artifact_link_id ASC",
            &[&artifact_id],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;

    let mut links = Vec::with_capacity(rows.len());
    for row in rows {
        let row_artifact_id: String = row.get(1);
        let link_type: String = row.get(7);
        links.push(ArtifactLinkRecord {
            artifact_link_id: row.get(0),
            source_artifact_id: row_artifact_id.clone(),
            source_title: row.get(2),
            source_note_path: row.get(3),
            target_artifact_id: row.get(4),
            target_title: row.get(5),
            target_note_path: row.get(6),
            link_type: ArtifactLinkType::parse(&link_type).ok_or_else(|| {
                StorageError::InvalidArtifactLinkType {
                    artifact_id: row_artifact_id,
                    value: link_type,
                }
            })?,
            link_value: row.get(8),
        });
    }

    Ok(links)
}

fn resolve_note_links_by_path(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
) -> StorageResult<()> {
    client
        .execute(
            "UPDATE oa_artifact_note_link
             SET resolved_artifact_id = $1,
                 resolution_status = 'resolved'
             WHERE resolved_artifact_id IS NULL
               AND resolution_status = 'unresolved'
               AND target_kind IN ('note', 'heading', 'block')
               AND target_path = (
                    SELECT source_conversation_key
                    FROM oa_artifact
                    WHERE artifact_id = $1
                      AND source_type = 'obsidian_vault'
               )
               AND artifact_id <> $1",
            &[&artifact_id],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;
    Ok(())
}

fn resolve_note_links_by_alias(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
) -> StorageResult<()> {
    client
        .execute(
            "UPDATE oa_artifact_note_link l
             SET resolved_artifact_id = $1,
                 resolution_status = 'resolved'
             FROM oa_artifact_note_alias a
             WHERE a.artifact_id = $1
               AND l.resolved_artifact_id IS NULL
               AND l.resolution_status = 'unresolved'
               AND l.target_kind = 'note'
               AND COALESCE(l.normalized_target, '') = a.normalized_alias
               AND l.artifact_id <> $1
               AND NOT EXISTS (
                    SELECT 1
                    FROM oa_artifact_note_alias a2
                    WHERE a2.normalized_alias = a.normalized_alias
                      AND a2.artifact_id <> a.artifact_id
               )",
            &[&artifact_id],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;
    Ok(())
}

fn sync_wikilink_edges(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
) -> StorageResult<()> {
    let rows = client
        .query(
            "SELECT DISTINCT artifact_id,
                    resolved_artifact_id,
                    COALESCE(target_path, normalized_target, raw_target) AS link_value
             FROM oa_artifact_note_link
             WHERE resolved_artifact_id IS NOT NULL
               AND artifact_id <> resolved_artifact_id
               AND (artifact_id = $1 OR resolved_artifact_id = $1)",
            &[&artifact_id],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;

    for row in rows {
        insert_artifact_link(
            client,
            &row.get::<_, String>(0),
            &row.get::<_, String>(1),
            ArtifactLinkType::Wikilink,
            &row.get::<_, String>(2),
        )?;
    }

    Ok(())
}

fn sync_shared_tag_edges(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
) -> StorageResult<()> {
    let rows = client
        .query(
            "SELECT DISTINCT LEAST($1, other_tags.artifact_id) AS source_artifact_id,
                    GREATEST($1, other_tags.artifact_id) AS target_artifact_id,
                    current_tags.normalized_tag
             FROM oa_artifact_note_tag current_tags
             JOIN oa_artifact_note_tag other_tags
               ON other_tags.normalized_tag = current_tags.normalized_tag
              AND other_tags.artifact_id <> current_tags.artifact_id
             JOIN oa_artifact other_artifact
               ON other_artifact.artifact_id = other_tags.artifact_id
              AND other_artifact.source_type = 'obsidian_vault'
             WHERE current_tags.artifact_id = $1
               AND current_tags.normalized_tag <> ''",
            &[&artifact_id],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;

    for row in rows {
        insert_artifact_link(
            client,
            &row.get::<_, String>(0),
            &row.get::<_, String>(1),
            ArtifactLinkType::SharedTag,
            &row.get::<_, String>(2),
        )?;
    }

    Ok(())
}

fn sync_alias_edges(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
) -> StorageResult<()> {
    let rows = client
        .query(
            "SELECT DISTINCT LEAST($1, other_aliases.artifact_id) AS source_artifact_id,
                    GREATEST($1, other_aliases.artifact_id) AS target_artifact_id,
                    current_aliases.normalized_alias
             FROM oa_artifact_note_alias current_aliases
             JOIN oa_artifact_note_alias other_aliases
               ON other_aliases.normalized_alias = current_aliases.normalized_alias
              AND other_aliases.artifact_id <> current_aliases.artifact_id
             JOIN oa_artifact other_artifact
               ON other_artifact.artifact_id = other_aliases.artifact_id
              AND other_artifact.source_type = 'obsidian_vault'
             WHERE current_aliases.artifact_id = $1
               AND current_aliases.normalized_alias <> ''",
            &[&artifact_id],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;

    for row in rows {
        insert_artifact_link(
            client,
            &row.get::<_, String>(0),
            &row.get::<_, String>(1),
            ArtifactLinkType::Alias,
            &row.get::<_, String>(2),
        )?;
    }

    Ok(())
}

pub(crate) fn insert_artifact_link(
    client: &mut Client,
    source_artifact_id: &str,
    target_artifact_id: &str,
    link_type: ArtifactLinkType,
    link_value: &str,
) -> StorageResult<()> {
    client
        .execute(
            "INSERT INTO oa_artifact_link
             (artifact_link_id, source_artifact_id, target_artifact_id, link_type, link_value)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (source_artifact_id, target_artifact_id, link_type, link_value) DO NOTHING",
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

fn new_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{nanos:x}-{counter:x}")
}

fn map_pg_err(connection_string: &str, source: postgres::Error) -> StorageError {
    StorageError::Db(DbError::ConnectPostgres {
        connection_string: connection_string.to_string(),
        source: Box::new(source),
    })
}
