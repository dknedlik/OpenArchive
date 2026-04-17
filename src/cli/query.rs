use anyhow::Context;
use clap::{Args, Subcommand};

#[derive(Args)]
pub struct ArtifactsArgs {
    /// Filter by source type (chatgpt_export, claude_export, grok_export, gemini_takeout, text_file, markdown_file, obsidian_vault)
    #[arg(long, value_name = "TYPE")]
    pub source: Option<String>,

    /// Filter by enrichment status (pending, running, completed, partial, failed)
    #[arg(long, value_name = "STATUS")]
    pub status: Option<String>,

    /// Maximum number of results (default: 20, max: 100)
    #[arg(long, value_name = "N")]
    pub limit: Option<usize>,

    /// Pagination offset (default: 0)
    #[arg(long, value_name = "N")]
    pub offset: Option<usize>,
}

#[derive(Args)]
pub struct ArtifactArgs {
    /// Artifact ID to load
    pub id: String,

    /// Include segment content in output
    #[arg(long)]
    pub segments: bool,

    /// Segment pagination offset (requires --segments)
    #[arg(long, value_name = "N")]
    pub segment_offset: Option<usize>,

    /// Maximum segments to return (requires --segments, default: 50)
    #[arg(long, value_name = "N")]
    pub segment_limit: Option<usize>,
}

#[derive(Args)]
pub struct SearchArgs {
    /// Search query text
    pub query: String,

    /// Maximum number of results (default: 20, max: 100)
    #[arg(long, value_name = "N")]
    pub limit: Option<usize>,

    /// Filter by source type (chatgpt_export, claude_export, grok_export, gemini_takeout, text_file, markdown_file, obsidian_vault)
    #[arg(long, value_name = "TYPE")]
    pub source: Option<String>,

    /// Filter by object type (summary, memory, entity, relationship, classification)
    #[arg(long, value_name = "TYPE")]
    pub object_type: Option<String>,
}

#[derive(Args)]
pub struct TimelineArgs {
    /// Filter by keyword in title
    #[arg(long, value_name = "TEXT")]
    pub keyword: Option<String>,

    /// Filter by source type (chatgpt_export, claude_export, grok_export, gemini_takeout, text_file, markdown_file, obsidian_vault)
    #[arg(long, value_name = "TYPE")]
    pub source: Option<String>,

    /// Filter by imported note tag (exact normalized match)
    #[arg(long, value_name = "TAG")]
    pub tag: Option<String>,

    /// Filter by imported note path prefix
    #[arg(long, value_name = "PREFIX")]
    pub path_prefix: Option<String>,

    /// Maximum number of results (default: 50, max: 100)
    #[arg(long, value_name = "N")]
    pub limit: Option<usize>,

    /// Pagination offset (default: 0)
    #[arg(long, value_name = "N")]
    pub offset: Option<usize>,
}

#[derive(Args)]
pub struct ReviewArgs {
    #[command(subcommand)]
    pub command: ReviewCommand,
}

#[derive(Subcommand)]
pub enum ReviewCommand {
    /// List items in the review queue
    List {
        /// Maximum number of results (default: 20, max: 100)
        #[arg(long, value_name = "N")]
        limit: Option<usize>,
    },
    /// Record a decision on a review item
    Decide {
        /// Artifact ID to decide on
        artifact_id: String,
        /// Decision status (dismissed, noted, resolved)
        status: String,
        /// Review kind (artifact_needs_attention, artifact_missing_summary, object_low_confidence, candidate_key_collision, object_missing_evidence)
        #[arg(long, value_name = "KIND")]
        kind: String,
        /// Derived object ID (required for object_low_confidence, candidate_key_collision, object_missing_evidence)
        #[arg(long, value_name = "ID")]
        object: Option<String>,
        /// Note text (required when status is noted)
        #[arg(long, value_name = "TEXT")]
        note: Option<String>,
    },
    /// Retry enrichment for an artifact
    Retry {
        /// Artifact ID to retry
        artifact_id: String,
    },
}

pub(crate) fn artifacts_command(args: ArtifactsArgs) -> anyhow::Result<()> {
    let runtime = crate::cli::load_local_runtime(true)?;

    let source_type = match args.source {
        Some(value) => Some(
            open_archive::storage::SourceType::parse(&value)
                .ok_or_else(|| anyhow::anyhow!("invalid source_type: '{value}'"))?,
        ),
        None => None,
    };
    let enrichment_status = match args.status {
        Some(value) => Some(
            open_archive::storage::EnrichmentStatus::parse(&value)
                .ok_or_else(|| anyhow::anyhow!("invalid enrichment_status: '{value}'"))?,
        ),
        None => None,
    };

    let filters = open_archive::storage::ArtifactListFilters {
        source_type,
        enrichment_status,
        ..Default::default()
    };

    let limit = args.limit.unwrap_or(20).clamp(1, 100);
    let offset = args.offset.unwrap_or(0);

    let artifacts = match runtime
        .services
        .app
        .artifacts
        .list_artifacts_filtered(&filters, limit, offset)
    {
        Ok(items) => items,
        Err(open_archive::StorageError::UnsupportedOperation { .. }) => {
            if source_type.is_some() || enrichment_status.is_some() {
                return Err(anyhow::anyhow!(
                    "filtered listing is not supported by the current storage provider"
                ));
            }
            runtime
                .services
                .app
                .artifacts
                .list_artifacts()
                .context("failed to list artifacts")?
                .into_iter()
                .skip(offset)
                .take(limit)
                .collect::<Vec<_>>()
        }
        Err(err) => return Err(anyhow::Error::new(err).context("failed to list artifacts")),
    };

    if artifacts.is_empty() {
        println!("no artifacts found");
        return Ok(());
    }

    for artifact in artifacts {
        let display_title = artifact
            .title
            .or(artifact.note_path)
            .unwrap_or_else(|| "(untitled)".to_string());
        println!(
            "{} {} {} {}",
            artifact.artifact_id,
            artifact.source_type,
            artifact.enrichment_status.as_str(),
            display_title
        );
    }

    Ok(())
}

pub(crate) fn search_command(args: SearchArgs) -> anyhow::Result<()> {
    let runtime = crate::cli::load_local_runtime(true)?;

    let service = runtime
        .services
        .app
        .require_search()
        .map_err(|err| anyhow::Error::new(err).context("search is unavailable"))?;

    let source_type = match args.source {
        Some(value) => Some(
            open_archive::storage::SourceType::parse(&value)
                .ok_or_else(|| anyhow::anyhow!("invalid source_type: '{value}'"))?,
        ),
        None => None,
    };

    let object_type = match args.object_type {
        Some(value) => Some(
            open_archive::storage::DerivedObjectType::parse(&value)
                .ok_or_else(|| anyhow::anyhow!("invalid object_type: '{value}'"))?,
        ),
        None => None,
    };

    let limit = args.limit.unwrap_or(20).clamp(1, 100);

    let filters = open_archive::storage::SearchFilters {
        object_type,
        source_type,
        tag: None,
        alias: None,
        path_prefix: None,
    };

    let response = service
        .search(open_archive::app::search::ArchiveSearchRequest {
            query_text: args.query,
            limit,
            filters,
        })
        .map_err(|err| anyhow::Error::new(err).context("search failed"))?;

    if response.hits.is_empty() {
        println!("no results");
        return Ok(());
    }

    const SNIPPET_TRUNCATE_LEN: usize = 120;
    for hit in response.hits {
        let match_kind_str = format_match_kind(&hit.match_kind);
        let snippet = if hit.snippet.chars().count() > SNIPPET_TRUNCATE_LEN {
            let cutoff = hit
                .snippet
                .char_indices()
                .nth(SNIPPET_TRUNCATE_LEN)
                .map(|(idx, _)| idx)
                .unwrap_or_else(|| hit.snippet.len());
            format!("{}…", &hit.snippet[..cutoff])
        } else {
            hit.snippet
        };
        // Score formatted to 2 decimal places as per requirements
        println!("{:.2} {} {}", hit.score, match_kind_str, hit.artifact_id);
        println!("  {}", snippet);
    }

    Ok(())
}

fn format_match_kind(kind: &open_archive::app::search::SearchMatchKind) -> String {
    use open_archive::app::search::SearchMatchKind;
    match kind {
        SearchMatchKind::ArtifactTitle => "title".to_string(),
        SearchMatchKind::ImportedNoteTag { .. } => "tag".to_string(),
        SearchMatchKind::ImportedNoteAlias { .. } => "alias".to_string(),
        SearchMatchKind::ImportedNotePath => "path".to_string(),
        SearchMatchKind::ImportedExternalLink { .. } => "link".to_string(),
        SearchMatchKind::DerivedObject { derived_type, .. } => derived_type.as_str().to_string(),
        SearchMatchKind::SegmentExcerpt { .. } => "segment".to_string(),
    }
}

pub(crate) fn timeline_command(args: TimelineArgs) -> anyhow::Result<()> {
    let runtime = crate::cli::load_local_runtime(true)?;

    let source_type = match args.source {
        Some(value) => Some(
            open_archive::storage::SourceType::parse(&value)
                .ok_or_else(|| anyhow::anyhow!("invalid source_type: '{value}'"))?,
        ),
        None => None,
    };

    let limit = args.limit.unwrap_or(50).clamp(1, 100);
    let offset = args.offset.unwrap_or(0);

    let filters = open_archive::storage::TimelineFilters {
        keyword: args.keyword,
        source_type,
        tag: args.tag.map(|t| t.to_lowercase()),
        path_prefix: args.path_prefix.map(|p| p.to_lowercase()),
    };

    let entries = runtime
        .services
        .app
        .artifacts
        .get_timeline(&filters, limit, offset)
        .map_err(|err| anyhow::Error::new(err).context("failed to load timeline"))?;

    if entries.is_empty() {
        println!("no entries");
        return Ok(());
    }

    const SUMMARY_TRUNCATE_LEN: usize = 120;
    for entry in entries {
        // Format captured_at as date-only (extract YYYY-MM-DD from ISO 8601)
        let captured_at_date = entry
            .captured_at
            .split('T')
            .next()
            .unwrap_or(&entry.captured_at);
        let display_title = entry
            .title
            .or(entry.note_path)
            .unwrap_or_else(|| "(untitled)".to_string());
        println!(
            "{} {} {} {}",
            captured_at_date, entry.source_type, entry.artifact_id, display_title
        );

        // Summary snippet on second line only when present
        if let Some(summary) = entry.summary_snippet {
            let truncated = if summary.chars().count() > SUMMARY_TRUNCATE_LEN {
                let cutoff = summary
                    .char_indices()
                    .nth(SUMMARY_TRUNCATE_LEN)
                    .map(|(idx, _)| idx)
                    .unwrap_or_else(|| summary.len());
                format!("{}…", &summary[..cutoff])
            } else {
                summary
            };
            println!("  {}", truncated);
        }
    }

    Ok(())
}

pub(crate) fn review_command(args: ReviewArgs) -> anyhow::Result<()> {
    let runtime = crate::cli::load_local_runtime(true)?;

    let service = runtime
        .services
        .app
        .require_review()
        .map_err(|err| anyhow::Error::new(err).context("review service is unavailable"))?;

    match args.command {
        ReviewCommand::List { limit } => review_list_command(service, limit),
        ReviewCommand::Decide {
            artifact_id,
            status,
            kind,
            object,
            note,
        } => review_decide_command(service, artifact_id, status, kind, object, note),
        ReviewCommand::Retry { artifact_id } => review_retry_command(service, artifact_id),
    }
}

fn review_list_command(
    service: &open_archive::app::review::ReviewService,
    limit: Option<usize>,
) -> anyhow::Result<()> {
    use open_archive::app::review::ReviewQueueRequest;
    use open_archive::storage::ReviewQueueFilters;

    let limit = limit.unwrap_or(20).clamp(1, 100);

    let response = service
        .list(ReviewQueueRequest {
            filters: ReviewQueueFilters::default(),
            limit,
        })
        .map_err(|err| anyhow::Error::new(err).context("failed to list review items"))?;

    if response.items.is_empty() {
        println!("no review items");
        return Ok(());
    }

    for item in response.items {
        let title = item.title.unwrap_or_else(|| "(untitled)".to_string());
        let priority_str = format!("[{}]", item.priority.as_str());
        let actions_str = item
            .recommended_actions
            .iter()
            .map(|a| a.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        println!(
            "{} {} {} {}",
            priority_str,
            item.kind.as_str(),
            item.artifact_id,
            title
        );
        println!("  {}", item.reason);
        println!("  actions: {}", actions_str);
    }

    Ok(())
}

fn review_decide_command(
    service: &open_archive::app::review::ReviewService,
    artifact_id: String,
    status: String,
    kind: String,
    object: Option<String>,
    note: Option<String>,
) -> anyhow::Result<()> {
    use open_archive::app::review::ReviewDecisionRequest;
    use open_archive::storage::{ReviewDecisionStatus, ReviewItemKind};

    let kind = ReviewItemKind::parse(&kind)
        .ok_or_else(|| anyhow::anyhow!("invalid review kind: '{kind}'"))?;

    let decision_status = ReviewDecisionStatus::parse(&status)
        .ok_or_else(|| anyhow::anyhow!("invalid decision status: '{status}'"))?;

    // Validate that note is provided when status is noted
    if decision_status == ReviewDecisionStatus::Noted && note.is_none() {
        return Err(anyhow::anyhow!(
            "--note is required when decision status is 'noted'"
        ));
    }

    let decision_id = service
        .record_decision(ReviewDecisionRequest {
            kind,
            artifact_id,
            derived_object_id: object,
            decision_status,
            note_text: note,
            decided_by: None,
        })
        .map_err(|err| anyhow::Error::new(err).context("failed to record decision"))?;

    println!("recorded decision_id={}", decision_id);

    Ok(())
}

fn review_retry_command(
    service: &open_archive::app::review::ReviewService,
    artifact_id: String,
) -> anyhow::Result<()> {
    use open_archive::app::review::RetryArtifactRequest;

    let job_id = service
        .retry_artifact(RetryArtifactRequest { artifact_id })
        .map_err(|err| anyhow::Error::new(err).context("failed to queue retry"))?;

    println!("job_id={}", job_id);

    Ok(())
}

pub(crate) fn artifact_command(args: ArtifactArgs) -> anyhow::Result<()> {
    let runtime = crate::cli::load_local_runtime(true)?;

    let service = runtime
        .services
        .app
        .require_artifact_detail()
        .map_err(|err| anyhow::Error::new(err).context("artifact detail is unavailable"))?;

    // Warn if offset/limit provided without --segments
    if (args.segment_offset.is_some() || args.segment_limit.is_some()) && !args.segments {
        eprintln!(
            "warning: --segment-offset and --segment-limit have no effect without --segments"
        );
    }

    let include_segments = args.segments;
    let segment_offset = args.segment_offset.unwrap_or(0);
    let segment_limit = args
        .segment_limit
        .unwrap_or(open_archive::app::artifact_detail::DEFAULT_SEGMENT_LIMIT)
        .clamp(1, open_archive::app::artifact_detail::DEFAULT_SEGMENT_LIMIT);

    let artifact_id = args.id;
    let response = service
        .get(open_archive::app::artifact_detail::ArtifactDetailRequest {
            artifact_id: artifact_id.clone(),
            include_segments,
            segment_offset,
            segment_limit,
        })
        .map_err(|err| anyhow::Error::new(err).context("failed to load artifact detail"))?;

    let Some(detail) = response else {
        return Err(anyhow::anyhow!("artifact not found: {}", artifact_id));
    };

    // Header line: id source status title
    let display_title = detail
        .title
        .or_else(|| detail.note_path.clone())
        .unwrap_or_else(|| "(untitled)".to_string());
    println!(
        "{} {} {} {}",
        detail.artifact_id,
        detail.source_type.as_str(),
        detail.enrichment_status.as_str(),
        display_title
    );

    // Derived objects: one line each — object_id type title
    for obj in &detail.derived_objects {
        let obj_title = obj
            .title
            .clone()
            .unwrap_or_else(|| "(untitled)".to_string());
        println!(
            "{} {} {}",
            obj.derived_object_id,
            obj.derived_object_type.as_str(),
            obj_title
        );
    }

    // Segments (if --segments): one line each — [seq] role: text_content (truncated)
    if include_segments {
        const SEGMENT_TRUNCATE_LEN: usize = 120;
        for seg in &detail.segments {
            let role_str = seg
                .participant_role
                .as_ref()
                .map(|r| r.as_str())
                .unwrap_or("unknown");
            let content = &seg.text_content;
            let truncated = if content.chars().count() > SEGMENT_TRUNCATE_LEN {
                let cutoff = content
                    .char_indices()
                    .nth(SEGMENT_TRUNCATE_LEN)
                    .map(|(idx, _)| idx)
                    .unwrap_or_else(|| content.len());
                format!("{}…", &content[..cutoff])
            } else {
                content.clone()
            };
            println!("[{}] {}: {}", seg.sequence_no, role_str, truncated);
        }
    }

    Ok(())
}
