use std::collections::{BTreeSet, HashMap};

use crate::processor::types::ArtifactProcessorInput;
use crate::storage::types::{ArtifactClass, ImportedNoteMetadata, SourceType};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ArtifactFeatures {
    pub artifact_class: ArtifactClass,
    pub source_type: SourceType,
    pub segment_count: usize,
    pub char_count: usize,
    pub participant_count: usize,
    pub unique_participant_role_count: usize,
    pub heading_count: usize,
    pub numbered_list_count: usize,
    pub bullet_list_count: usize,
    pub task_count: usize,
    pub code_fence_count: usize,
    pub table_line_count: usize,
    pub question_count: usize,
    pub imperative_line_count: usize,
    pub wikilink_count: usize,
    pub markdown_link_count: usize,
    pub dataview_marker_count: usize,
    pub templater_marker_count: usize,
    pub command_line_count: usize,
    pub config_line_count: usize,
    pub setup_keyword_count: usize,
    pub planning_keyword_count: usize,
    pub project_keyword_count: usize,
    pub journal_keyword_count: usize,
    pub scientific_keyword_count: usize,
    pub technical_keyword_count: usize,
    pub definition_phrase_count: usize,
    pub first_person_count: usize,
    pub title_token_count: usize,
    pub title_setup_keyword_count: usize,
    pub title_planning_keyword_count: usize,
    pub title_project_keyword_count: usize,
    pub title_scientific_keyword_count: usize,
    pub title_technical_keyword_count: usize,
    pub title_personal_keyword_count: usize,
    pub title_looks_date: bool,
    pub title_looks_term_like: bool,
    pub title_contains_dashboard_like: bool,
    pub has_document_type_definition: bool,
    pub has_document_type_concept: bool,
    pub has_document_type_meeting: bool,
    pub has_document_type_dashboard: bool,
    pub has_journal_tag: bool,
    pub has_definition_tag: bool,
    pub has_project_tag: bool,
    pub has_template_path_hint: bool,
    pub has_dashboard_path_hint: bool,
    pub metadata_property_keys: BTreeSet<String>,
}

impl Default for ArtifactFeatures {
    fn default() -> Self {
        Self {
            artifact_class: ArtifactClass::Document,
            source_type: SourceType::MarkdownFile,
            segment_count: 0,
            char_count: 0,
            participant_count: 0,
            unique_participant_role_count: 0,
            heading_count: 0,
            numbered_list_count: 0,
            bullet_list_count: 0,
            task_count: 0,
            code_fence_count: 0,
            table_line_count: 0,
            question_count: 0,
            imperative_line_count: 0,
            wikilink_count: 0,
            markdown_link_count: 0,
            dataview_marker_count: 0,
            templater_marker_count: 0,
            command_line_count: 0,
            config_line_count: 0,
            setup_keyword_count: 0,
            planning_keyword_count: 0,
            project_keyword_count: 0,
            journal_keyword_count: 0,
            scientific_keyword_count: 0,
            technical_keyword_count: 0,
            definition_phrase_count: 0,
            first_person_count: 0,
            title_token_count: 0,
            title_setup_keyword_count: 0,
            title_planning_keyword_count: 0,
            title_project_keyword_count: 0,
            title_scientific_keyword_count: 0,
            title_technical_keyword_count: 0,
            title_personal_keyword_count: 0,
            title_looks_date: false,
            title_looks_term_like: false,
            title_contains_dashboard_like: false,
            has_document_type_definition: false,
            has_document_type_concept: false,
            has_document_type_meeting: false,
            has_document_type_dashboard: false,
            has_journal_tag: false,
            has_definition_tag: false,
            has_project_tag: false,
            has_template_path_hint: false,
            has_dashboard_path_hint: false,
            metadata_property_keys: BTreeSet::new(),
        }
    }
}

pub(super) fn extract_features(input: &ArtifactProcessorInput) -> ArtifactFeatures {
    let title = input.title.as_deref().unwrap_or_default();
    let title_lower = title.to_ascii_lowercase();
    let title_token_count = title_lower
        .split_whitespace()
        .filter(|token| !token.is_empty())
        .count();

    let mut features = ArtifactFeatures {
        artifact_class: input.artifact_class,
        source_type: input.source_type,
        segment_count: input.segments.len(),
        participant_count: input.participants.len(),
        title_token_count,
        title_setup_keyword_count: count_keyword_hits(&title_lower, SETUP_KEYWORDS),
        title_planning_keyword_count: count_keyword_hits(&title_lower, PLANNING_KEYWORDS),
        title_project_keyword_count: count_keyword_hits(&title_lower, PROJECT_KEYWORDS),
        title_scientific_keyword_count: count_keyword_hits(&title_lower, SCIENTIFIC_KEYWORDS),
        title_technical_keyword_count: count_keyword_hits(&title_lower, TECHNICAL_KEYWORDS),
        title_personal_keyword_count: count_keyword_hits(&title_lower, PERSONAL_KEYWORDS),
        title_looks_date: looks_like_date(&title_lower),
        title_looks_term_like: looks_like_term_title(&title_lower, title_token_count),
        title_contains_dashboard_like: title_lower.contains("dashboard")
            || title_lower == "inbox"
            || title_lower == "index"
            || title_lower == "home"
            || title_lower.ends_with(" home")
            || title_lower.ends_with(" hub")
            || title_lower.ends_with(" index")
            || title_lower.starts_with("all "),
        ..ArtifactFeatures::default()
    };

    let mut participant_roles = BTreeSet::new();
    for participant in &input.participants {
        participant_roles.insert(participant.participant_role.as_str().to_string());
    }
    for segment in &input.segments {
        if let Some(role) = segment.participant_role {
            participant_roles.insert(role.as_str().to_string());
        }
        features.char_count += segment.text_content.len();
        scan_text(&mut features, &segment.text_content);
    }
    features.unique_participant_role_count = participant_roles.len();

    if let Some(metadata) = input.imported_note_metadata.as_ref() {
        apply_note_metadata(&mut features, metadata);
    }

    features
}

fn apply_note_metadata(features: &mut ArtifactFeatures, metadata: &ImportedNoteMetadata) {
    if let Some(note_path) = metadata.note_path.as_deref() {
        let note_path = note_path.to_ascii_lowercase();
        features.has_template_path_hint = note_path.contains("/template")
            || note_path.contains("/templates/")
            || note_path.contains("/_data_/");
        features.has_dashboard_path_hint = note_path.contains("dashboard")
            || note_path.ends_with("/all notes.md")
            || note_path.ends_with("/all meetings.md");
    }

    let property_map = collect_property_map(&metadata.properties);
    for key in property_map.keys() {
        features.metadata_property_keys.insert(key.clone());
    }
    let document_type = property_map
        .get("document_type")
        .or_else(|| property_map.get("type"))
        .map(String::as_str)
        .unwrap_or_default();
    features.has_document_type_definition = document_type.contains("definition");
    features.has_document_type_concept = document_type.contains("concept");
    features.has_document_type_meeting = document_type.contains("meeting");
    features.has_document_type_dashboard = document_type.contains("dashboard");

    for tag in &metadata.tags {
        let tag = tag.normalized_tag.to_ascii_lowercase();
        if tag.contains("journal") || tag.contains("daily") || tag.contains("log") {
            features.has_journal_tag = true;
        }
        if tag.contains("definition") {
            features.has_definition_tag = true;
        }
        if tag.contains("project") || tag.contains("concept") {
            features.has_project_tag = true;
        }
    }
}

fn collect_property_map(
    properties: &[crate::storage::types::ImportedNotePropertyRecord],
) -> HashMap<String, String> {
    let mut values = HashMap::new();
    for property in properties {
        let key = property.property_key.to_ascii_lowercase();
        let value = property
            .value_text
            .clone()
            .unwrap_or_else(|| property.value_json.to_string())
            .to_ascii_lowercase();
        values.insert(key, value);
    }
    values
}

fn scan_text(features: &mut ArtifactFeatures, text: &str) {
    let mut in_fence = false;
    for raw_line in text.lines() {
        let line = raw_line.trim();
        let line_lower = line.to_ascii_lowercase();

        if line.starts_with("```") || line.starts_with("~~~") {
            features.code_fence_count += 1;
            in_fence = !in_fence;
        }
        if !in_fence {
            if line.starts_with('#') {
                features.heading_count += 1;
            }
            if is_numbered_list_item(line) {
                features.numbered_list_count += 1;
            }
            if is_bullet_list_item(line) {
                features.bullet_list_count += 1;
            }
            if line.starts_with("- [") || line.starts_with("* [") {
                features.task_count += 1;
            }
            if line.contains('|') && line.matches('|').count() >= 2 {
                features.table_line_count += 1;
            }
            if looks_like_imperative_line(&line_lower) {
                features.imperative_line_count += 1;
            }
        }

        features.question_count += line.matches('?').count();
        features.wikilink_count += line.matches("[[").count();
        features.markdown_link_count += line.matches("](").count();
        features.dataview_marker_count += count_markers(
            &line_lower,
            &[
                "```dataview",
                "```dataviewjs",
                "dataviewjs",
                "dv.",
                "customjs",
            ],
        );
        features.templater_marker_count +=
            count_markers(&line_lower, &["<%", "%>", "{{title}}", "tp."]);
        if looks_like_command_line(line) {
            features.command_line_count += 1;
        }
        if looks_like_config_line(line) {
            features.config_line_count += 1;
        }
        features.setup_keyword_count += count_keyword_hits(&line_lower, SETUP_KEYWORDS);
        features.planning_keyword_count += count_keyword_hits(&line_lower, PLANNING_KEYWORDS);
        features.project_keyword_count += count_keyword_hits(&line_lower, PROJECT_KEYWORDS);
        features.journal_keyword_count += count_keyword_hits(
            &line_lower,
            &[
                "today",
                "yesterday",
                "journal",
                "daily note",
                "i felt",
                "i learned",
                "i think",
                "i want",
            ],
        );
        features.scientific_keyword_count += count_keyword_hits(&line_lower, SCIENTIFIC_KEYWORDS);
        features.technical_keyword_count += count_keyword_hits(&line_lower, TECHNICAL_KEYWORDS);
        features.definition_phrase_count += count_definition_phrases(&line_lower);
        features.first_person_count += count_keyword_hits(
            &line_lower,
            &[" i ", " i'm ", " i’ve ", " my ", " me ", " we ", " our "],
        );
    }
}

const SETUP_KEYWORDS: &[&str] = &[
    "install",
    "setup",
    "set up",
    "configure",
    "configuration",
    "enable",
    "api key",
    "run ",
    "clone ",
    "download",
    "import",
    "usage",
    "prerequisite",
    "requirement",
];

const PLANNING_KEYWORDS: &[&str] = &[
    "next step",
    "todo",
    "follow up",
    "planning",
    "prototype",
    "action item",
    "decision",
    "owner",
];

const PROJECT_KEYWORDS: &[&str] = &[
    "project",
    "milestone",
    "roadmap",
    "deliverable",
    "meeting",
    "scope",
];

const SCIENTIFIC_KEYWORDS: &[&str] = &[
    "orbit",
    "equation",
    "radiation",
    "belt",
    "planet",
    "satellite",
    "hypothesis",
    "experiment",
    "periapsis",
    "enceladus",
    "geyser",
    "hydroponic",
    "formula",
];

const TECHNICAL_KEYWORDS: &[&str] = &[
    "api",
    "plugin",
    "database",
    "docker",
    "schema",
    "repo",
    "repository",
    "config",
    "model",
    "sql",
    "json",
    "yaml",
    "obsidian",
];

const PERSONAL_KEYWORDS: &[&str] = &["my ", "myself", "personal", "for me", "i "];

fn count_markers(line: &str, markers: &[&str]) -> usize {
    markers
        .iter()
        .filter(|marker| line.contains(**marker))
        .count()
}

fn count_keyword_hits(line: &str, keywords: &[&str]) -> usize {
    keywords
        .iter()
        .filter(|keyword| line.contains(**keyword))
        .count()
}

fn count_definition_phrases(line: &str) -> usize {
    count_keyword_hits(
        line,
        &[
            " is the point ",
            " is a ",
            " refers to ",
            " is defined as ",
            " closest approach ",
            " minimum distance ",
        ],
    )
}

fn is_numbered_list_item(line: &str) -> bool {
    let digits = line.chars().take_while(|ch| ch.is_ascii_digit()).count();
    digits > 0 && line[digits..].starts_with('.')
}

fn is_bullet_list_item(line: &str) -> bool {
    line.starts_with("- ") || line.starts_with("* ")
}

fn looks_like_command_line(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with('$')
        || [
            "git ",
            "cargo ",
            "docker ",
            "docker-compose ",
            "npm ",
            "pnpm ",
            "yarn ",
            "pip ",
            "uv ",
            "make ",
            "curl ",
            "psql ",
            "cp ",
            "mv ",
            "cd ",
        ]
        .iter()
        .any(|prefix| trimmed.starts_with(prefix))
}

fn looks_like_imperative_line(line: &str) -> bool {
    [
        "install ",
        "create ",
        "open ",
        "click ",
        "choose ",
        "select ",
        "set ",
        "set up ",
        "configure ",
        "enable ",
        "disable ",
        "add ",
        "remove ",
        "use ",
        "run ",
        "import ",
    ]
    .iter()
    .any(|prefix| line.starts_with(prefix))
}

fn looks_like_config_line(line: &str) -> bool {
    let trimmed = line.trim();
    if let Some((lhs, rhs)) = trimmed.split_once('=') {
        return !rhs.trim().is_empty() && looks_like_key(lhs);
    }
    if let Some((lhs, rhs)) = trimmed.split_once(':') {
        return !rhs.trim().is_empty() && looks_like_key(lhs);
    }
    false
}

fn looks_like_key(value: &str) -> bool {
    let key = value.trim();
    !key.is_empty()
        && key.len() <= 48
        && !key.contains(' ')
        && key
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
}

fn looks_like_date(title: &str) -> bool {
    let bytes = title.as_bytes();
    bytes.len() == 10
        && bytes.get(4) == Some(&b'-')
        && bytes.get(7) == Some(&b'-')
        && bytes
            .iter()
            .enumerate()
            .all(|(index, ch)| matches!(index, 4 | 7) || ch.is_ascii_digit())
}

fn looks_like_term_title(title: &str, token_count: usize) -> bool {
    token_count > 0
        && token_count <= 4
        && !title.contains(':')
        && !title.contains('?')
        && !title.contains('.')
}
