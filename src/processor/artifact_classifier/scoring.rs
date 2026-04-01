use super::features::ArtifactFeatures;
use super::types::{
    ArchetypeScore, ArtifactArchetype, ArtifactClassificationDebug, ArtifactClassificationProfile,
    ArtifactFacet, FacetScore,
};

#[derive(Debug, Clone)]
struct ScoreContribution {
    score: i32,
    reason: &'static str,
}

#[derive(Debug, Clone, Default)]
struct ScoreCard {
    total: i32,
    contributions: Vec<ScoreContribution>,
}

impl ScoreCard {
    fn add(&mut self, score: i32, reason: &'static str) {
        if score == 0 {
            return;
        }
        self.total += score;
        self.contributions.push(ScoreContribution { score, reason });
    }
}

pub(super) fn classify(features: &ArtifactFeatures) -> ArtifactClassificationDebug {
    let archetype_cards = score_archetypes(features);
    let mut archetype_scores: Vec<_> = archetype_cards
        .iter()
        .map(|(archetype, card)| ArchetypeScore {
            archetype: *archetype,
            score: card.total,
        })
        .collect();
    archetype_scores.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.archetype.as_str().cmp(right.archetype.as_str()))
    });

    let primary = select_primary_archetype(&archetype_scores);
    let facet_cards = score_facets(features, primary.archetype);

    let mut facet_scores: Vec<_> = facet_cards
        .iter()
        .filter(|(_, card)| card.total > 0)
        .map(|(facet, card)| FacetScore {
            facet: *facet,
            score: card.total,
        })
        .collect();
    facet_scores.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.facet.as_str().cmp(right.facet.as_str()))
    });

    let second_score = archetype_scores
        .get(1)
        .map(|score| score.score)
        .unwrap_or(0);
    let primary_archetype = if primary.score >= 4 {
        primary.archetype
    } else {
        ArtifactArchetype::Unknown
    };
    let confidence = confidence_for(primary.score, second_score);
    let reasons = reasons_for(primary_archetype, &archetype_cards, &facet_scores);
    let facets = facet_scores
        .iter()
        .filter(|score| score.score >= 3)
        .map(|score| score.facet)
        .collect();

    ArtifactClassificationDebug {
        profile: ArtifactClassificationProfile {
            primary_archetype,
            facets,
            confidence,
            reasons,
        },
        archetype_scores,
        facet_scores,
    }
}

fn score_archetypes(features: &ArtifactFeatures) -> Vec<(ArtifactArchetype, ScoreCard)> {
    vec![
        (
            ArtifactArchetype::Conversation,
            score_conversation(features),
        ),
        (
            ArtifactArchetype::ProceduralNote,
            score_procedural(features),
        ),
        (ArtifactArchetype::ReferenceNote, score_reference(features)),
        (
            ArtifactArchetype::DefinitionNote,
            score_definition(features),
        ),
        (ArtifactArchetype::WorkingNote, score_working(features)),
        (ArtifactArchetype::JournalLog, score_journal(features)),
        (
            ArtifactArchetype::DashboardTemplate,
            score_dashboard(features),
        ),
        (ArtifactArchetype::Unknown, ScoreCard::default()),
    ]
}

fn score_facets(
    features: &ArtifactFeatures,
    primary_archetype: ArtifactArchetype,
) -> Vec<(ArtifactFacet, ScoreCard)> {
    vec![
        (
            ArtifactFacet::Technical,
            score_technical_facet(features, primary_archetype),
        ),
        (
            ArtifactFacet::Setup,
            score_setup_facet(features, primary_archetype),
        ),
        (
            ArtifactFacet::Project,
            score_project_facet(features, primary_archetype),
        ),
        (
            ArtifactFacet::Planning,
            score_planning_facet(features, primary_archetype),
        ),
        (
            ArtifactFacet::Personal,
            score_personal_facet(features, primary_archetype),
        ),
        (
            ArtifactFacet::Reference,
            score_reference_facet(features, primary_archetype),
        ),
        (
            ArtifactFacet::Scientific,
            score_scientific_facet(features, primary_archetype),
        ),
        (ArtifactFacet::Automation, score_automation_facet(features)),
    ]
}

fn score_conversation(features: &ArtifactFeatures) -> ScoreCard {
    let mut card = ScoreCard::default();
    if features.artifact_class == crate::storage::types::ArtifactClass::Conversation {
        card.add(12, "artifact is already classified as a conversation");
    }
    if matches!(
        features.source_type,
        crate::storage::types::SourceType::ChatGptExport
            | crate::storage::types::SourceType::ClaudeExport
            | crate::storage::types::SourceType::GrokExport
            | crate::storage::types::SourceType::GeminiTakeout
    ) {
        card.add(8, "source format is a chat export");
    }
    if features.participant_count >= 2 || features.unique_participant_role_count >= 2 {
        card.add(4, "multiple participants or participant roles are present");
    }
    if features.question_count >= 2 {
        card.add(2, "the artifact contains multiple questions");
    }
    if features.heading_count >= 4 {
        card.add(
            -2,
            "heavy heading structure looks more like a document than a chat",
        );
    }
    if features.has_document_type_definition || features.dataview_marker_count > 0 {
        card.add(
            -3,
            "note metadata or query syntax points away from a conversation",
        );
    }
    card
}

fn score_procedural(features: &ArtifactFeatures) -> ScoreCard {
    let mut card = ScoreCard::default();
    if features.title_setup_keyword_count > 0 {
        card.add(3, "title points directly at setup or procedural work");
    }
    if features.numbered_list_count >= 2 {
        card.add(6, "numbered steps suggest a procedure");
    }
    if features.imperative_line_count >= 3 {
        card.add(
            5,
            "multiple imperative instruction lines suggest a procedure",
        );
    }
    if features.setup_keyword_count >= 3 {
        card.add(5, "setup and configuration language is prominent");
    }
    if features.command_line_count >= 2 {
        card.add(4, "multiple command-like lines are present");
    }
    if features.config_line_count >= 2 {
        card.add(4, "multiple config-like lines are present");
    }
    if features.heading_count > 0 && features.setup_keyword_count > 0 {
        card.add(3, "section headings align with setup or usage content");
    }
    if features.imperative_line_count >= 2 && features.heading_count > 0 {
        card.add(
            2,
            "instructional headings and imperative lines reinforce a guide format",
        );
    }
    if features.artifact_class == crate::storage::types::ArtifactClass::Conversation {
        card.add(
            -5,
            "conversation artifacts should not be routed as procedures by default",
        );
    }
    if features.dataview_marker_count > 0 || features.templater_marker_count > 0 {
        card.add(
            -4,
            "automation syntax points toward a dashboard or template",
        );
    }
    card
}

fn score_reference(features: &ArtifactFeatures) -> ScoreCard {
    let mut card = ScoreCard::default();
    if features.artifact_class == crate::storage::types::ArtifactClass::Document {
        card.add(
            2,
            "document artifacts default toward reference unless stronger cues win",
        );
    }
    if features.task_count == 0 && features.numbered_list_count == 0 {
        card.add(
            2,
            "lack of tasks or procedures suggests a reference-style note",
        );
    }
    if features.definition_phrase_count > 0 {
        card.add(
            2,
            "definition-like phrasing overlaps with reference material",
        );
    }
    if features.char_count > 0 && features.char_count < 4_000 {
        card.add(
            2,
            "short focused documents often behave like reference notes",
        );
    }
    if features.heading_count >= 3 && features.char_count > 1_500 {
        card.add(
            2,
            "multi-section descriptive documents often behave like reference material",
        );
    }
    if features.imperative_line_count == 0 && features.heading_count > 0 {
        card.add(
            2,
            "descriptive sectioning without imperative steps suggests reference material",
        );
    }
    if features.has_document_type_definition {
        card.add(
            -4,
            "explicit definition metadata should route to the definition archetype",
        );
    }
    if features.has_document_type_meeting {
        card.add(
            -5,
            "explicit meeting metadata should route to active work or meeting-style notes",
        );
    }
    if features.numbered_list_count >= 2 || features.task_count > 0 {
        card.add(
            -3,
            "procedural or working-note structure is stronger than reference",
        );
    }
    if features.imperative_line_count >= 3 {
        card.add(
            -4,
            "imperative instruction flow is stronger than reference prose",
        );
    }
    card
}

fn score_definition(features: &ArtifactFeatures) -> ScoreCard {
    let mut card = ScoreCard::default();
    if features.has_document_type_definition {
        card.add(8, "document_type explicitly says definition");
    }
    if features.has_definition_tag {
        card.add(5, "tags explicitly mark this as a definition");
    }
    if features.title_looks_term_like {
        card.add(3, "title looks like a term or glossary entry");
    }
    if features.definition_phrase_count > 0 {
        card.add(5, "the body contains definitional phrasing");
    }
    if features.char_count > 0 && features.char_count < 2_500 {
        card.add(2, "short focused notes often match definition entries");
    }
    if features.heading_count >= 4 && features.char_count > 4_000 {
        card.add(
            -3,
            "long multi-section documents are broader than a single definition entry",
        );
    }
    if features.numbered_list_count >= 2 || features.task_count > 0 {
        card.add(
            -4,
            "task or step structure is atypical for a glossary entry",
        );
    }
    card
}

fn score_working(features: &ArtifactFeatures) -> ScoreCard {
    let mut card = ScoreCard::default();
    if features.has_document_type_meeting {
        card.add(8, "document_type explicitly marks this as a meeting note");
    }
    if features.title_project_keyword_count > 0 {
        card.add(2, "title points at project coordination or meeting work");
    }
    if features.title_project_keyword_count > 0 && features.planning_keyword_count >= 2 {
        card.add(
            3,
            "project or meeting titles paired with planning language suggest active work",
        );
    }
    if features.title_project_keyword_count > 0 && features.heading_count >= 2 {
        card.add(
            2,
            "project or meeting titles with structured sections suggest an active work note",
        );
    }
    if features.title_looks_date && features.title_project_keyword_count > 0 {
        card.add(
            3,
            "dated project titles often indicate meetings or work-log notes",
        );
    }
    if features.task_count >= 2 {
        card.add(4, "checkboxes or task lines suggest a working note");
    }
    if features.task_count > 0 && features.planning_keyword_count >= 2 {
        card.add(
            3,
            "tasks and planning language together suggest active work tracking",
        );
    }
    if features.planning_keyword_count >= 3 {
        card.add(4, "planning and decision language is prominent");
    }
    if features.project_keyword_count >= 3 {
        card.add(2, "project-oriented language is prominent");
    }
    if features.has_project_tag || features.has_document_type_concept {
        card.add(2, "metadata suggests project or concept tracking");
    }
    if features.numbered_list_count > 0 && features.task_count > 0 {
        card.add(
            2,
            "step lists and tasks often appear together in working notes",
        );
    }
    if features.metadata_property_keys.contains("status")
        || features.metadata_property_keys.contains("owner")
        || features.metadata_property_keys.contains("attendees")
        || features.metadata_property_keys.contains("meeting")
    {
        card.add(2, "metadata includes active work tracking fields");
    }
    if features.imperative_line_count >= 3 && features.task_count == 0 {
        card.add(
            -3,
            "instruction-heavy documents without task tracking are less likely to be working notes",
        );
    }
    if features.char_count > 5_000 && features.task_count < 2 {
        card.add(
            -2,
            "long documents without strong task structure are less likely to be working notes",
        );
    }
    if features.artifact_class == crate::storage::types::ArtifactClass::Conversation {
        card.add(
            -2,
            "conversation artifacts should not route to working notes without stronger cues",
        );
    }
    card
}

fn score_journal(features: &ArtifactFeatures) -> ScoreCard {
    let mut card = ScoreCard::default();
    if features.title_looks_date {
        card.add(7, "date-like titles strongly suggest a journal or log");
    }
    if features.has_journal_tag {
        card.add(4, "metadata tags point to journal or log content");
    }
    if features.journal_keyword_count >= 2 {
        card.add(4, "journal-style language is present");
    }
    if features.first_person_count >= 4 {
        card.add(2, "first-person language is prominent");
    }
    if features.command_line_count > 0 || features.dataview_marker_count > 0 {
        card.add(
            -3,
            "automation or code heavy notes are unlikely to be journals",
        );
    }
    card
}

fn score_dashboard(features: &ArtifactFeatures) -> ScoreCard {
    let mut card = ScoreCard::default();
    if features.has_document_type_dashboard {
        card.add(
            5,
            "document_type explicitly marks this as a dashboard or index note",
        );
    }
    if features.dataview_marker_count >= 2 {
        card.add(8, "dataview or query markers dominate the note");
    }
    if features.templater_marker_count >= 2 {
        card.add(8, "templater or automation markers dominate the note");
    }
    if features.title_contains_dashboard_like {
        card.add(4, "title looks like a dashboard, index, or home page");
    }
    if features.has_template_path_hint {
        card.add(3, "note metadata path hints at templates or scaffolding");
    }
    if features.has_dashboard_path_hint {
        card.add(3, "note metadata path hints at dashboards or index pages");
    }
    if features.title_contains_dashboard_like && features.char_count < 3_000 {
        card.add(2, "short hub-like titles often belong to navigation notes");
    }
    if features.wikilink_count >= 4 && features.char_count < 8_000 {
        card.add(2, "link-heavy, lower-prose notes often act as dashboards");
    }
    if features.artifact_class == crate::storage::types::ArtifactClass::Conversation {
        card.add(
            -6,
            "conversation artifacts should not route to dashboard/template",
        );
    }
    card
}

fn score_technical_facet(
    features: &ArtifactFeatures,
    primary_archetype: ArtifactArchetype,
) -> ScoreCard {
    let mut card = ScoreCard::default();
    if primary_archetype == ArtifactArchetype::Conversation {
        if features.title_technical_keyword_count > 0 {
            card.add(4, "title points directly at technical subject matter");
        }
        if features.command_line_count > 0 {
            card.add(4, "command lines indicate technical content");
        }
        if features.config_line_count > 0 {
            card.add(4, "config-style lines indicate technical content");
        }
        if features.code_fence_count > 0 {
            card.add(3, "code fences indicate technical content");
        }
        if features.technical_keyword_count >= 5 {
            card.add(
                2,
                "technical vocabulary is sustained across the conversation",
            );
        }
        return card;
    }
    if features.command_line_count > 0 {
        card.add(3, "command lines indicate technical content");
    }
    if features.config_line_count > 0 {
        card.add(3, "config-style lines indicate technical content");
    }
    if features.code_fence_count > 0 {
        card.add(2, "code fences indicate technical content");
    }
    if features.technical_keyword_count >= 3 {
        card.add(3, "technical vocabulary is prominent");
    }
    card
}

fn score_setup_facet(
    features: &ArtifactFeatures,
    primary_archetype: ArtifactArchetype,
) -> ScoreCard {
    let mut card = ScoreCard::default();
    if primary_archetype == ArtifactArchetype::Conversation {
        if features.title_setup_keyword_count > 0 {
            card.add(4, "title points at setup or troubleshooting intent");
        }
        if features.setup_keyword_count >= 4 {
            card.add(3, "setup language is sustained across the conversation");
        }
        if features.numbered_list_count > 0 {
            card.add(2, "numbered steps suggest guided setup");
        }
        if features.command_line_count > 0 || features.config_line_count > 0 {
            card.add(3, "commands or config details support a setup facet");
        }
        return card;
    }
    if features.setup_keyword_count >= 2 {
        card.add(5, "setup and configuration language is prominent");
    }
    if features.numbered_list_count > 0 {
        card.add(2, "numbered steps often appear in setup guides");
    }
    if features.command_line_count > 0 || features.config_line_count > 0 {
        card.add(2, "commands or config lines support a setup facet");
    }
    card
}

fn score_project_facet(
    features: &ArtifactFeatures,
    primary_archetype: ArtifactArchetype,
) -> ScoreCard {
    let mut card = ScoreCard::default();
    if primary_archetype == ArtifactArchetype::Conversation {
        if features.title_project_keyword_count > 0 {
            card.add(4, "title points directly at project work");
        }
        if features.has_project_tag || features.has_document_type_concept {
            card.add(4, "metadata points at project or concept work");
        }
        if features.project_keyword_count >= 4 {
            card.add(
                3,
                "project-oriented terms recur throughout the conversation",
            );
        }
        if features.project_keyword_count >= 2 && features.planning_keyword_count >= 3 {
            card.add(2, "project and planning language appear together");
        }
        return card;
    }
    if features.project_keyword_count >= 2 {
        card.add(4, "project-oriented terms are present");
    }
    if features.title_project_keyword_count > 0 {
        card.add(4, "title points directly at project work");
    }
    if features.has_project_tag || features.has_document_type_concept {
        card.add(4, "metadata points at project or concept work");
    }
    if features.planning_keyword_count >= 2 {
        card.add(2, "planning structure often accompanies project work");
    }
    card
}

fn score_planning_facet(
    features: &ArtifactFeatures,
    primary_archetype: ArtifactArchetype,
) -> ScoreCard {
    let mut card = ScoreCard::default();
    if primary_archetype == ArtifactArchetype::Conversation {
        if features.title_planning_keyword_count > 0 {
            card.add(4, "title points at planning or decision support");
        }
        if features.task_count > 0 {
            card.add(3, "task list structure points at planning work");
        }
        if features.planning_keyword_count >= 4 {
            card.add(3, "planning language is sustained across the conversation");
        }
        return card;
    }
    if features.task_count >= 2 {
        card.add(3, "task list structure points at planning work");
    }
    if features.planning_keyword_count >= 3 {
        card.add(4, "planning and decision language is present");
    }
    card
}

fn score_personal_facet(
    features: &ArtifactFeatures,
    primary_archetype: ArtifactArchetype,
) -> ScoreCard {
    let mut card = ScoreCard::default();
    if primary_archetype == ArtifactArchetype::JournalLog {
        card.add(4, "journal-style routing implies personal context");
    }
    if primary_archetype == ArtifactArchetype::Conversation {
        if features.title_personal_keyword_count > 0 {
            card.add(4, "title explicitly frames the topic as personal");
        }
        if features.first_person_count >= 12 {
            card.add(
                2,
                "first-person language is sustained throughout the conversation",
            );
        }
        return card;
    }
    if features.first_person_count >= 6 {
        card.add(3, "first-person language is prominent");
    }
    card
}

fn score_reference_facet(
    features: &ArtifactFeatures,
    primary_archetype: ArtifactArchetype,
) -> ScoreCard {
    let mut card = ScoreCard::default();
    if matches!(
        primary_archetype,
        ArtifactArchetype::ReferenceNote | ArtifactArchetype::DefinitionNote
    ) {
        card.add(4, "reference-oriented archetype implies a reference facet");
    }
    if features.definition_phrase_count > 0 {
        card.add(2, "definitional phrasing supports a reference facet");
    }
    card
}

fn score_scientific_facet(
    features: &ArtifactFeatures,
    primary_archetype: ArtifactArchetype,
) -> ScoreCard {
    let mut card = ScoreCard::default();
    if primary_archetype == ArtifactArchetype::Conversation {
        if features.title_scientific_keyword_count > 0 {
            card.add(4, "title points directly at scientific subject matter");
        }
        if features.scientific_keyword_count >= 4 {
            card.add(
                3,
                "scientific vocabulary is sustained across the conversation",
            );
        }
        return card;
    }
    if features.scientific_keyword_count >= 3 {
        card.add(4, "scientific vocabulary is prominent");
    }
    if matches!(
        primary_archetype,
        ArtifactArchetype::DefinitionNote | ArtifactArchetype::ReferenceNote
    ) && features.title_looks_term_like
        && features.scientific_keyword_count >= 1
    {
        card.add(
            2,
            "term-like title plus science vocabulary suggests a scientific note",
        );
    }
    if features.title_looks_term_like && features.scientific_keyword_count >= 1 {
        card.add(
            2,
            "term-like titles with science vocabulary suggest a scientific note",
        );
    }
    card
}

fn score_automation_facet(features: &ArtifactFeatures) -> ScoreCard {
    let mut card = ScoreCard::default();
    if features.dataview_marker_count > 0 {
        card.add(5, "dataview markers indicate automation or query behavior");
    }
    if features.templater_marker_count > 0 {
        card.add(5, "templater markers indicate automation or scaffolding");
    }
    card
}

fn confidence_for(primary_score: i32, second_score: i32) -> f32 {
    if primary_score <= 0 {
        return 0.15;
    }
    let gap = (primary_score - second_score).max(0) as f32;
    let score_component = (primary_score as f32 / 18.0).min(0.4);
    let gap_component = (gap / 12.0).min(0.25);
    (0.3 + score_component + gap_component).min(0.95)
}

fn reasons_for(
    archetype: ArtifactArchetype,
    archetype_cards: &[(ArtifactArchetype, ScoreCard)],
    facet_scores: &[FacetScore],
) -> Vec<String> {
    let mut reasons = Vec::new();
    if let Some((_, card)) = archetype_cards
        .iter()
        .find(|(candidate, _)| *candidate == archetype)
    {
        let mut positives: Vec<_> = card
            .contributions
            .iter()
            .filter(|contribution| contribution.score > 0)
            .cloned()
            .collect();
        positives.sort_by(|left, right| right.score.cmp(&left.score));
        for contribution in positives.into_iter().take(3) {
            reasons.push(contribution.reason.to_string());
        }
    }
    for facet in facet_scores.iter().take(2) {
        if facet.score >= 4 {
            reasons.push(format!(
                "facet {} scored strongly ({})",
                facet.facet.as_str(),
                facet.score
            ));
        }
    }
    reasons.truncate(5);
    reasons
}

fn select_primary_archetype(archetype_scores: &[ArchetypeScore]) -> ArchetypeScore {
    archetype_scores.first().cloned().unwrap_or(ArchetypeScore {
        archetype: ArtifactArchetype::Unknown,
        score: 0,
    })
}
