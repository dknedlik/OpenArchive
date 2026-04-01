use std::collections::HashSet;

use crate::storage::types::RetrievalIntent;

use super::*;

pub(crate) fn cleanup_artifact_processor_output(
    input: &ArtifactProcessorInput,
    mut output: ArtifactProcessorOutput,
) -> ArtifactProcessorOutput {
    let policy = extraction_policy_for(input);
    output.memories = dedupe_memory_outputs(output.memories);
    output.memories =
        filter_memory_outputs_for_archetype(output.memories, policy.profile.primary_archetype);
    output.entities = dedupe_entity_outputs(output.entities);
    output.relationships = sanitize_relationship_outputs(output.relationships, &output.entities);
    output.retrieval_intents = dedupe_retrieval_intents(output.retrieval_intents);
    output.retrieval_intents = filter_retrieval_intents_for_archetype(
        output.retrieval_intents,
        policy.profile.primary_archetype,
    );
    if output.classifications.len() > policy.classification_limit {
        output.classifications.truncate(policy.classification_limit);
    }
    if output.memories.len() > policy.memory_limit {
        output.memories.truncate(policy.memory_limit);
    }
    if output.entities.len() > policy.entity_limit {
        output.entities.truncate(policy.entity_limit);
    }
    if output.relationships.len() > policy.relationship_limit {
        output.relationships.truncate(policy.relationship_limit);
    }
    if output.retrieval_intents.len() > policy.retrieval_limit {
        output.retrieval_intents.truncate(policy.retrieval_limit);
    }
    output
}

fn filter_memory_outputs_for_archetype(
    memories: Vec<MemoryOutput>,
    archetype: ArtifactArchetype,
) -> Vec<MemoryOutput> {
    memories
        .into_iter()
        .filter(|memory| should_keep_memory_output(memory, archetype))
        .collect()
}

fn should_keep_memory_output(memory: &MemoryOutput, archetype: ArtifactArchetype) -> bool {
    if !matches!(
        archetype,
        ArtifactArchetype::ProceduralNote
            | ArtifactArchetype::ReferenceNote
            | ArtifactArchetype::DefinitionNote
            | ArtifactArchetype::WorkingNote
            | ArtifactArchetype::DashboardTemplate
            | ArtifactArchetype::JournalLog
    ) {
        return true;
    }

    let normalized = normalize_merge_text(&format!(
        "{} {}",
        memory.title.as_deref().unwrap_or_default(),
        memory.body_text
    ));
    if looks_like_low_value_document_memory(&normalized) {
        return false;
    }
    if archetype == ArtifactArchetype::DashboardTemplate
        && looks_like_low_value_dashboard_navigation_memory(&normalized)
    {
        return false;
    }
    true
}

fn filter_retrieval_intents_for_archetype(
    intents: Vec<RetrievalIntent>,
    archetype: ArtifactArchetype,
) -> Vec<RetrievalIntent> {
    if !matches!(
        archetype,
        ArtifactArchetype::ProceduralNote
            | ArtifactArchetype::ReferenceNote
            | ArtifactArchetype::DefinitionNote
            | ArtifactArchetype::DashboardTemplate
    ) {
        return intents;
    }

    intents
        .into_iter()
        .filter(|intent| {
            let normalized =
                normalize_merge_text(&format!("{} {}", intent.question, intent.query_text));
            !looks_like_low_value_document_memory(&normalized)
        })
        .collect()
}

fn looks_like_low_value_document_memory(normalized: &str) -> bool {
    const LOW_VALUE_PATTERNS: &[&str] = &[
        "note structure",
        "section structure",
        "document type",
        "note title",
        "note metadata",
        "navigation link",
        "dashboard link",
        "journal dashboard link",
        "linked to journal dashboard",
        "linked to dashboard",
        "link to journal dashboard",
        "link to dashboard",
        "creation and modification date",
        "creation date",
        "modification date",
        "created and modified",
        "note tag",
        "imported tag",
        "imported alias",
        "journal tag",
        "tagged as",
        "concept board association",
        "association with concept board",
        "file metadata",
        "placeholder text",
        "example significant event text",
    ];

    LOW_VALUE_PATTERNS
        .iter()
        .any(|pattern| normalized.contains(pattern))
}

fn looks_like_low_value_dashboard_navigation_memory(normalized: &str) -> bool {
    const DASHBOARD_NAVIGATION_PATTERNS: &[&str] = &[
        "navigation structure",
        "dashboard section",
        "display limit",
        "project navigation",
        "recent item",
        "dashboard home",
        "home dashboard",
        "index page",
        "project index",
        "index note",
        "latest entry",
    ];

    DASHBOARD_NAVIGATION_PATTERNS
        .iter()
        .any(|pattern| normalized.contains(pattern))
}

fn dedupe_memory_outputs(memories: Vec<MemoryOutput>) -> Vec<MemoryOutput> {
    let mut deduped = Vec::<MemoryOutput>::new();
    for memory in memories {
        if let Some(existing) = deduped
            .iter_mut()
            .find(|existing| should_merge_memory_outputs(existing, &memory))
        {
            merge_memory_outputs(existing, &memory);
        } else {
            deduped.push(memory);
        }
    }
    deduped
}

fn dedupe_entity_outputs(entities: Vec<EntityOutput>) -> Vec<EntityOutput> {
    let mut deduped = Vec::<EntityOutput>::new();
    for entity in entities {
        if let Some(existing) = deduped
            .iter_mut()
            .find(|existing| should_merge_entity_outputs(existing, &entity))
        {
            merge_entity_outputs(existing, &entity);
        } else {
            deduped.push(entity);
        }
    }
    deduped
}

fn sanitize_relationship_outputs(
    relationships: Vec<RelationshipOutput>,
    entities: &[EntityOutput],
) -> Vec<RelationshipOutput> {
    let entity_keys: HashSet<&str> = entities
        .iter()
        .map(|entity| entity.entity_key.as_str())
        .collect();
    let mut deduped = Vec::<RelationshipOutput>::new();
    for relationship in relationships {
        if relationship.subject_key == relationship.object_key {
            continue;
        }
        if !entity_keys.contains(relationship.subject_key.as_str())
            || !entity_keys.contains(relationship.object_key.as_str())
        {
            continue;
        }
        if !matches!(
            relationship.confidence_label.as_str(),
            "low" | "medium" | "high"
        ) {
            continue;
        }
        if let Some(existing) = deduped.iter_mut().find(|existing| {
            existing.relationship_type == relationship.relationship_type
                && existing.subject_key == relationship.subject_key
                && existing.object_key == relationship.object_key
        }) {
            merge_relationship_outputs(existing, &relationship);
        } else {
            deduped.push(relationship);
        }
    }
    deduped
}

fn dedupe_retrieval_intents(intents: Vec<RetrievalIntent>) -> Vec<RetrievalIntent> {
    let mut deduped = Vec::<RetrievalIntent>::new();
    for intent in intents {
        if let Some(existing) = deduped
            .iter_mut()
            .find(|existing| should_merge_retrieval_intents(existing, &intent))
        {
            merge_retrieval_intents(existing, &intent);
        } else {
            deduped.push(intent);
        }
    }
    deduped
}

fn should_merge_memory_outputs(left: &MemoryOutput, right: &MemoryOutput) -> bool {
    if left.candidate_key == right.candidate_key {
        return true;
    }
    if left.memory_type != right.memory_type
        || left.memory_scope != right.memory_scope
        || left.memory_scope_value != right.memory_scope_value
    {
        return false;
    }

    let left_title = normalize_merge_text(left.title.as_deref().unwrap_or_default());
    let right_title = normalize_merge_text(right.title.as_deref().unwrap_or_default());
    if !left_title.is_empty() && left_title == right_title {
        return text_overlap_score(&left.body_text, &right.body_text) >= 0.82;
    }

    text_overlap_score(&left.body_text, &right.body_text) >= 0.9
}

fn should_merge_entity_outputs(left: &EntityOutput, right: &EntityOutput) -> bool {
    if normalize_merge_text(&left.entity_key) == normalize_merge_text(&right.entity_key) {
        return true;
    }
    normalize_merge_text(&left.display_name) == normalize_merge_text(&right.display_name)
}

fn should_merge_retrieval_intents(left: &RetrievalIntent, right: &RetrievalIntent) -> bool {
    if left.intent_type != right.intent_type {
        return false;
    }
    let left_query = normalize_merge_text(&left.query_text);
    let right_query = normalize_merge_text(&right.query_text);
    let left_question = normalize_merge_text(&left.question);
    let right_question = normalize_merge_text(&right.question);
    left_query == right_query
        || (!left_question.is_empty() && left_question == right_question)
        || text_overlap_score(&left.query_text, &right.query_text) >= 0.88
}

fn merge_memory_outputs(target: &mut MemoryOutput, incoming: &MemoryOutput) {
    target.title = prefer_richer_optional_text(target.title.take(), incoming.title.clone());
    if incoming.body_text.len() > target.body_text.len() {
        target.body_text = incoming.body_text.clone();
    }
    extend_unique_strings(
        &mut target.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
}

fn merge_entity_outputs(target: &mut EntityOutput, incoming: &EntityOutput) {
    if incoming.display_name.len() > target.display_name.len() {
        target.display_name = incoming.display_name.clone();
    }
    if incoming.entity_type.len() > target.entity_type.len() {
        target.entity_type = incoming.entity_type.clone();
    }
    extend_unique_strings(
        &mut target.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
}

fn merge_relationship_outputs(target: &mut RelationshipOutput, incoming: &RelationshipOutput) {
    target.title = prefer_richer_optional_text(target.title.take(), incoming.title.clone());
    if incoming.body_text.len() > target.body_text.len() {
        target.body_text = incoming.body_text.clone();
    }
    if confidence_rank(&incoming.confidence_label) > confidence_rank(&target.confidence_label) {
        target.confidence_label = incoming.confidence_label.clone();
    }
    extend_unique_strings(
        &mut target.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
}

fn merge_retrieval_intents(target: &mut RetrievalIntent, incoming: &RetrievalIntent) {
    if incoming.question.len() > target.question.len() {
        target.question = incoming.question.clone();
    }
    if incoming.query_text.len() > target.query_text.len() {
        target.query_text = incoming.query_text.clone();
    }
    extend_unique_strings(
        &mut target.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
}

fn extend_unique_strings(target: &mut Vec<String>, values: &[String]) {
    for value in values {
        if !target.contains(value) {
            target.push(value.clone());
        }
    }
}

fn normalize_merge_text(value: &str) -> String {
    normalize_candidate_key_text(value)
        .split_whitespace()
        .map(singularize_merge_token)
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn singularize_merge_token(token: &str) -> String {
    if token.len() > 4 && token.ends_with("ies") {
        format!("{}y", &token[..token.len() - 3])
    } else if token.len() > 4 && token.ends_with('s') && !token.ends_with("ss") {
        token[..token.len() - 1].to_string()
    } else {
        token.to_string()
    }
}

fn text_overlap_score(left: &str, right: &str) -> f32 {
    let left = normalize_merge_text(left);
    let right = normalize_merge_text(right);
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    if left == right || left.contains(&right) || right.contains(&left) {
        return 1.0;
    }

    let left_tokens: HashSet<&str> = left.split_whitespace().collect();
    let right_tokens: HashSet<&str> = right.split_whitespace().collect();
    if left_tokens.is_empty() || right_tokens.is_empty() {
        return 0.0;
    }
    let overlap = left_tokens.intersection(&right_tokens).count() as f32;
    let smaller = left_tokens.len().min(right_tokens.len()) as f32;
    overlap / smaller
}

pub(crate) fn canonicalize_memory_type(raw: &str, title: &str, body_text: &str) -> String {
    let raw = normalize_candidate_key_text(raw);
    let title = normalize_candidate_key_text(title);
    let body = normalize_candidate_key_text(body_text);
    let combined = format!("{title} {body}");

    if matches!(
        raw.as_str(),
        "personal fact"
            | "personal_fact"
            | "identity fact"
            | "identity_fact"
            | "fact"
            | "experience"
            | "biographical fact"
            | "health fact"
    ) {
        return "personal_fact".to_string();
    }
    if matches!(raw.as_str(), "preference" | "preferences") {
        return "preference".to_string();
    }
    if matches!(
        raw.as_str(),
        "ongoing task"
            | "ongoing_task"
            | "ongoing state"
            | "ongoing_state"
            | "current state"
            | "task"
    ) {
        return "ongoing_state".to_string();
    }
    if matches!(raw.as_str(), "project fact" | "project_fact" | "decision") {
        return "project_fact".to_string();
    }
    if matches!(
        raw.as_str(),
        "reference" | "reference material" | "reference_material" | "skill"
    ) {
        return "reference".to_string();
    }
    if matches!(
        raw.as_str(),
        "reference_fact"
            | "reference fact"
            | "definition"
            | "alias"
            | "comparison"
            | "formula"
            | "glossary_term"
            | "glossary term"
            | "cross_reference"
            | "cross reference"
            | "procedure_step"
            | "procedure step"
            | "requirement"
            | "constraint"
            | "configuration"
            | "command"
            | "navigation"
            | "display_rule"
            | "display rule"
            | "template_rule"
            | "template rule"
    ) {
        return "reference".to_string();
    }
    if matches!(
        raw.as_str(),
        "decision"
            | "action_item"
            | "action item"
            | "status"
            | "owner"
            | "open_question"
            | "open question"
            | "project_fact"
            | "project fact"
            | "attendee"
    ) {
        return "project_fact".to_string();
    }
    if matches!(
        raw.as_str(),
        "state_change"
            | "state change"
            | "reflection"
            | "event"
            | "ongoing_state"
            | "ongoing state"
    ) {
        return "ongoing_state".to_string();
    }

    if combined.contains("user weigh")
        || combined.contains("body weight")
        || combined.contains("weight history")
        || combined.contains("user age")
        || combined.contains("cholesterol")
        || combined.contains("ldl")
        || combined.contains("apob")
        || combined.contains("lp a")
        || combined.contains("egfr")
        || combined.contains("tbi")
        || combined.contains("concussion")
        || combined.contains("brain hemorrhage")
        || combined.contains("hemorrhage")
        || combined.contains("lung nodule")
        || combined.contains("smoking history")
        || combined.contains("pack year")
        || combined.contains("fracture")
        || combined.contains("broken back")
        || combined.contains("broke my back")
        || combined.contains("injur")
        || combined.contains("elite cyclist")
        || combined.contains("cycling history")
        || combined.contains("racing year")
        || combined.contains("university of")
        || combined.contains("lab result")
    {
        return "personal_fact".to_string();
    }

    if combined.contains("prefer")
        || combined.contains("avoid")
        || combined.contains("likes ")
        || combined.contains("dislike")
        || combined.contains("favorite")
        || combined.contains("prefers ")
    {
        return "preference".to_string();
    }

    if combined.contains("protocol")
        || combined.contains("plan")
        || combined.contains("schedule")
        || combined.contains("rotation")
        || combined.contains("transition")
        || combined.contains("trial")
        || combined.contains("current ")
        || combined.contains("currently ")
        || combined.contains("taking ")
        || combined.contains("supply")
        || combined.contains("dose")
        || combined.contains("dosing")
        || combined.contains("arriv")
        || combined.contains("week")
        || combined.contains("daily ")
    {
        return "ongoing_state".to_string();
    }

    if combined.contains("program")
        || combined.contains("system")
        || combined.contains("workflow")
        || combined.contains("architecture")
        || combined.contains("clean engine")
        || combined.contains("project")
        || combined.contains("decision")
        || combined.contains("should ")
        || combined.contains("must ")
    {
        return "project_fact".to_string();
    }

    if combined.contains("reference")
        || combined.contains("cheatsheet")
        || combined.contains("cheat sheet")
        || combined.contains("script")
        || combined.contains("template")
        || combined.contains("guide")
    {
        return "reference".to_string();
    }

    match raw.as_str() {
        "" => "personal_fact".to_string(),
        other if other.contains("preference") => "preference".to_string(),
        other if other.contains("personal") || other.contains("identity") => {
            "personal_fact".to_string()
        }
        other if other.contains("project") || other.contains("decision") => {
            "project_fact".to_string()
        }
        other if other.contains("ongoing") || other.contains("state") || other.contains("task") => {
            "ongoing_state".to_string()
        }
        other if other.contains("reference") => "reference".to_string(),
        _ => "personal_fact".to_string(),
    }
}

pub(crate) fn canonicalize_memory_type_for_input(
    input: &ArtifactProcessorInput,
    raw: &str,
    title: &str,
    body_text: &str,
) -> String {
    let normalized = canonicalize_memory_type(raw, title, body_text);
    if !raw.trim().is_empty() {
        return normalized;
    }

    let policy = extraction_policy_for(input);
    let archetype = policy.profile.primary_archetype;
    let combined = normalize_candidate_key_text(&format!("{title} {body_text}"));
    if memory_looks_personal(&combined)
        || memory_looks_preference(&combined)
        || memory_looks_ongoing(&combined)
        || memory_looks_reference(&combined)
    {
        return normalized;
    }
    if matches!(
        archetype,
        ArtifactArchetype::Conversation
            | ArtifactArchetype::WorkingNote
            | ArtifactArchetype::Unknown
    ) && memory_looks_project(&combined)
    {
        return normalized;
    }
    if archetype == ArtifactArchetype::DashboardTemplate
        && policy.profile.facets.contains(&ArtifactFacet::Project)
        && memory_looks_project(&combined)
    {
        return "project_fact".to_string();
    }

    match archetype {
        ArtifactArchetype::ProceduralNote
        | ArtifactArchetype::ReferenceNote
        | ArtifactArchetype::DefinitionNote
        | ArtifactArchetype::DashboardTemplate => "reference".to_string(),
        ArtifactArchetype::WorkingNote => "project_fact".to_string(),
        ArtifactArchetype::JournalLog => "ongoing_state".to_string(),
        ArtifactArchetype::Conversation | ArtifactArchetype::Unknown => normalized,
    }
}

fn memory_looks_personal(combined: &str) -> bool {
    combined.contains("user weigh")
        || combined.contains("body weight")
        || combined.contains("weight history")
        || combined.contains("user age")
        || combined.contains("cholesterol")
        || combined.contains("ldl")
        || combined.contains("apob")
        || combined.contains("lp a")
        || combined.contains("egfr")
        || combined.contains("tbi")
        || combined.contains("concussion")
        || combined.contains("brain hemorrhage")
        || combined.contains("hemorrhage")
        || combined.contains("lung nodule")
        || combined.contains("smoking history")
        || combined.contains("pack year")
        || combined.contains("fracture")
        || combined.contains("broken back")
        || combined.contains("broke my back")
        || combined.contains("injur")
        || combined.contains("elite cyclist")
        || combined.contains("cycling history")
        || combined.contains("racing year")
        || combined.contains("university of")
        || combined.contains("lab result")
}

fn memory_looks_preference(combined: &str) -> bool {
    combined.contains("prefer")
        || combined.contains("avoid")
        || combined.contains("likes ")
        || combined.contains("dislike")
        || combined.contains("favorite")
        || combined.contains("prefers ")
}

fn memory_looks_ongoing(combined: &str) -> bool {
    combined.contains("protocol")
        || combined.contains("current plan")
        || combined.contains("planned")
        || combined.contains("schedule")
        || combined.contains("rotation")
        || combined.contains("transition")
        || combined.contains("trial")
        || combined.contains("current ")
        || combined.contains("currently ")
        || combined.contains("taking ")
        || combined.contains("supply")
        || combined.contains("dose")
        || combined.contains("dosing")
        || combined.contains("arriv")
        || combined.contains("week")
        || combined.contains("daily ")
}

fn memory_looks_project(combined: &str) -> bool {
    combined.contains("program")
        || combined.contains("system")
        || combined.contains("workflow")
        || combined.contains("architecture")
        || combined.contains("clean engine")
        || combined.contains("project")
        || combined.contains("decision")
        || combined.contains("should ")
        || combined.contains("must ")
}

fn memory_looks_reference(combined: &str) -> bool {
    combined.contains("reference")
        || combined.contains("cheatsheet")
        || combined.contains("cheat sheet")
        || combined.contains("script")
        || combined.contains("template")
        || combined.contains("guide")
}

pub(crate) fn canonicalize_entity_type(raw: &str) -> String {
    let raw = normalize_candidate_key_text(raw);
    match raw.as_str() {
        "brand" | "organization brand" => "brand".to_string(),
        "organization" | "company" | "retailer" | "store" => "organization".to_string(),
        "food product" | "food product or source" | "food source" | "product or ingredient" => {
            "food_product".to_string()
        }
        "supplement form" | "supplement" => "supplement_form".to_string(),
        "laboratory marker" | "biomarker" => "biomarker".to_string(),
        "exercise program" | "training type" | "exercise type" => "training_type".to_string(),
        "medication" | "medication or brand name mention" => "medication".to_string(),
        "quality standard" => "quality_standard".to_string(),
        "" => "entity".to_string(),
        _ => raw.replace(' ', "_"),
    }
}

pub(crate) fn prefer_richer_optional_text(
    current: Option<String>,
    incoming: Option<String>,
) -> Option<String> {
    match (current, incoming) {
        (Some(current), Some(incoming)) => {
            if incoming.len() > current.len() {
                Some(incoming)
            } else {
                Some(current)
            }
        }
        (Some(current), None) => Some(current),
        (None, Some(incoming)) => Some(incoming),
        (None, None) => None,
    }
}

pub(crate) fn confidence_rank(label: &str) -> i32 {
    match label {
        "high" => 3,
        "medium" => 2,
        "low" => 1,
        _ => 0,
    }
}

pub(crate) fn normalize_optional_text(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

pub(crate) fn normalize_candidate_key_text(value: &str) -> String {
    let mut normalized = String::with_capacity(value.len());
    let mut previous_was_space = true;

    for ch in value.chars().flat_map(char::to_lowercase) {
        let ch = if ch.is_ascii_alphanumeric() || ch.is_whitespace() {
            ch
        } else {
            ' '
        };
        if ch.is_whitespace() {
            if !previous_was_space {
                normalized.push(' ');
                previous_was_space = true;
            }
        } else {
            normalized.push(ch);
            previous_was_space = false;
        }
    }

    normalized.trim().to_string()
}
