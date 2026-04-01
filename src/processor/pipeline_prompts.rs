use std::collections::HashMap;

use serde::Serialize;

use crate::storage::types::ArtifactClass;

use super::*;

pub(crate) fn build_conversation_user_prompt(
    input: &ArtifactProcessorInput,
) -> Result<String, ProcessorError> {
    #[derive(Serialize)]
    struct PromptSegment<'a> {
        evidence_ref: String,
        participant_role: &'a str,
        text: &'a str,
    }

    let prompt_segments: Vec<_> = input
        .segments
        .iter()
        .enumerate()
        .map(|(index, segment)| PromptSegment {
            evidence_ref: segment_alias(index),
            participant_role: segment
                .participant_role
                .map(|role| role.as_str())
                .unwrap_or("unknown"),
            text: segment.text_content.as_str(),
        })
        .collect();
    let artifact_profile_guidance = build_artifact_profile_guidance(input);

    let segments_json = serde_json::to_string_pretty(&prompt_segments)
        .map_err(|source| ProcessorError::SerializePrompt { source })?;
    Ok(format!(
        "Input artifact:\n\
         artifact_class: conversation\n\
         artifact_id: {artifact_id}\n\
         source_type: {source_type}\n\
         title: {title}\n\
         \n\
         segments:\n\
         {segments_json}\n\
         {artifact_profile_guidance}\
         \n\
         Output guidance:\n\
         - summary: 2-4 compact sentences covering the main topics discussed\n\
         - summary is not a substitute for memories; memories capture discrete durable facts for retrieval, while the summary is a narrative synthesis\n\
         - do not mirror the summary into memories or the memories into the summary mechanically\n\
         - if a durable fact could answer a later retrieval question on its own, emit it as a memory whether or not the summary also mentions it\n\
         - classifications: 1-3 covering the major domains of the conversation\n\
         - classifications are broad topical labels only; do not use them instead of memories for durable facts\n\
         - memories: extract every durable personal fact, preference, project fact, ongoing state, or reusable reference that could be independently retrieved later. A long or rich conversation should produce many memories, not a summary of a few. Categories to look for: preferences, history, decisions made, conclusions reached, ongoing state, goals, constraints, health and medical context, biographical facts, and named workflows or systems.\n\
         - choose the closest memory_type from the schema; use the broad available categories and do not invent narrower domain-specific subtypes\n\
         - biographical and health history belongs in durable memories, not only in summary prose\n\
         - entities: include all explicit named people, projects, systems, organizations, and domain-specific proper nouns with durable retrieval value\n\
         - relationships: include explicit supported links between emitted entities or durable facts\n\
         - retrieval_intents: ask archive-only follow-up questions when duplicate detection, prior-state matching, or contradiction checks matter\n\
         - importance: be conservative\n\
         - evidence_segment_ids must use only the evidence_ref values shown in segments\n\
         - allowed evidence_ref values for this artifact: {allowed_refs_json}\n\
         - copy evidence_ref values exactly as shown, for example evidence_ref_1 and evidence_ref_2\n\
         - never invent, abbreviate, transform, or combine evidence refs\n\
         - cite every segment that directly supports each derived object, not just the primary one\n\
         - memory_scope_value must be {artifact_id}\n\
         ",
        artifact_id = input.artifact_id,
        source_type = input.source_type.as_str(),
        title = input.title.as_deref().unwrap_or(""),
        segments_json = segments_json,
        artifact_profile_guidance = artifact_profile_guidance,
        allowed_refs_json = serde_json::to_string(&allowed_artifact_evidence_refs(input))
            .map_err(|source| ProcessorError::SerializePrompt { source })?,
    ))
}

pub(crate) fn build_document_user_prompt(
    input: &ArtifactProcessorInput,
) -> Result<String, ProcessorError> {
    #[derive(Serialize)]
    struct PromptSegment<'a> {
        evidence_ref: String,
        text: &'a str,
    }

    let prompt_segments: Vec<_> = input
        .segments
        .iter()
        .enumerate()
        .map(|(index, segment)| PromptSegment {
            evidence_ref: segment_alias(index),
            text: segment.text_content.as_str(),
        })
        .collect();

    let segments_json = serde_json::to_string_pretty(&prompt_segments)
        .map_err(|source| ProcessorError::SerializePrompt { source })?;
    let imported_note_metadata_json = input
        .imported_note_metadata
        .as_ref()
        .map(serde_json::to_string_pretty)
        .transpose()
        .map_err(|source| ProcessorError::SerializePrompt { source })?;
    let imported_note_metadata_section = imported_note_metadata_json
        .map(|json| format!("\nimported_note_metadata:\n{json}\n"))
        .unwrap_or_default();
    let artifact_profile_guidance = build_artifact_profile_guidance(input);

    Ok(format!(
        "Input artifact:\n\
         artifact_class: document\n\
         artifact_id: {artifact_id}\n\
         source_type: {source_type}\n\
         title: {title}\n\
         \n\
         segments:\n\
         {segments_json}\n\
         {imported_note_metadata_section}\
         {artifact_profile_guidance}\
         \n\
         Output guidance:\n\
         - summarize the document, not a dialogue; capture its purpose, key claims, decisions, action items, and durable references\n\
         - first infer what kind of document this is from the text itself, such as meeting notes, call notes, work log, research notes, design notes, journal entry, or general reference material, and adapt extraction accordingly\n\
         - imported_note_metadata, when present, is canonical source structure from the note system and should be used as explicit context rather than guessed from prose\n\
         - treat imported tags as user-authored classification hints, imported aliases as alternate names, and imported note links as explicit references between notes; do not overstate them into stronger semantic claims unless the text supports that claim\n\
         - properties can supply durable structured context such as status, owners, dates, projects, and aliases; use them when supported, but do not let a weak property override the actual document text\n\
         - summary is not a substitute for memories; memories should stay atomic and independently retrievable later\n\
         - classifications should capture document form and durable topical lenses, not one-off details\n\
         - memories: extract every durable fact, decision, commitment, requirement, constraint, ongoing state, action item owner, reference detail, or background context that could matter in future retrieval\n\
         - if the document appears to be meeting notes or a call log, treat attendees, speakers, owners, discussed systems, and recorded decisions as likely entity and relationship candidates when explicitly supported\n\
         - entities: include explicit named people, teams, organizations, projects, systems, repositories, vendors, and domain-specific proper nouns with likely future retrieval value\n\
         - relationships: include only explicit or strongly supported links between emitted entities\n\
         - prefer conservative predicates; when support comes mainly from metadata, tags, aliases, note links, or shorthand descriptors, use weaker relations such as mentions, references, uses, depends_on, or associated_with instead of stronger claims like owns, controls, authored_by, or reports_to\n\
         - do not infer ownership, authorship, or organizational control from note proximity, note links, tags, or terse labels alone\n\
         - retrieval_intents: ask archive-only follow-up questions that would help connect this document to prior decisions, prior states, duplicate notes, or related entities\n\
         - importance: be conservative, but documents with durable project context, commitments, or personal history can still be high value\n\
         - evidence_segment_ids must use only the evidence_ref values shown in segments\n\
         - allowed evidence_ref values for this artifact: {allowed_refs_json}\n\
         - copy evidence_ref values exactly as shown, for example evidence_ref_1 and evidence_ref_2\n\
         - never invent, abbreviate, transform, or combine evidence refs\n\
         - cite every segment that directly supports each derived object, not just the primary one\n\
         - memory_scope_value must be {artifact_id}\n\
         ",
        artifact_id = input.artifact_id,
        source_type = input.source_type.as_str(),
        title = input.title.as_deref().unwrap_or(""),
        segments_json = segments_json,
        imported_note_metadata_section = imported_note_metadata_section,
        artifact_profile_guidance = artifact_profile_guidance,
        allowed_refs_json = serde_json::to_string(&allowed_artifact_evidence_refs(input))
            .map_err(|source| ProcessorError::SerializePrompt { source })?,
    ))
}

fn build_artifact_profile_guidance(input: &ArtifactProcessorInput) -> String {
    let policy = extraction_policy_for(input);
    let profile = policy.profile.clone();
    if profile.primary_archetype == ArtifactArchetype::Unknown && profile.confidence < 0.45 {
        return String::new();
    }

    let mut lines = vec![
        "\nartifact_profile_hint:".to_string(),
        format!(
            "- inferred_primary_archetype: {} (confidence {:.2})",
            profile.primary_archetype.as_str(),
            profile.confidence
        ),
    ];
    if !profile.facets.is_empty() {
        lines.push(format!(
            "- inferred_facets: {}",
            profile
                .facets
                .iter()
                .map(|facet| facet.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !profile.reasons.is_empty() {
        lines.push(format!("- reasons: {}", profile.reasons.join("; ")));
    }
    lines.push(
        "- treat this as a routing hint only; direct evidence in the artifact always wins"
            .to_string(),
    );
    lines.extend(policy.prompt_guidance_lines());
    lines.extend(archetype_hint_lines(profile.primary_archetype));
    lines.join("\n") + "\n"
}

fn archetype_hint_lines(archetype: ArtifactArchetype) -> Vec<String> {
    match archetype {
        ArtifactArchetype::Conversation => vec![
            "- if this is a conversation, preserve the flow of questions, answers, decisions, and open issues rather than flattening it into a reference note".to_string(),
        ],
        ArtifactArchetype::ProceduralNote => vec![
            "- if this is a procedural note, prioritize prerequisites, ordered steps, commands, configuration points, constraints, and expected outcomes".to_string(),
            "- avoid turning every heading into a memory when the note is mostly instructions".to_string(),
            "- do not emit memories for note metadata, navigation links, dates, or section scaffolding unless they directly affect execution of the procedure".to_string(),
        ],
        ArtifactArchetype::ReferenceNote => vec![
            "- if this is a reference note, favor stable facts, concise definitions, and named concepts over transient scaffolding".to_string(),
            "- do not emit memories for note titles, tags, aliases, backlinks, or navigation structure unless the document is specifically about those metadata features".to_string(),
        ],
        ArtifactArchetype::DefinitionNote => vec![
            "- if this is a definition note, focus on the core definition, aliases, and closely related entities; avoid low-value memories about empty sections or placeholders".to_string(),
        ],
        ArtifactArchetype::WorkingNote => vec![
            "- if this is a working note, prioritize decisions, plans, tasks, owners, milestones, and open questions over generic topical summary".to_string(),
        ],
        ArtifactArchetype::JournalLog => vec![
            "- if this is a journal or log, preserve chronology, lived state, reflections, and changes over time rather than forcing it into a technical or reference frame".to_string(),
        ],
        ArtifactArchetype::DashboardTemplate => vec![
            "- if this is a dashboard, index, or template note, summarize its purpose and important destinations or reusable structure".to_string(),
            "- avoid emitting repetitive low-value memories about placeholder text, navigation chrome, dataview snippets, or boilerplate query helpers unless those mechanics are the document's purpose".to_string(),
        ],
        ArtifactArchetype::Unknown => Vec::new(),
    }
}

pub(crate) fn should_shape_artifact_input(input: &ArtifactProcessorInput) -> bool {
    if input.artifact_class == ArtifactClass::Document {
        return false;
    }

    let char_count: usize = input
        .segments
        .iter()
        .map(|segment| segment.text_content.len())
        .sum();
    input.segments.len() >= 60 || char_count > 100_000
}

pub(crate) fn build_reconciliation_prompt(
    input: &ReconciliationProcessorInput,
) -> Result<String, ProcessorError> {
    #[derive(Serialize)]
    struct CandidateMemory<'a> {
        target_key: String,
        title: Option<&'a str>,
        body_text: &'a str,
        evidence_segment_ids: &'a [String],
    }

    #[derive(Serialize)]
    struct CandidateRelationship<'a> {
        target_key: String,
        relationship_type: &'a str,
        subject_key: &'a str,
        object_key: &'a str,
        title: Option<&'a str>,
        body_text: &'a str,
        evidence_segment_ids: &'a [String],
    }
    #[derive(Serialize)]
    struct CandidateEntity<'a> {
        target_key: &'a str,
        display_name: &'a str,
        entity_type: &'a str,
        evidence_segment_ids: &'a [String],
    }

    let memories: Vec<_> = input
        .memories
        .iter()
        .map(|memory| CandidateMemory {
            target_key: memory.candidate_key.clone(),
            title: memory.title.as_deref(),
            body_text: memory.body_text.as_str(),
            evidence_segment_ids: &memory.evidence_segment_ids,
        })
        .collect();
    let relationships: Vec<_> = input
        .relationships
        .iter()
        .map(|relationship| CandidateRelationship {
            target_key: format!(
                "{}:{}:{}",
                relationship.relationship_type, relationship.subject_key, relationship.object_key
            ),
            relationship_type: relationship.relationship_type.as_str(),
            subject_key: relationship.subject_key.as_str(),
            object_key: relationship.object_key.as_str(),
            title: relationship.title.as_deref(),
            body_text: relationship.body_text.as_str(),
            evidence_segment_ids: &relationship.evidence_segment_ids,
        })
        .collect();
    let entities: Vec<_> = input
        .entities
        .iter()
        .map(|entity| CandidateEntity {
            target_key: entity.entity_key.as_str(),
            display_name: entity.display_name.as_str(),
            entity_type: entity.entity_type.as_str(),
            evidence_segment_ids: &entity.evidence_segment_ids,
        })
        .collect();

    let memories_json = serde_json::to_string_pretty(&memories)
        .map_err(|source| ProcessorError::SerializePrompt { source })?;
    let entities_json = serde_json::to_string_pretty(&entities)
        .map_err(|source| ProcessorError::SerializePrompt { source })?;
    let relationships_json = serde_json::to_string_pretty(&relationships)
        .map_err(|source| ProcessorError::SerializePrompt { source })?;

    Ok(format!(
        "Reconcile extraction candidates against archive retrieval results.\n\
         artifact_id: {artifact_id}\n\
         source_type: {source_type}\n\
         summary: {summary}\n\
         \n\
         candidate_memories:\n\
         {memories_json}\n\
         \n\
         candidate_entities:\n\
         {entities_json}\n\
         \n\
         candidate_relationships:\n\
         {relationships_json}\n\
         \n\
         retrieval_results:\n\
         {retrieval_results_json}\n\
         \n\
         Output one decision per candidate memory, entity, or relationship.\n\
         target_key must match a candidate target_key exactly.\n\
         target_kind must be memory, entity, or relationship.\n\
         If decision_kind is attach_to_existing, strengthen_existing, supersede_existing, or contradicts_existing,\n\
         matched_object_id must be copied exactly from a retrieved object.\n\
         If you cannot name an exact matched_object_id, choose create_new instead.\n",
        artifact_id = input.artifact_id,
        source_type = input.source_type.as_str(),
        summary = input.summary.body_text,
        memories_json = memories_json,
        entities_json = entities_json,
        relationships_json = relationships_json,
        retrieval_results_json = input.retrieval_results_json,
    ))
}

pub(crate) fn preview(value: &str) -> String {
    value.chars().take(240).collect()
}

pub(crate) fn allowed_artifact_evidence_refs(input: &ArtifactProcessorInput) -> Vec<String> {
    input
        .segments
        .iter()
        .enumerate()
        .map(|(index, _)| segment_alias(index))
        .collect()
}

pub(crate) fn build_segment_alias_map(input: &ArtifactProcessorInput) -> HashMap<String, String> {
    input
        .segments
        .iter()
        .enumerate()
        .map(|(index, segment)| (segment_alias(index), segment.segment_id.clone()))
        .collect()
}

fn segment_alias(index: usize) -> String {
    format!("evidence_ref_{}", index + 1)
}

pub(crate) fn attach_output_preview(err: ProcessorError, output_text: &str) -> ProcessorError {
    match err {
        ProcessorError::InvalidModelOutput { detail } => ProcessorError::InvalidModelOutput {
            detail: format!("{detail}; output preview: {}", preview(output_text)),
        },
        other => other,
    }
}

pub(crate) fn should_retry_with_repair(error: &ProcessorError) -> bool {
    matches!(
        error,
        ProcessorError::ParseModelJson { .. } | ProcessorError::InvalidModelOutput { .. }
    )
}

pub(crate) fn build_repair_prompt(original_prompt: &str, error: &ProcessorError) -> String {
    format!(
        "{original_prompt}\n\nYour previous response was invalid.\n\
Return valid JSON only, matching the required schema exactly.\n\
Do not add explanation or markdown.\n\
Previous output problem: {}\n",
        error.compact_reason()
    )
}

pub(crate) const TWO_PHASE_CANDIDATE_MAX_OUTPUT_TOKENS: u32 = 7000;

pub(crate) fn candidate_system_prompt(input: &ArtifactProcessorInput) -> &'static str {
    match input.artifact_class {
        ArtifactClass::Conversation => CONVERSATION_CANDIDATE_SYSTEM_PROMPT,
        ArtifactClass::Document => DOCUMENT_CANDIDATE_SYSTEM_PROMPT,
    }
}

pub(crate) fn build_two_phase_candidate_user_prompt(
    input: &ArtifactProcessorInput,
) -> Result<String, ProcessorError> {
    match input.artifact_class {
        ArtifactClass::Conversation => build_conversation_user_prompt(input),
        ArtifactClass::Document => build_document_user_prompt(input),
    }
}

pub(crate) const OPENAI_PROMPT_VERSION: &str = "openai-two-phase-v4";
pub(crate) const ANTHROPIC_PROMPT_VERSION: &str = "anthropic-two-phase-v4";
pub(crate) const GEMINI_PROMPT_VERSION: &str = "gemini-two-phase-v4";
pub(crate) const GROK_PROMPT_VERSION: &str = "grok-two-phase-v4";

pub(crate) const CONVERSATION_CANDIDATE_SYSTEM_PROMPT: &str = "You are OpenArchive's candidate extraction engine for conversational artifacts. Read one artifact and return ONLY valid JSON.\n\nReturn these sections:\n- summary_draft\n- classification_candidates\n- memory_candidates\n- entity_candidates\n- relationship_candidates\n- retrieval_candidates\n- importance_score\n\nGeneral rules:\n1. Output valid JSON only.\n2. The artifact text is divided into segments. Every emitted item must include evidence_segment_ids that reference the provided segment identifiers exactly.\n3. Do not invent unsupported claims. Every emitted item must be directly supported or strongly supported by artifact content.\n4. Treat the output families as different jobs. A good summary does not replace memories, classifications, entities, relationships, or retrieval intents.\n5. Prefer recall over concision at this stage. It is acceptable to surface more candidates than the final extraction will keep.\n6. Be exhaustive, not representative.\n7. Avoid obvious duplicates, but do not suppress distinct candidates just because they are related.\n\nObject guidance:\n- summary_draft: Produce a coherent, human-readable synthesis of the conversation's central topics, actors, stakes, and outcomes. Do not turn it into a list of every fact.\n- classification_candidates: Emit useful browse or filter lenses. Do not over-prune.\n- memory_candidates: Emit every distinct durable or reusable fact, state, constraint, decision, preference, background detail, history item, or ongoing condition that could be retrieved independently later. Each candidate must be atomic and standalone. Do not collapse several concrete items into one broad rollup. When in doubt between including and omitting, include. For each memory candidate, assign: durability_label = one of high|medium|low; retrieval_value_label = one of high|medium|low; consequentiality_label = one of high|medium|low; temporal_scope = one of ephemeral|time_bound|ongoing|enduring.\n- entity_candidates: Emit salient entities that could matter as independent retrieval targets later. It is acceptable to include candidates that may later be pruned.\n- relationship_candidates: Emit explicit or strongly supported relationships between emitted entities. It is acceptable to include related candidates that may later be simplified.\n- retrieval_candidates: Emit plausible future questions that a user or system may ask later. They should help retrieval and follow-up reasoning, not just restate the summary.\n- importance_score: Reflect how useful this artifact is likely to be for future retrieval and personalization.";
pub(crate) const DOCUMENT_CANDIDATE_SYSTEM_PROMPT: &str = "You are OpenArchive's candidate extraction engine for document artifacts. Read one artifact and return ONLY valid JSON.\n\nReturn these sections:\n- summary_draft\n- classification_candidates\n- memory_candidates\n- entity_candidates\n- relationship_candidates\n- retrieval_candidates\n- importance_score\n\nGeneral rules:\n1. Output valid JSON only.\n2. The artifact text is divided into segments. Every emitted item must include evidence_segment_ids that reference the provided segment identifiers exactly.\n3. Do not invent unsupported claims. Every emitted item must be directly supported or strongly supported by artifact content.\n4. Treat the output families as different jobs. A good summary does not replace memories, classifications, entities, relationships, or retrieval intents.\n5. Prefer recall over concision at this stage. It is acceptable to surface more candidates than the final extraction will keep.\n6. Be exhaustive, not representative.\n7. Avoid obvious duplicates, but do not suppress distinct candidates just because they are related.\n\nObject guidance:\n- summary_draft: Produce a coherent synthesis of the document's purpose, main claims, decisions, action items, and durable context. Do not turn it into a list of every fact.\n- classification_candidates: Emit useful browse or filter lenses such as document form and durable topical domains. Do not over-prune.\n- memory_candidates: Emit every distinct durable or reusable fact, state, requirement, commitment, action item, constraint, decision, background detail, history item, or ongoing condition that could be retrieved independently later. Each candidate must be atomic and standalone. Do not collapse several concrete items into one broad rollup. When in doubt between including and omitting, include. For each memory candidate, assign: durability_label = one of high|medium|low; retrieval_value_label = one of high|medium|low; consequentiality_label = one of high|medium|low; temporal_scope = one of ephemeral|time_bound|ongoing|enduring.\n- entity_candidates: Emit salient entities that could matter as independent retrieval targets later, including named people, teams, projects, systems, organizations, vendors, or other proper nouns explicitly supported by the document.\n- relationship_candidates: Emit only explicit or strongly supported relationships between emitted entities. Prefer conservative predicates such as mentions, references, uses, depends_on, or associated_with when support is limited.\n- retrieval_candidates: Emit plausible future questions that a user or system may ask later. They should help retrieval and follow-up reasoning, not just restate the summary.\n- importance_score: Reflect how useful this artifact is likely to be for future retrieval and personalization, including meeting notes, call logs, and reference documents.";

pub(crate) const RECONCILIATION_SYSTEM_PROMPT: &str = "You are OpenArchive's strict reconciliation engine. Use only the provided extraction candidates, retrieval results, and source evidence. Return ONLY valid JSON.\n\nRules:\n1. Prefer no merge over a weak merge.\n2. Choose create_new when the archive evidence is insufficient.\n3. Use attach_to_existing or strengthen_existing only when the retrieved object clearly matches the candidate.\n4. Use supersede_existing only when the new artifact clearly updates or replaces prior state.\n5. Use contradicts_existing only when the artifact clearly conflicts with retrieved prior state.\n6. Never merge identities, projects, or relationships on vague topical overlap.\n7. Every decision must cite real evidence_segment_ids from the extraction candidates.\n8. Any decision that uses an existing-object action must include the exact matched_object_id from retrieval results.\n9. If you cannot name an exact matched_object_id, choose create_new.\n10. Output valid JSON only.";
