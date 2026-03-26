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

    Ok(format!(
        "Input artifact:\n\
         artifact_class: document\n\
         artifact_id: {artifact_id}\n\
         source_type: {source_type}\n\
         title: {title}\n\
         \n\
         segments:\n\
         {segments_json}\n\
         \n\
         Output guidance:\n\
         - summarize the document, not a dialogue; capture its purpose, key claims, decisions, action items, and durable references\n\
         - first infer what kind of document this is from the text itself, such as meeting notes, call notes, work log, research notes, design notes, journal entry, or general reference material, and adapt extraction accordingly\n\
         - summary is not a substitute for memories; memories should stay atomic and independently retrievable later\n\
         - classifications should capture document form and durable topical lenses, not one-off details\n\
         - memories: extract every durable fact, decision, commitment, requirement, constraint, ongoing state, action item owner, reference detail, or background context that could matter in future retrieval\n\
         - if the document appears to be meeting notes or a call log, treat attendees, speakers, owners, discussed systems, and recorded decisions as likely entity and relationship candidates when explicitly supported\n\
         - entities: include explicit named people, teams, organizations, projects, systems, repositories, vendors, and domain-specific proper nouns with likely future retrieval value\n\
         - relationships: include explicit or strongly supported links such as ownership, participation, dependency, decision impact, coordination, or affiliation between emitted entities\n\
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
        allowed_refs_json = serde_json::to_string(&allowed_artifact_evidence_refs(input))
            .map_err(|source| ProcessorError::SerializePrompt { source })?,
    ))
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

    let memories_json = serde_json::to_string_pretty(&memories)
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
         candidate_relationships:\n\
         {relationships_json}\n\
         \n\
         retrieval_results:\n\
         {retrieval_results_json}\n\
         \n\
         Output one decision per candidate memory or relationship.\n\
         target_key must match a candidate target_key exactly.\n\
         target_kind must be memory or relationship.\n\
         If decision_kind is attach_to_existing, strengthen_existing, supersede_existing, or contradicts_existing,\n\
         matched_object_id must be copied exactly from a retrieved object.\n\
         If you cannot name an exact matched_object_id, choose create_new instead.\n",
        artifact_id = input.artifact_id,
        source_type = input.source_type.as_str(),
        summary = input.summary.body_text,
        memories_json = memories_json,
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
#[allow(dead_code)]
pub(crate) const TWO_PHASE_FINALIZER_MAX_OUTPUT_TOKENS: u32 = 4000;

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

#[allow(dead_code)]
pub(crate) fn build_two_phase_finalizer_prompt(
    artifact_id: &str,
    candidate: &ModelCandidateArtifactOutput,
) -> Result<String, ProcessorError> {
    let candidate_json = serde_json::to_string_pretty(candidate)
        .map_err(|source| ProcessorError::SerializePrompt { source })?;
    Ok(format!(
        "Finalize this extraction candidate bundle into the target schema.\n\
         artifact_id: {artifact_id}\n\
         \n\
         Candidate bundle:\n\
         {candidate_json}\n\
         \n\
         Rules:\n\
         - Use only facts and evidence refs present in the candidate bundle.\n\
         - Do not invent new memories, entities, relationships, classifications, or retrieval intents.\n\
         - Merge overlapping candidates and drop non-durable or redundant items.\n\
         - Prefer specific, retrieval-worthy memories over broad umbrella memories.\n\
         - Choose memory_type only from: personal_fact, preference, project_fact, ongoing_state, reference.\n\
         - Preserve evidence refs exactly as provided.\n\
         - Fix mistyped or weak candidates where possible.\n\
         - Output valid JSON only.\n"
    ))
}

pub(crate) const OPENAI_PROMPT_VERSION: &str = "openai-two-phase-v2";
pub(crate) const ANTHROPIC_PROMPT_VERSION: &str = "anthropic-two-phase-v2";
pub(crate) const GEMINI_PROMPT_VERSION: &str = "gemini-two-phase-v2";
pub(crate) const GROK_PROMPT_VERSION: &str = "grok-two-phase-v2";

pub(crate) const CONVERSATION_CANDIDATE_SYSTEM_PROMPT: &str = "You are OpenArchive's candidate extraction engine for conversational artifacts. Read one artifact and return ONLY valid JSON.\n\nReturn these sections:\n- summary_draft\n- classification_candidates\n- memory_candidates\n- entity_candidates\n- relationship_candidates\n- retrieval_candidates\n- importance_score\n\nGeneral rules:\n1. Output valid JSON only.\n2. The artifact text is divided into segments. Every emitted item must include evidence_segment_ids that reference the provided segment identifiers exactly.\n3. Do not invent unsupported claims. Every emitted item must be directly supported or strongly supported by artifact content.\n4. Treat the output families as different jobs. A good summary does not replace memories, classifications, entities, relationships, or retrieval intents.\n5. Prefer recall over concision at this stage. It is acceptable to surface more candidates than the final extraction will keep.\n6. Be exhaustive, not representative.\n7. Avoid obvious duplicates, but do not suppress distinct candidates just because they are related.\n\nObject guidance:\n- summary_draft: Produce a coherent, human-readable synthesis of the conversation's central topics, actors, stakes, and outcomes. Do not turn it into a list of every fact.\n- classification_candidates: Emit useful browse or filter lenses. Do not over-prune.\n- memory_candidates: Emit every distinct durable or reusable fact, state, constraint, decision, preference, background detail, history item, or ongoing condition that could be retrieved independently later. Each candidate must be atomic and standalone. Do not collapse several concrete items into one broad rollup. When in doubt between including and omitting, include. For each memory candidate, assign: durability_label = one of high|medium|low; retrieval_value_label = one of high|medium|low; consequentiality_label = one of high|medium|low; temporal_scope = one of ephemeral|time_bound|ongoing|enduring.\n- entity_candidates: Emit salient entities that could matter as independent retrieval targets later. It is acceptable to include candidates that may later be pruned.\n- relationship_candidates: Emit explicit or strongly supported relationships between emitted entities. It is acceptable to include related candidates that may later be simplified.\n- retrieval_candidates: Emit plausible future questions that a user or system may ask later. They should help retrieval and follow-up reasoning, not just restate the summary.\n- importance_score: Reflect how useful this artifact is likely to be for future retrieval and personalization.";
pub(crate) const DOCUMENT_CANDIDATE_SYSTEM_PROMPT: &str = "You are OpenArchive's candidate extraction engine for document artifacts. Read one artifact and return ONLY valid JSON.\n\nReturn these sections:\n- summary_draft\n- classification_candidates\n- memory_candidates\n- entity_candidates\n- relationship_candidates\n- retrieval_candidates\n- importance_score\n\nGeneral rules:\n1. Output valid JSON only.\n2. The artifact text is divided into segments. Every emitted item must include evidence_segment_ids that reference the provided segment identifiers exactly.\n3. Do not invent unsupported claims. Every emitted item must be directly supported or strongly supported by artifact content.\n4. Treat the output families as different jobs. A good summary does not replace memories, classifications, entities, relationships, or retrieval intents.\n5. Prefer recall over concision at this stage. It is acceptable to surface more candidates than the final extraction will keep.\n6. Be exhaustive, not representative.\n7. Avoid obvious duplicates, but do not suppress distinct candidates just because they are related.\n\nObject guidance:\n- summary_draft: Produce a coherent synthesis of the document's purpose, main claims, decisions, action items, and durable context. Do not turn it into a list of every fact.\n- classification_candidates: Emit useful browse or filter lenses such as document form and durable topical domains. Do not over-prune.\n- memory_candidates: Emit every distinct durable or reusable fact, state, requirement, commitment, action item, constraint, decision, background detail, history item, or ongoing condition that could be retrieved independently later. Each candidate must be atomic and standalone. Do not collapse several concrete items into one broad rollup. When in doubt between including and omitting, include. For each memory candidate, assign: durability_label = one of high|medium|low; retrieval_value_label = one of high|medium|low; consequentiality_label = one of high|medium|low; temporal_scope = one of ephemeral|time_bound|ongoing|enduring.\n- entity_candidates: Emit salient entities that could matter as independent retrieval targets later, including named people, teams, projects, systems, organizations, vendors, or other proper nouns explicitly supported by the document.\n- relationship_candidates: Emit explicit or strongly supported relationships between emitted entities, including ownership, participation, coordination, dependency, affiliation, or decision impact when supported.\n- retrieval_candidates: Emit plausible future questions that a user or system may ask later. They should help retrieval and follow-up reasoning, not just restate the summary.\n- importance_score: Reflect how useful this artifact is likely to be for future retrieval and personalization, including meeting notes, call logs, and reference documents.";

#[allow(dead_code)]
pub(crate) const TWO_PHASE_FINALIZER_SYSTEM_PROMPT: &str = "You are OpenArchive's extraction finalizer. Use only the provided candidate bundle and return ONLY valid JSON in the requested final schema.\n\nGeneral rules:\n1. Do not invent facts, evidence refs, or objects not supported by the candidate bundle.\n2. Preserve evidence refs exactly.\n3. Do not treat any count limit as a target. Keep the strongest distinct items that belong; do not pad, and do not collapse unrelated facts merely to be shorter.\n4. When several candidates are individually valid, prefer the set with higher long-term retrieval value, clearer future usefulness, and greater downstream consequence.\n5. The candidate bundle is already a first-pass extraction. Pruning is optional, not required. If the candidate count is already within the schema limit, you do not need to remove anything. Drop or merge only when candidates are genuine duplicates, clear near-duplicates, unsupported by the bundle, or clearly low-value and local to this artifact.\n\nObject guidance:\n- summary: Keep it coherent and central. Preserve the main story of the artifact without turning the summary into a dump of all candidates.\n- classifications: Keep only useful browse or filter lenses. Remove generic, weak, or redundant classifications.\n- memories: Keep distinct, durable, retrieval-worthy memories. Use the candidate labels for durability, retrieval value, consequentiality, and temporal scope as editorial signals, not rigid rules. Candidates marked high on durability, retrieval value, or consequentiality should usually be retained unless they duplicate another stronger candidate. Deprioritize transient logistics, one-off setup details, low-value inventories, and items whose future usefulness is narrowly local to this artifact. Keep separate memories when candidates would answer meaningfully different future retrieval questions, even if they are related or appear close together in the source. Merge only genuine duplicates or near-duplicates. Do not collapse multiple important facts into an umbrella memory when keeping them separate would improve future retrieval.\n- entities: Keep only salient entities that stand on their own as future retrieval targets, but be conservative about dropping supported entities when they anchor retained memories, relationships, or retrieval intents.\n- relationships: Keep semantically clean, well-supported relationships between retained entities. Prefer retaining a supported relationship over dropping it merely for brevity.\n- retrieval_intents: Keep questions that are likely future lookups or follow-ups, not paraphrases of the summary. Prefer preserving intents that map to retained memories or relationships.\n\nSchema guidance:\n- Fix bad type assignments and choose memory_type only from the allowed schema values.\n- Output valid JSON only.";

pub(crate) const RECONCILIATION_SYSTEM_PROMPT: &str = "You are OpenArchive's strict reconciliation engine. Use only the provided extraction candidates, retrieval results, and source evidence. Return ONLY valid JSON.\n\nRules:\n1. Prefer no merge over a weak merge.\n2. Choose create_new when the archive evidence is insufficient.\n3. Use attach_to_existing or strengthen_existing only when the retrieved object clearly matches the candidate.\n4. Use supersede_existing only when the new artifact clearly updates or replaces prior state.\n5. Use contradicts_existing only when the artifact clearly conflicts with retrieved prior state.\n6. Never merge identities, projects, or relationships on vague topical overlap.\n7. Every decision must cite real evidence_segment_ids from the extraction candidates.\n8. Any decision that uses an existing-object action must include the exact matched_object_id from retrieval results.\n9. If you cannot name an exact matched_object_id, choose create_new.\n10. Output valid JSON only.";
