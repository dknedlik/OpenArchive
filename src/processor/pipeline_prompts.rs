use serde::Serialize;

use super::*;

pub(crate) fn build_reconciliation_prompt(
    input: &ReconciliationProcessorInput,
) -> Result<String, ProcessorError> {
    #[derive(Serialize)]
    struct CandidateMemory<'a> {
        target_key: String,
        title: Option<&'a str>,
        body_text: &'a str,
    }

    #[derive(Serialize)]
    struct CandidateRelationship<'a> {
        target_key: String,
        relationship_type: &'a str,
        subject_key: &'a str,
        object_key: &'a str,
        title: Option<&'a str>,
        body_text: &'a str,
    }

    #[derive(Serialize)]
    struct CandidateEntity<'a> {
        target_key: &'a str,
        display_name: &'a str,
        entity_type: &'a str,
    }

    let memories: Vec<_> = input
        .memories
        .iter()
        .map(|memory| CandidateMemory {
            target_key: memory.candidate_key.clone(),
            title: memory.title.as_deref(),
            body_text: memory.body_text.as_str(),
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
        })
        .collect();
    let entities: Vec<_> = input
        .entities
        .iter()
        .map(|entity| CandidateEntity {
            target_key: entity.entity_key.as_str(),
            display_name: entity.display_name.as_str(),
            entity_type: entity.entity_type.as_str(),
        })
        .collect();

    let memories_json = serde_json::to_string_pretty(&memories)
        .map_err(|source| ProcessorError::SerializePrompt { source })?;
    let entities_json = serde_json::to_string_pretty(&entities)
        .map_err(|source| ProcessorError::SerializePrompt { source })?;
    let relationships_json = serde_json::to_string_pretty(&relationships)
        .map_err(|source| ProcessorError::SerializePrompt { source })?;

    Ok(format!(
        "Compare ambiguous extracted candidates against a short list of possible existing-object matches.\n\
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
         candidate_match_options:\n\
         {retrieval_results_json}\n\
         \n\
         Output exactly one decision for each listed candidate memory, entity, or relationship.\n\
         Treat each candidate as a narrow comparison task: compare it only to the provided candidate_match_options for that target_key.\n\
         Prefer create_new unless one provided object is clearly the same underlying thing.\n\
         target_key must match a listed candidate target_key exactly.\n\
         target_kind must be memory, entity, or relationship.\n\
         If you choose any existing-object action, matched_object_id must be copied exactly from the candidate_match_options for that target_key.\n\
         Never invent or infer a matched_object_id that is not present in the provided candidate_match_options.\n\
         If the listed matches are uncertain, partial, or merely topically related, choose create_new.\n",
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

pub(crate) const RECONCILIATION_SYSTEM_PROMPT: &str = "You are OpenArchive's strict ambiguous-match adjudicator. You are not reconciling a whole artifact; you are resolving a small set of candidate-vs-existing-object comparisons. Use only the provided candidates and candidate_match_options. Return ONLY valid JSON.\n\nRules:\n1. Prefer create_new over a weak merge.\n2. Use an existing-object action only when a provided match option clearly refers to the same underlying object.\n3. Never merge identities, projects, memories, or relationships on vague topical overlap.\n4. Any decision that uses an existing-object action must include the exact matched_object_id from candidate_match_options.\n5. If you cannot name an exact matched_object_id from candidate_match_options, choose create_new.\n6. Output valid JSON only.";
