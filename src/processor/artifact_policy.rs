use super::{
    classify_artifact, ArtifactArchetype, ArtifactClassificationProfile, ArtifactProcessorInput,
};

#[derive(Debug, Clone)]
pub(crate) struct ArtifactExtractionPolicy {
    pub classification_limit: usize,
    pub memory_limit: usize,
    pub entity_limit: usize,
    pub relationship_limit: usize,
    pub retrieval_limit: usize,
    pub profile: ArtifactClassificationProfile,
}

pub(crate) fn extraction_policy_for(input: &ArtifactProcessorInput) -> ArtifactExtractionPolicy {
    let profile = classify_artifact(input);
    let (classification_limit, memory_limit, entity_limit, relationship_limit, retrieval_limit) =
        match profile.primary_archetype {
            ArtifactArchetype::Conversation => (3, 10, 8, 6, 4),
            ArtifactArchetype::ProceduralNote => (3, 6, 6, 3, 3),
            ArtifactArchetype::ReferenceNote => (3, 6, 6, 3, 3),
            ArtifactArchetype::DefinitionNote => (3, 3, 4, 2, 2),
            ArtifactArchetype::WorkingNote => (4, 10, 6, 5, 5),
            ArtifactArchetype::JournalLog => (3, 8, 3, 2, 3),
            ArtifactArchetype::DashboardTemplate => (3, 3, 3, 1, 2),
            ArtifactArchetype::Unknown => (4, 10, 6, 4, 4),
        };
    ArtifactExtractionPolicy {
        classification_limit,
        memory_limit,
        entity_limit,
        relationship_limit,
        retrieval_limit,
        profile,
    }
}

impl ArtifactExtractionPolicy {
    pub(crate) fn memory_role_values(&self) -> &'static [&'static str] {
        match self.profile.primary_archetype {
            ArtifactArchetype::Conversation => &[
                "personal_fact",
                "preference",
                "project_fact",
                "ongoing_state",
                "reference_fact",
            ],
            ArtifactArchetype::ProceduralNote => &[
                "procedure_step",
                "requirement",
                "constraint",
                "configuration",
                "command",
                "reference_fact",
            ],
            ArtifactArchetype::ReferenceNote => &[
                "definition",
                "reference_fact",
                "comparison",
                "formula",
                "glossary_term",
                "cross_reference",
            ],
            ArtifactArchetype::DefinitionNote => {
                &["definition", "alias", "reference_fact", "glossary_term"]
            }
            ArtifactArchetype::WorkingNote => &[
                "decision",
                "action_item",
                "status",
                "owner",
                "open_question",
                "project_fact",
            ],
            ArtifactArchetype::JournalLog => {
                &["state_change", "reflection", "event", "ongoing_state"]
            }
            ArtifactArchetype::DashboardTemplate => &[
                "project_fact",
                "reference_fact",
                "navigation",
                "display_rule",
                "template_rule",
            ],
            ArtifactArchetype::Unknown => &[
                "personal_fact",
                "preference",
                "project_fact",
                "ongoing_state",
                "reference_fact",
            ],
        }
    }

    pub(crate) fn memory_role_guidance(&self) -> String {
        match self.profile.primary_archetype {
            ArtifactArchetype::Conversation => {
                "Choose memory_role values that reflect the durable fact itself: personal_fact, preference, project_fact, ongoing_state, or reference_fact.".to_string()
            }
            ArtifactArchetype::ProceduralNote => {
                "For procedural notes, use memory_role to distinguish procedure_step, requirement, constraint, configuration, command, or reference_fact. Prefer operational roles over generic topic restatements.".to_string()
            }
            ArtifactArchetype::ReferenceNote => {
                "For reference notes, use memory_role to distinguish definition, reference_fact, comparison, formula, glossary_term, or cross_reference. Prefer stable knowledge roles over note-structure roles.".to_string()
            }
            ArtifactArchetype::DefinitionNote => {
                "For definition notes, use memory_role values like definition, alias, glossary_term, or reference_fact and keep them tightly definition-centric.".to_string()
            }
            ArtifactArchetype::WorkingNote => {
                "For working notes, use memory_role values like decision, action_item, status, owner, open_question, or project_fact. Prefer active work state over passive summary.".to_string()
            }
            ArtifactArchetype::JournalLog => {
                "For journal/log notes, use memory_role values like state_change, reflection, event, or ongoing_state. Preserve chronology and lived context.".to_string()
            }
            ArtifactArchetype::DashboardTemplate => {
                "For dashboards/templates, use memory_role sparingly: project_fact, reference_fact, navigation, display_rule, or template_rule. Prefer reusable structure over boilerplate.".to_string()
            }
            ArtifactArchetype::Unknown => {
                "Choose the closest memory_role for each candidate: personal_fact, preference, project_fact, ongoing_state, or reference_fact.".to_string()
            }
        }
    }

    pub(crate) fn relationship_type_values(&self) -> &'static [&'static str] {
        match self.profile.primary_archetype {
            ArtifactArchetype::Conversation => &[
                "mentions",
                "references",
                "uses",
                "depends_on",
                "associated_with",
                "discusses",
            ],
            ArtifactArchetype::ProceduralNote => &[
                "mentions",
                "references",
                "uses",
                "depends_on",
                "configured_by",
                "step_for",
                "associated_with",
            ],
            ArtifactArchetype::ReferenceNote => &[
                "mentions",
                "references",
                "defines",
                "compares_with",
                "associated_with",
                "depends_on",
            ],
            ArtifactArchetype::DefinitionNote => {
                &["defines", "alias_of", "references", "associated_with"]
            }
            ArtifactArchetype::WorkingNote => &[
                "mentions",
                "references",
                "assigned_to",
                "blocked_by",
                "depends_on",
                "associated_with",
            ],
            ArtifactArchetype::JournalLog => &["mentions", "references", "associated_with"],
            ArtifactArchetype::DashboardTemplate => {
                &["references", "uses", "depends_on", "associated_with"]
            }
            ArtifactArchetype::Unknown => &[
                "mentions",
                "references",
                "uses",
                "depends_on",
                "associated_with",
            ],
        }
    }

    pub(crate) fn relationship_type_guidance(&self) -> String {
        match self.profile.primary_archetype {
            ArtifactArchetype::ProceduralNote => {
                "For procedural notes, keep relationships operational and conservative: references, uses, depends_on, configured_by, step_for, or associated_with.".to_string()
            }
            ArtifactArchetype::ReferenceNote => {
                "For reference notes, keep relationships descriptive and conservative: defines, references, compares_with, depends_on, or associated_with.".to_string()
            }
            ArtifactArchetype::DefinitionNote => {
                "For definition notes, keep relationships sparse and definition-centric: defines, alias_of, references, or associated_with.".to_string()
            }
            ArtifactArchetype::WorkingNote => {
                "For working notes, keep relationships tied to active work: assigned_to, blocked_by, depends_on, references, or associated_with.".to_string()
            }
            ArtifactArchetype::DashboardTemplate => {
                "For dashboards and templates, keep relationships structural and conservative: references, uses, depends_on, or associated_with.".to_string()
            }
            _ => {
                "Use only conservative relationship predicates from the allowed schema enum; avoid stronger claims unless the schema explicitly allows them.".to_string()
            }
        }
    }

    pub(crate) fn retrieval_intent_values(&self) -> &'static [&'static str] {
        match self.profile.primary_archetype {
            ArtifactArchetype::Conversation => &[
                "topic_lookup",
                "memory_match",
                "entity_lookup",
                "relationship_lookup",
                "contradiction_check",
            ],
            ArtifactArchetype::ProceduralNote => &[
                "topic_lookup",
                "entity_lookup",
                "relationship_lookup",
                "contradiction_check",
            ],
            ArtifactArchetype::ReferenceNote => &[
                "topic_lookup",
                "entity_lookup",
                "relationship_lookup",
                "contradiction_check",
            ],
            ArtifactArchetype::DefinitionNote => {
                &["topic_lookup", "entity_lookup", "relationship_lookup"]
            }
            ArtifactArchetype::WorkingNote => &[
                "topic_lookup",
                "memory_match",
                "entity_lookup",
                "relationship_lookup",
                "contradiction_check",
            ],
            ArtifactArchetype::JournalLog => {
                &["memory_match", "topic_lookup", "contradiction_check"]
            }
            ArtifactArchetype::DashboardTemplate => {
                &["topic_lookup", "entity_lookup", "relationship_lookup"]
            }
            ArtifactArchetype::Unknown => &[
                "topic_lookup",
                "memory_match",
                "entity_lookup",
                "relationship_lookup",
                "contradiction_check",
            ],
        }
    }

    pub(crate) fn retrieval_intent_guidance(&self) -> String {
        match self.profile.primary_archetype {
            ArtifactArchetype::ProceduralNote => {
                "For procedural notes, retrieval intents should focus on troubleshooting, alternate paths, prerequisite context, platform-specific caveats, and related setup docs.".to_string()
            }
            ArtifactArchetype::ReferenceNote => {
                "For reference notes, retrieval intents should focus on related concepts, formulas, contrasting notes, linked references, and possible contradictions.".to_string()
            }
            ArtifactArchetype::DefinitionNote => {
                "For definition notes, retrieval intents should stay sparse and focus on related terms, related entities, and nearby definitions.".to_string()
            }
            ArtifactArchetype::WorkingNote => {
                "For working notes, retrieval intents should focus on prior decisions, related meetings, open tasks, blockers, dependencies, and status history.".to_string()
            }
            ArtifactArchetype::JournalLog => {
                "For journal/log notes, retrieval intents should focus on prior states, repeated events, chronology, and contradictions with earlier entries.".to_string()
            }
            ArtifactArchetype::DashboardTemplate => {
                "For dashboards/templates, retrieval intents should focus on linked resources, related dashboards, and the meaning of reusable structural rules.".to_string()
            }
            _ => {
                "Use retrieval intents only for archive-only follow-up questions that materially improve linking, deduplication, contradiction checks, or related-object recall.".to_string()
            }
        }
    }

    pub(crate) fn classification_guidance(&self) -> String {
        match self.profile.primary_archetype {
            ArtifactArchetype::ProceduralNote => {
                "Classifications should emphasize procedural form, target system, platform, and product/domain rather than metadata or section names.".to_string()
            }
            ArtifactArchetype::ReferenceNote => {
                "Classifications should emphasize subject matter and reference form, not ephemeral usage context.".to_string()
            }
            ArtifactArchetype::DefinitionNote => {
                "Classifications should stay sparse and emphasize the defined domain or glossary context, not generic note metadata.".to_string()
            }
            ArtifactArchetype::WorkingNote => {
                "Classifications should emphasize active work form, project/domain, and coordination context.".to_string()
            }
            ArtifactArchetype::JournalLog => {
                "Classifications should emphasize chronology, domain, and lived context rather than generic topics.".to_string()
            }
            ArtifactArchetype::DashboardTemplate => {
                "Classifications should emphasize dashboard/template form and the main workspace or domain it serves.".to_string()
            }
            _ => {
                "Classifications should capture durable topical lenses and broad document form, not one-off details.".to_string()
            }
        }
    }

    pub(crate) fn prompt_guidance_lines(&self) -> Vec<String> {
        let mut lines = vec![
            format!(
                "- target output budget: classifications <= {}, memories <= {}, entities <= {}, relationships <= {}, retrieval_intents <= {}",
                self.classification_limit,
                self.memory_limit,
                self.entity_limit,
                self.relationship_limit,
                self.retrieval_limit
            ),
            "- if more candidates seem plausible than the target budget allows, keep the highest long-term retrieval value items first within each array".to_string(),
            format!(
                "- memory_candidates.memory_role must be one of: {}",
                self.memory_role_values().join(", ")
            ),
            format!("- {}", self.memory_role_guidance()),
            format!(
                "- relationship_candidates.relationship_type must be one of: {}",
                self.relationship_type_values().join(", ")
            ),
            format!("- {}", self.relationship_type_guidance()),
            format!(
                "- retrieval_candidates.intent_type must be one of: {}",
                self.retrieval_intent_values().join(", ")
            ),
            format!("- {}", self.retrieval_intent_guidance()),
            format!("- {}", self.classification_guidance()),
        ];
        match self.profile.primary_archetype {
            ArtifactArchetype::Conversation => {
                lines.push(
                    "- keep the strongest durable facts first; do not spend budget on weak topical restatements"
                        .to_string(),
                );
            }
            ArtifactArchetype::ProceduralNote => {
                lines.push(
                    "- spend budget on prerequisites, ordered steps, commands, configuration points, constraints, and expected outcomes before broad topical paraphrases"
                        .to_string(),
                );
                lines.push(
                    "- do not spend memory budget on note titles, section scaffolding, tags, navigation links, creation dates, or other document metadata unless they directly change how the procedure is executed"
                        .to_string(),
                );
            }
            ArtifactArchetype::ReferenceNote => {
                lines.push(
                    "- spend budget on stable facts, definitions, named concepts, and explicit cross-references before generic summary-like memories"
                        .to_string(),
                );
                lines.push(
                    "- do not spend memory budget on note structure, tags, aliases, navigation links, or file metadata unless they are themselves the subject of the reference note"
                        .to_string(),
                );
            }
            ArtifactArchetype::DefinitionNote => {
                lines.push(
                    "- keep extraction sparse and definition-centric; focus on the core term, aliases, and only the most useful related entities or links"
                        .to_string(),
                );
                lines.push(
                    "- avoid memories about note scaffolding, empty sections, navigation links, or file metadata"
                        .to_string(),
                );
            }
            ArtifactArchetype::WorkingNote => {
                lines.push(
                    "- prioritize tasks, decisions, owners, statuses, open questions, and concrete next steps ahead of background context"
                        .to_string(),
                );
            }
            ArtifactArchetype::JournalLog => {
                lines.push(
                    "- prioritize dated state changes, meaningful reflections, and ongoing conditions rather than generic topical labels"
                        .to_string(),
                );
            }
            ArtifactArchetype::DashboardTemplate => {
                lines.push(
                    "- keep extraction intentionally sparse; prefer one strong summary plus only the most reusable navigational, procedural, or structural facts"
                        .to_string(),
                );
                lines.push(
                    "- do not spend budget on placeholder text, navigation chrome, repeated query mechanics, or obvious boilerplate"
                        .to_string(),
                );
            }
            ArtifactArchetype::Unknown => {}
        }
        lines
    }
}
