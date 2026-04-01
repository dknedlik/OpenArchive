use super::{classify_artifact, ArtifactArchetype, ArtifactClassificationProfile, ArtifactProcessorInput};

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
            ArtifactArchetype::Conversation => (4, 15, 8, 6, 5),
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
