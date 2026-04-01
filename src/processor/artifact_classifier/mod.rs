mod features;
mod scoring;
mod types;

pub use types::{
    ArchetypeScore, ArtifactArchetype, ArtifactClassificationDebug, ArtifactClassificationProfile,
    ArtifactFacet, FacetScore,
};

use super::ArtifactProcessorInput;

pub fn classify_artifact(input: &ArtifactProcessorInput) -> ArtifactClassificationProfile {
    classify_artifact_debug(input).profile
}

pub fn classify_artifact_debug(input: &ArtifactProcessorInput) -> ArtifactClassificationDebug {
    let features = features::extract_features(input);
    scoring::classify(&features)
}

#[cfg(test)]
mod tests {
    use crate::domain::ParticipantRole;
    use crate::processor::{ArtifactArchetype, ArtifactFacet, ArtifactProcessorInput};
    use crate::storage::types::{
        ArtifactClass, ImportedNoteMetadata, ImportedNotePropertyRecord, ImportedNoteTagRecord,
        ImportedNoteTagSourceKind, LoadedParticipant, LoadedSegment, SourceType,
    };
    use crate::VisibilityStatus;

    use super::classify_artifact;

    #[test]
    fn classifies_chat_exports_as_conversations() {
        let input = ArtifactProcessorInput {
            artifact_id: "a1".to_string(),
            import_id: "i1".to_string(),
            artifact_class: ArtifactClass::Conversation,
            source_type: SourceType::ChatGptExport,
            title: Some("How should I set up Docker for this project?".to_string()),
            imported_note_metadata: None,
            participants: vec![
                LoadedParticipant {
                    participant_id: "p1".to_string(),
                    participant_role: ParticipantRole::User,
                    display_name: Some("User".to_string()),
                    external_id: None,
                },
                LoadedParticipant {
                    participant_id: "p2".to_string(),
                    participant_role: ParticipantRole::Assistant,
                    display_name: Some("Assistant".to_string()),
                    external_id: None,
                },
            ],
            segments: vec![
                segment(
                    0,
                    Some(ParticipantRole::User),
                    "How do I set this up with Docker?",
                ),
                segment(
                    1,
                    Some(ParticipantRole::Assistant),
                    "Install Docker, clone the repo, and run `docker compose up`.",
                ),
            ],
        };

        let profile = classify_artifact(&input);
        assert_eq!(profile.primary_archetype, ArtifactArchetype::Conversation);
        assert!(profile.facets.contains(&ArtifactFacet::Technical));
        assert!(profile.facets.contains(&ArtifactFacet::Setup));
    }

    #[test]
    fn classifies_definition_notes() {
        let input = ArtifactProcessorInput {
            artifact_id: "a2".to_string(),
            import_id: "i1".to_string(),
            artifact_class: ArtifactClass::Document,
            source_type: SourceType::ObsidianVault,
            title: Some("Periapsis".to_string()),
            imported_note_metadata: Some(ImportedNoteMetadata {
                note_path: Some("Definitions/Periapsis.md".to_string()),
                properties: vec![ImportedNotePropertyRecord {
                    property_key: "document_type".to_string(),
                    value_kind: crate::storage::types::ImportedNotePropertyValueKind::String,
                    value_text: Some("definition".to_string()),
                    value_json: serde_json::Value::String("definition".to_string()),
                }],
                tags: vec![ImportedNoteTagRecord {
                    raw_tag: "#definition".to_string(),
                    normalized_tag: "definition".to_string(),
                    tag_path: "definition".to_string(),
                    source_kind: ImportedNoteTagSourceKind::Inline,
                }],
                aliases: Vec::new(),
                outbound_links: Vec::new(),
            }),
            participants: Vec::new(),
            segments: vec![segment(
                0,
                None,
                "# Definition\nPeriapsis is the point in an orbit where the distance is minimal.",
            )],
        };

        let profile = classify_artifact(&input);
        assert_eq!(profile.primary_archetype, ArtifactArchetype::DefinitionNote);
        assert!(profile.facets.contains(&ArtifactFacet::Reference));
        assert!(profile.facets.contains(&ArtifactFacet::Scientific));
    }

    #[test]
    fn classifies_dashboards_from_query_syntax() {
        let input = ArtifactProcessorInput {
            artifact_id: "a3".to_string(),
            import_id: "i1".to_string(),
            artifact_class: ArtifactClass::Document,
            source_type: SourceType::ObsidianVault,
            title: Some("Notes Dashboard".to_string()),
            imported_note_metadata: Some(ImportedNoteMetadata {
                note_path: Some("Notes/Notes Dashboard.md".to_string()),
                ..ImportedNoteMetadata::default()
            }),
            participants: Vec::new(),
            segments: vec![segment(
                0,
                None,
                "# Notes Dashboard\n```dataview\nTABLE file.link\nFROM #note\n```\n<% tp.file.cursor(1) %>",
            )],
        };

        let profile = classify_artifact(&input);
        assert_eq!(
            profile.primary_archetype,
            ArtifactArchetype::DashboardTemplate
        );
        assert!(profile.facets.contains(&ArtifactFacet::Automation));
    }

    #[test]
    fn does_not_treat_descriptive_plugin_mentions_as_dashboard_syntax() {
        let input = ArtifactProcessorInput {
            artifact_id: "a5".to_string(),
            import_id: "i1".to_string(),
            artifact_class: ArtifactClass::Document,
            source_type: SourceType::MarkdownFile,
            title: Some("Obsidian Starter Vault by SoRobby".to_string()),
            imported_note_metadata: None,
            participants: Vec::new(),
            segments: vec![segment(
                0,
                None,
                "# Obsidian Starter Vault by SoRobby\n\nThis starter vault uses the Minimal Theme and recommends the Dataview and Templater plugins.\n\n## Setup\nInstall the community plugins and configure your API key before using the project templates.",
            )],
        };

        let profile = classify_artifact(&input);
        assert_ne!(
            profile.primary_archetype,
            ArtifactArchetype::DashboardTemplate
        );
        assert!(profile.facets.contains(&ArtifactFacet::Setup));
        assert!(profile.facets.contains(&ArtifactFacet::Technical));
    }

    #[test]
    fn classifies_procedural_documents() {
        let input = ArtifactProcessorInput {
            artifact_id: "a4".to_string(),
            import_id: "i1".to_string(),
            artifact_class: ArtifactClass::Document,
            source_type: SourceType::MarkdownFile,
            title: Some("Deployment setup".to_string()),
            imported_note_metadata: None,
            participants: Vec::new(),
            segments: vec![segment(
                0,
                None,
                "# Setup\n1. Install Docker\n2. Clone the repo\n3. Run `docker compose up`\nOA_API_KEY=change-me",
            )],
        };

        let profile = classify_artifact(&input);
        assert_eq!(profile.primary_archetype, ArtifactArchetype::ProceduralNote);
        assert!(profile.facets.contains(&ArtifactFacet::Technical));
        assert!(profile.facets.contains(&ArtifactFacet::Setup));
    }

    #[test]
    fn classifies_inbox_hub_notes_as_dashboards() {
        let input = ArtifactProcessorInput {
            artifact_id: "a6".to_string(),
            import_id: "i1".to_string(),
            artifact_class: ArtifactClass::Document,
            source_type: SourceType::ObsidianVault,
            title: Some("Inbox".to_string()),
            imported_note_metadata: Some(ImportedNoteMetadata {
                note_path: Some("Vault/Inbox/Inbox.md".to_string()),
                ..ImportedNoteMetadata::default()
            }),
            participants: Vec::new(),
            segments: vec![segment(
                0,
                None,
                "# Inbox\n[[Today]]\n[[Projects]]\n[[Notes]]\n[[Journal]]",
            )],
        };

        let profile = classify_artifact(&input);
        assert_eq!(
            profile.primary_archetype,
            ArtifactArchetype::DashboardTemplate
        );
    }

    #[test]
    fn classifies_dated_meeting_notes_as_working_notes() {
        let input = ArtifactProcessorInput {
            artifact_id: "a7".to_string(),
            import_id: "i1".to_string(),
            artifact_class: ArtifactClass::Document,
            source_type: SourceType::ObsidianVault,
            title: Some("2023-10-29 Example Meeting Note".to_string()),
            imported_note_metadata: Some(ImportedNoteMetadata {
                note_path: Some("Meetings/2023-10-29 Example Meeting Note.md".to_string()),
                properties: vec![ImportedNotePropertyRecord {
                    property_key: "status".to_string(),
                    value_kind: crate::storage::types::ImportedNotePropertyValueKind::String,
                    value_text: Some("open".to_string()),
                    value_json: serde_json::Value::String("open".to_string()),
                }],
                ..ImportedNoteMetadata::default()
            }),
            participants: Vec::new(),
            segments: vec![segment(
                0,
                None,
                "# Agenda\nDiscuss launch blockers and next steps.\n\n## Decision\nKeep the current scope.\n\n## Action items\n- Follow up on propulsion vendor timeline.",
            )],
        };

        let profile = classify_artifact(&input);
        assert_eq!(profile.primary_archetype, ArtifactArchetype::WorkingNote);
        assert!(profile.facets.contains(&ArtifactFacet::Project));
    }

    #[test]
    fn classifies_explicit_meeting_document_type_as_working_note() {
        let input = ArtifactProcessorInput {
            artifact_id: "a9".to_string(),
            import_id: "i1".to_string(),
            artifact_class: ArtifactClass::Document,
            source_type: SourceType::ObsidianVault,
            title: Some("2023-10-29 - Example Meeting Note".to_string()),
            imported_note_metadata: Some(ImportedNoteMetadata {
                note_path: Some(
                    "Projects/Top Secret Project/Meetings/2023-10-29 - Example Meeting Note.md"
                        .to_string(),
                ),
                properties: vec![ImportedNotePropertyRecord {
                    property_key: "document_type".to_string(),
                    value_kind: crate::storage::types::ImportedNotePropertyValueKind::String,
                    value_text: Some("meeting".to_string()),
                    value_json: serde_json::Value::String("meeting".to_string()),
                }],
                tags: vec![ImportedNoteTagRecord {
                    raw_tag: "#meeting".to_string(),
                    normalized_tag: "meeting".to_string(),
                    tag_path: "meeting".to_string(),
                    source_kind: ImportedNoteTagSourceKind::Inline,
                }],
                aliases: Vec::new(),
                outbound_links: Vec::new(),
            }),
            participants: Vec::new(),
            segments: vec![segment(
                0,
                None,
                "# Overview\nDescription:: Example Meeting Note\n\n## Attendees\n- SoRobby\n\n## Notes\n- Example note\n\n## Action Items\n*Add follow up actions / tasks*",
            )],
        };

        let profile = classify_artifact(&input);
        assert_eq!(profile.primary_archetype, ArtifactArchetype::WorkingNote);
        assert!(profile.facets.contains(&ArtifactFacet::Project));
    }

    #[test]
    fn classifies_import_guides_as_procedural_notes() {
        let input = ArtifactProcessorInput {
            artifact_id: "a8".to_string(),
            import_id: "i1".to_string(),
            artifact_class: ArtifactClass::Document,
            source_type: SourceType::MarkdownFile,
            title: Some("Import notes".to_string()),
            imported_note_metadata: None,
            participants: Vec::new(),
            segments: vec![segment(
                0,
                None,
                "# Import notes\n1. Open Settings.\n2. Choose Importer.\n3. Import your files.\n\n- [ ] Review unsupported file types.",
            )],
        };

        let profile = classify_artifact(&input);
        assert_eq!(profile.primary_archetype, ArtifactArchetype::ProceduralNote);
        assert!(profile.facets.contains(&ArtifactFacet::Setup));
    }

    fn segment(
        sequence_no: i32,
        participant_role: Option<ParticipantRole>,
        text: &str,
    ) -> LoadedSegment {
        LoadedSegment {
            segment_id: format!("seg-{sequence_no}"),
            participant_id: None,
            participant_role,
            sequence_no,
            text_content: text.to_string(),
            created_at_source: None,
            visibility_status: VisibilityStatus::Visible,
        }
    }
}
