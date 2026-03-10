use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::domain::SourceTimestamp;
use crate::error::OpenArchiveError;
use crate::parser::{
    self, ParsedConversation, ParsedMessage, CHATGPT_CONTENT_FACETS, CHATGPT_CONTENT_HASH_VERSION,
    CHATGPT_NORMALIZATION_VERSION,
};
use crate::storage::{
    ArtifactClass, ArtifactStatus, EnrichmentStatus, ImportStatus, ImportWriteStore, JobStatus,
    JobType, NewArtifact, NewEnrichmentJob, NewImport, NewImportPayload, NewParticipant,
    NewSegment, PayloadFormat, SegmentType, SourceType, VisibilityStatus, WriteArtifactSet,
    WriteImportSet,
};

static ID_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ImportArtifactStatus {
    pub artifact_id: String,
    pub enrichment_status: String,
    pub ingest_result: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ImportResponse {
    pub import_id: String,
    pub import_status: String,
    pub artifacts: Vec<ImportArtifactStatus>,
}

pub fn import_chatgpt_payload<S>(
    store: &S,
    payload_bytes: &[u8],
) -> Result<ImportResponse, OpenArchiveError>
where
    S: ImportWriteStore,
{
    let parsed = parser::chatgpt::parse_conversations(payload_bytes)?;
    let import_set = build_import_set(payload_bytes, parsed)?;
    let result = store.write_import(import_set)?;

    Ok(ImportResponse {
        import_id: result.import_id,
        import_status: result.import_status.as_str().to_string(),
        artifacts: result
            .artifacts
            .into_iter()
            .map(|artifact| ImportArtifactStatus {
                artifact_id: artifact.artifact_id,
                enrichment_status: artifact.enrichment_status.as_str().to_string(),
                ingest_result: artifact.ingest_result.as_str().to_string(),
            })
            .collect(),
    })
}

fn build_import_set(
    payload_bytes: &[u8],
    conversations: Vec<ParsedConversation>,
) -> Result<WriteImportSet, OpenArchiveError> {
    let payload_sha256 = sha256_hex(payload_bytes);
    let payload_id = new_id("payload");
    let import_id = new_id("import");
    let content_facets_json = serde_json::to_string(CHATGPT_CONTENT_FACETS).map_err(|err| {
        OpenArchiveError::Invariant(format!("failed to serialize content facets: {err}"))
    })?;

    let artifact_sets = conversations
        .into_iter()
        .map(|conversation| build_artifact_set(&import_id, &content_facets_json, conversation))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(WriteImportSet {
        payload: NewImportPayload {
            payload_id: payload_id.clone(),
            payload_format: PayloadFormat::ChatGptExportJson,
            payload_mime_type: "application/json".to_string(),
            payload_bytes: payload_bytes.to_vec(),
            payload_size_bytes: payload_bytes.len() as i64,
            payload_sha256: payload_sha256.clone(),
        },
        import: NewImport {
            import_id,
            source_type: SourceType::ChatGptExport,
            import_status: ImportStatus::Pending,
            payload_id,
            source_filename: None,
            source_content_hash: payload_sha256,
            conversation_count_detected: artifact_sets.len() as i64,
        },
        artifact_sets,
    })
}

fn build_artifact_set(
    import_id: &str,
    content_facets_json: &str,
    conversation: ParsedConversation,
) -> Result<WriteArtifactSet, OpenArchiveError> {
    let artifact_id = new_id("artifact");
    let mut participant_ids_by_key = HashMap::with_capacity(conversation.participants.len());

    let participants = conversation
        .participants
        .into_iter()
        .map(|participant| {
            let participant_id = new_id("participant");
            participant_ids_by_key.insert(participant.source_key.clone(), participant_id.clone());
            NewParticipant {
                participant_id,
                artifact_id: artifact_id.clone(),
                participant_role: participant.role,
                display_name: participant.display_name,
                provider_name: None,
                model_name: participant.model_name,
                source_participant_key: Some(participant.source_key),
                sequence_no: participant.sequence_no,
            }
        })
        .collect::<Vec<_>>();

    let segments = conversation
        .messages
        .into_iter()
        .enumerate()
        .map(|(sequence_no, message)| {
            build_segment(&artifact_id, &participant_ids_by_key, sequence_no, message)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let created_at_source = conversation.create_time.map(SourceTimestamp::from);
    let ended_at = conversation.update_time.map(SourceTimestamp::from);

    Ok(WriteArtifactSet {
        artifact: NewArtifact {
            artifact_id: artifact_id.clone(),
            import_id: import_id.to_string(),
            artifact_class: ArtifactClass::Conversation,
            source_type: SourceType::ChatGptExport,
            artifact_status: ArtifactStatus::Captured,
            enrichment_status: EnrichmentStatus::Pending,
            source_conversation_key: Some(conversation.source_id.clone()),
            source_conversation_hash: conversation.content_hash,
            title: conversation.title,
            created_at_source: created_at_source.clone(),
            started_at: created_at_source,
            ended_at,
            primary_language: None,
            content_hash_version: CHATGPT_CONTENT_HASH_VERSION.to_string(),
            content_facets_json: content_facets_json.to_string(),
            normalization_version: CHATGPT_NORMALIZATION_VERSION.to_string(),
        },
        participants,
        segments,
        job: NewEnrichmentJob {
            job_id: new_id("job"),
            artifact_id: artifact_id.clone(),
            job_type: JobType::ConversationEnrichment,
            job_status: JobStatus::Pending,
            max_attempts: 3,
            priority_no: 100,
            payload_json: serde_json::json!({
                "job_type": JobType::ConversationEnrichment.as_str(),
                "import_id": import_id,
                "artifact_id": artifact_id,
                "source_type": SourceType::ChatGptExport.as_str()
            })
            .to_string(),
        },
    })
}

fn build_segment(
    artifact_id: &str,
    participant_ids_by_key: &HashMap<String, String>,
    sequence_no: usize,
    message: ParsedMessage,
) -> Result<NewSegment, OpenArchiveError> {
    let participant_id = participant_ids_by_key
        .get(&message.participant_key)
        .cloned()
        .ok_or_else(|| {
            OpenArchiveError::Invariant(format!(
                "missing participant id for message participant key {}",
                message.participant_key
            ))
        })?;

    let unsupported_content_json = if message.unsupported_content.is_empty() {
        None
    } else {
        Some(
            serde_json::to_string(&message.unsupported_content).map_err(|err| {
                OpenArchiveError::Invariant(format!(
                    "failed to serialize unsupported content for node {}: {err}",
                    message.source_node_id
                ))
            })?,
        )
    };

    Ok(NewSegment {
        segment_id: new_id("segment"),
        artifact_id: artifact_id.to_string(),
        participant_id: Some(participant_id),
        segment_type: SegmentType::Message,
        source_segment_key: Some(message.source_node_id.clone()),
        parent_segment_id: None,
        sequence_no: sequence_no as i64,
        created_at_source: message.create_time.map(SourceTimestamp::from),
        text_content_hash: sha256_hex(message.text_content.as_bytes()),
        text_content: message.text_content,
        locator_json: Some(
            serde_json::json!({ "source_node_id": message.source_node_id }).to_string(),
        ),
        visibility_status: map_visibility(message.visibility),
        unsupported_content_json,
    })
}

fn map_visibility(visibility: VisibilityStatus) -> VisibilityStatus {
    match visibility {
        VisibilityStatus::Visible => VisibilityStatus::Visible,
        VisibilityStatus::Hidden => VisibilityStatus::Hidden,
        VisibilityStatus::SkippedUnsupported => VisibilityStatus::SkippedUnsupported,
    }
}

fn new_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{nanos:x}-{counter:x}")
}

fn sha256_hex(input: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{
        ArtifactIngestResult, EnrichmentStatus, ImportWriteResult, ImportedArtifact, WriteImportSet,
    };
    use std::sync::Mutex;

    struct MockStore {
        captured: Mutex<Vec<WriteImportSet>>,
    }

    impl MockStore {
        fn new() -> Self {
            Self {
                captured: Mutex::new(Vec::new()),
            }
        }
    }

    impl ImportWriteStore for MockStore {
        fn write_import(
            &self,
            import_set: WriteImportSet,
        ) -> crate::error::StorageResult<ImportWriteResult> {
            let artifact_ids = import_set
                .artifact_sets
                .iter()
                .map(|set| set.artifact.artifact_id.clone())
                .collect::<Vec<_>>();
            self.captured.lock().unwrap().push(import_set);
            Ok(ImportWriteResult {
                import_id: "import-test".to_string(),
                import_status: ImportStatus::Completed,
                artifacts: artifact_ids
                    .into_iter()
                    .map(|artifact_id| ImportedArtifact {
                        artifact_id,
                        enrichment_status: EnrichmentStatus::Pending,
                        ingest_result: ArtifactIngestResult::Created,
                    })
                    .collect(),
                failed_artifact_ids: Vec::new(),
                segments_written: 0,
            })
        }
    }

    #[test]
    fn import_chatgpt_payload_returns_compact_response() {
        let store = MockStore::new();
        let response =
            import_chatgpt_payload(&store, single_conversation_export().as_bytes()).unwrap();

        assert_eq!(response.import_id, "import-test");
        assert_eq!(response.import_status, "completed");
        assert_eq!(response.artifacts.len(), 1);
        assert_eq!(response.artifacts[0].enrichment_status, "pending");
        assert_eq!(response.artifacts[0].ingest_result, "created");
    }

    #[test]
    fn import_chatgpt_payload_builds_multi_conversation_write_set() {
        let store = MockStore::new();
        import_chatgpt_payload(&store, multi_conversation_export().as_bytes()).unwrap();

        let captured = store.captured.lock().unwrap();
        let import_set = captured.first().unwrap();
        assert_eq!(import_set.import.conversation_count_detected, 2);
        assert_eq!(import_set.artifact_sets.len(), 2);
        assert_eq!(
            import_set.artifact_sets[0].job.job_status,
            JobStatus::Pending
        );
        assert_eq!(
            import_set.artifact_sets[1].artifact.enrichment_status,
            EnrichmentStatus::Pending
        );
    }

    fn single_conversation_export() -> &'static str {
        r#"[{
          "id": "conv-1",
          "title": "First",
          "create_time": 1710000000,
          "update_time": 1710000060,
          "current_node": "m2",
          "mapping": {
            "root": {"id": "root", "message": null, "parent": null, "children": ["m1"]},
            "m1": {
              "id": "m1",
              "parent": "root",
              "children": ["m2"],
              "message": {
                "author": {"role": "user", "name": "David"},
                "create_time": 1710000001,
                "content": {"content_type": "text", "parts": ["Hello"]},
                "metadata": {}
              }
            },
            "m2": {
              "id": "m2",
              "parent": "m1",
              "children": [],
              "message": {
                "author": {"role": "assistant", "name": "ChatGPT"},
                "create_time": 1710000002,
                "content": {"content_type": "text", "parts": ["Hi"]},
                "metadata": {"model_slug": "gpt-4"}
              }
            }
          },
          "default_model_slug": "gpt-4"
        }]"#
    }

    fn multi_conversation_export() -> &'static str {
        r#"[{
          "id": "conv-1",
          "title": "First",
          "create_time": 1710000000,
          "update_time": 1710000060,
          "current_node": "m1",
          "mapping": {
            "root": {"id": "root", "message": null, "parent": null, "children": ["m1"]},
            "m1": {
              "id": "m1",
              "parent": "root",
              "children": [],
              "message": {
                "author": {"role": "user", "name": "David"},
                "create_time": 1710000001,
                "content": {"content_type": "text", "parts": ["One"]},
                "metadata": {}
              }
            }
          },
          "default_model_slug": null
        },
        {
          "id": "conv-2",
          "title": "Second",
          "create_time": 1710000100,
          "update_time": 1710000160,
          "current_node": "n2",
          "mapping": {
            "root2": {"id": "root2", "message": null, "parent": null, "children": ["n1"]},
            "n1": {
              "id": "n1",
              "parent": "root2",
              "children": ["n2"],
              "message": {
                "author": {"role": "user", "name": "David"},
                "create_time": 1710000101,
                "content": {"content_type": "text", "parts": ["Two"]},
                "metadata": {}
              }
            },
            "n2": {
              "id": "n2",
              "parent": "n1",
              "children": [],
              "message": {
                "author": {"role": "assistant", "name": "ChatGPT"},
                "create_time": 1710000102,
                "content": {"content_type": "text", "parts": ["Three"]},
                "metadata": {}
              }
            }
          },
          "default_model_slug": null
        }]"#
    }

    #[test]
    fn import_chatgpt_payload_rejects_malformed_json() {
        let store = MockStore::new();
        let err = import_chatgpt_payload(&store, br#"{"bad":true}"#).unwrap_err();
        assert!(matches!(err, OpenArchiveError::Parser(_)));
        assert!(
            store.captured.lock().unwrap().is_empty(),
            "parser failures must not call the storage boundary"
        );
    }

    #[test]
    fn import_chatgpt_payload_parser_failure_does_not_mutate_prior_store_state() {
        let store = MockStore::new();
        import_chatgpt_payload(&store, single_conversation_export().as_bytes()).unwrap();

        let err = import_chatgpt_payload(&store, br#"{"bad":true}"#).unwrap_err();
        assert!(matches!(err, OpenArchiveError::Parser(_)));

        let captured = store.captured.lock().unwrap();
        assert_eq!(
            captured.len(),
            1,
            "malformed payloads must not enqueue a second write"
        );
    }
}
