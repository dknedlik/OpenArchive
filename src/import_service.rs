use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::domain::SourceTimestamp;
use crate::error::OpenArchiveError;
use crate::object_store::{NewObject, ObjectStore, PutObjectResult};
use crate::parser::{self, ParsedConversation, ParsedMessage};
use crate::storage::{
    ArtifactClass, ArtifactExtractPayload, ArtifactStatus, EnrichmentStatus, ImportStatus,
    ImportWriteStore, JobStatus, JobType, NewArtifact, NewEnrichmentJob, NewImport,
    NewImportObjectRef, NewParticipant, NewSegment, PayloadFormat, SegmentType, SourceType,
    WriteArtifactSet, WriteImportSet,
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

#[derive(Debug, Clone, Copy)]
struct SourceImportSpec {
    source_type: SourceType,
    payload_format: PayloadFormat,
    normalization_version: &'static str,
    content_hash_version: &'static str,
    content_facets: &'static [&'static str],
}

pub fn import_payload<S, O>(
    store: &S,
    object_store: &O,
    source_type: SourceType,
    payload_format: PayloadFormat,
    payload_bytes: &[u8],
) -> Result<ImportResponse, OpenArchiveError>
where
    S: ImportWriteStore + ?Sized,
    O: ObjectStore + ?Sized,
{
    let spec = source_import_spec(source_type, payload_format);
    let parsed = parse_conversations_for_source(source_type, payload_bytes)?;
    let prepared = build_import_set(payload_bytes, spec, parsed, object_store)?;
    let result = match store.write_import(prepared.write_set) {
        Ok(result) => result,
        Err(err) => {
            cleanup_unreferenced_payload_object(object_store, &prepared.payload_put, &err);
            return Err(err.into());
        }
    };

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

pub fn import_chatgpt_payload<S, O>(
    store: &S,
    object_store: &O,
    payload_bytes: &[u8],
) -> Result<ImportResponse, OpenArchiveError>
where
    S: ImportWriteStore + ?Sized,
    O: ObjectStore + ?Sized,
{
    import_payload(
        store,
        object_store,
        SourceType::ChatGptExport,
        PayloadFormat::ChatGptExportJson,
        payload_bytes,
    )
}

pub fn import_claude_payload<S, O>(
    store: &S,
    object_store: &O,
    payload_bytes: &[u8],
) -> Result<ImportResponse, OpenArchiveError>
where
    S: ImportWriteStore + ?Sized,
    O: ObjectStore + ?Sized,
{
    import_payload(
        store,
        object_store,
        SourceType::ClaudeExport,
        PayloadFormat::ClaudeExportJson,
        payload_bytes,
    )
}

pub fn import_grok_payload<S, O>(
    store: &S,
    object_store: &O,
    payload_bytes: &[u8],
) -> Result<ImportResponse, OpenArchiveError>
where
    S: ImportWriteStore + ?Sized,
    O: ObjectStore + ?Sized,
{
    import_payload(
        store,
        object_store,
        SourceType::GrokExport,
        PayloadFormat::GrokExportJson,
        payload_bytes,
    )
}

pub fn import_gemini_payload<S, O>(
    store: &S,
    object_store: &O,
    payload_bytes: &[u8],
) -> Result<ImportResponse, OpenArchiveError>
where
    S: ImportWriteStore + ?Sized,
    O: ObjectStore + ?Sized,
{
    import_payload(
        store,
        object_store,
        SourceType::GeminiTakeout,
        PayloadFormat::GeminiTakeoutJson,
        payload_bytes,
    )
}

struct PreparedImportSet {
    write_set: WriteImportSet,
    payload_put: PutObjectResult,
}

fn build_import_set(
    payload_bytes: &[u8],
    spec: SourceImportSpec,
    conversations: Vec<ParsedConversation>,
    object_store: &(impl ObjectStore + ?Sized),
) -> Result<PreparedImportSet, OpenArchiveError> {
    let payload_sha256 = sha256_hex(payload_bytes);
    let payload_object_id = new_id("payload");
    let import_id = new_id("import");
    let payload_put = persist_raw_payload(
        object_store,
        &payload_object_id,
        payload_bytes,
        &payload_sha256,
    )?;
    let content_facets_json = serde_json::to_string(spec.content_facets).map_err(|err| {
        OpenArchiveError::Invariant(format!("failed to serialize content facets: {err}"))
    })?;

    let artifact_sets = conversations
        .into_iter()
        .map(|conversation| {
            build_artifact_set(&import_id, &content_facets_json, spec, conversation)
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(PreparedImportSet {
        write_set: WriteImportSet {
            payload_object: NewImportObjectRef {
                object_id: payload_object_id.clone(),
                payload_format: spec.payload_format,
                mime_type: "application/json".to_string(),
                size_bytes: payload_bytes.len() as i64,
                sha256: payload_sha256.clone(),
                stored_object: payload_put.stored_object.clone(),
            },
            import: NewImport {
                import_id,
                source_type: spec.source_type,
                import_status: ImportStatus::Pending,
                payload_object_id,
                source_filename: None,
                source_content_hash: payload_sha256,
                conversation_count_detected: artifact_sets.len() as i32,
            },
            artifact_sets,
        },
        payload_put,
    })
}

fn persist_raw_payload(
    object_store: &(impl ObjectStore + ?Sized),
    object_id: &str,
    payload_bytes: &[u8],
    payload_sha256: &str,
) -> Result<PutObjectResult, OpenArchiveError> {
    object_store
        .put_object(NewObject {
            object_id: object_id.to_string(),
            namespace: "import-payloads".to_string(),
            mime_type: "application/json".to_string(),
            bytes: payload_bytes.to_vec(),
            sha256: payload_sha256.to_string(),
        })
        .map_err(OpenArchiveError::from)
}

fn cleanup_unreferenced_payload_object(
    object_store: &(impl ObjectStore + ?Sized),
    payload_put: &PutObjectResult,
    error: &crate::error::StorageError,
) {
    if !payload_put.was_created || !is_safe_precommit_import_failure(error) {
        return;
    }

    if let Err(cleanup_err) = object_store.delete_object(&payload_put.stored_object) {
        log::warn!(
            "failed to clean up unreferenced payload object {} after import write error: {}",
            payload_put.stored_object.object_id,
            cleanup_err
        );
    }
}

fn is_safe_precommit_import_failure(error: &crate::error::StorageError) -> bool {
    matches!(
        error,
        crate::error::StorageError::InsertPayload { .. }
            | crate::error::StorageError::InsertImport { .. }
    )
}

fn build_artifact_set(
    import_id: &str,
    content_facets_json: &str,
    spec: SourceImportSpec,
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
                sequence_no: participant.sequence_no as i32,
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
            source_type: spec.source_type,
            artifact_status: ArtifactStatus::Captured,
            enrichment_status: EnrichmentStatus::Pending,
            source_conversation_key: Some(conversation.source_id.clone()),
            source_conversation_hash: conversation.content_hash,
            title: conversation.title,
            created_at_source: created_at_source.clone(),
            started_at: created_at_source,
            ended_at,
            primary_language: None,
            content_hash_version: spec.content_hash_version.to_string(),
            content_facets_json: content_facets_json.to_string(),
            normalization_version: spec.normalization_version.to_string(),
        },
        participants,
        segments,
        job: NewEnrichmentJob {
            job_id: new_id("job"),
            artifact_id: artifact_id.clone(),
            job_type: JobType::ArtifactExtract,
            enrichment_tier: crate::storage::EnrichmentTier::Standard,
            spawned_by_job_id: None,
            job_status: JobStatus::Pending,
            max_attempts: 3,
            priority_no: 100,
            required_capabilities: vec!["text".to_string()],
            payload_json: ArtifactExtractPayload::new_v1(
                &artifact_id,
                import_id,
                spec.source_type,
                Vec::new(),
                Vec::new(),
            )
            .to_json(),
        },
    })
}

fn parse_conversations_for_source(
    source_type: SourceType,
    payload_bytes: &[u8],
) -> Result<Vec<ParsedConversation>, OpenArchiveError> {
    match source_type {
        SourceType::ChatGptExport => parser::chatgpt::parse_conversations(payload_bytes),
        SourceType::ClaudeExport => parser::claude::parse_conversations(payload_bytes),
        SourceType::GrokExport => parser::grok::parse_conversations(payload_bytes),
        SourceType::GeminiTakeout => parser::gemini::parse_conversations(payload_bytes),
    }
    .map_err(OpenArchiveError::from)
}

fn source_import_spec(source_type: SourceType, payload_format: PayloadFormat) -> SourceImportSpec {
    match source_type {
        SourceType::ChatGptExport => SourceImportSpec {
            source_type,
            payload_format,
            normalization_version: parser::CHATGPT_NORMALIZATION_VERSION,
            content_hash_version: parser::CHATGPT_CONTENT_HASH_VERSION,
            content_facets: parser::CHATGPT_CONTENT_FACETS,
        },
        SourceType::ClaudeExport => SourceImportSpec {
            source_type,
            payload_format,
            normalization_version: parser::claude::CLAUDE_NORMALIZATION_VERSION,
            content_hash_version: parser::claude::CLAUDE_CONTENT_HASH_VERSION,
            content_facets: parser::claude::CLAUDE_CONTENT_FACETS,
        },
        SourceType::GrokExport => SourceImportSpec {
            source_type,
            payload_format,
            normalization_version: parser::grok::GROK_NORMALIZATION_VERSION,
            content_hash_version: parser::grok::GROK_CONTENT_HASH_VERSION,
            content_facets: parser::grok::GROK_CONTENT_FACETS,
        },
        SourceType::GeminiTakeout => SourceImportSpec {
            source_type,
            payload_format,
            normalization_version: parser::gemini::GEMINI_NORMALIZATION_VERSION,
            content_hash_version: parser::gemini::GEMINI_CONTENT_HASH_VERSION,
            content_facets: parser::gemini::GEMINI_CONTENT_FACETS,
        },
    }
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
        sequence_no: sequence_no as i32,
        created_at_source: message.create_time.map(SourceTimestamp::from),
        text_content_hash: sha256_hex(message.text_content.as_bytes()),
        text_content: message.text_content,
        locator_json: Some(
            serde_json::json!({ "source_node_id": message.source_node_id }).to_string(),
        ),
        visibility_status: message.visibility,
        unsupported_content_json,
    })
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
    use crate::error::ParserError;
    use crate::object_store::{NewObject, ObjectStore, PutObjectResult, StoredObject};
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

    struct FailingImportStore;

    impl ImportWriteStore for FailingImportStore {
        fn write_import(
            &self,
            import_set: WriteImportSet,
        ) -> crate::error::StorageResult<ImportWriteResult> {
            Err(crate::error::StorageError::InsertImport {
                import_id: import_set.import.import_id,
                source: Box::new(std::io::Error::other("synthetic precommit failure")),
            })
        }
    }

    struct MockObjectStore {
        puts: Mutex<Vec<NewObject>>,
        deletes: Mutex<Vec<StoredObject>>,
    }

    impl MockObjectStore {
        fn new() -> Self {
            Self {
                puts: Mutex::new(Vec::new()),
                deletes: Mutex::new(Vec::new()),
            }
        }
    }

    impl ObjectStore for MockObjectStore {
        fn put_object(
            &self,
            object: NewObject,
        ) -> crate::error::ObjectStoreResult<PutObjectResult> {
            let stored = StoredObject {
                object_id: object.object_id.clone(),
                provider: "mock".to_string(),
                storage_key: format!("{}/{}", object.namespace, object.sha256),
                mime_type: object.mime_type.clone(),
                size_bytes: object.bytes.len() as i64,
                sha256: object.sha256.clone(),
            };
            self.puts.lock().unwrap().push(object);
            Ok(PutObjectResult {
                stored_object: stored,
                was_created: true,
            })
        }

        fn get_object_bytes(
            &self,
            object: &StoredObject,
        ) -> crate::error::ObjectStoreResult<Vec<u8>> {
            Ok(object.storage_key.as_bytes().to_vec())
        }

        fn delete_object(&self, object: &StoredObject) -> crate::error::ObjectStoreResult<()> {
            self.deletes.lock().unwrap().push(object.clone());
            Ok(())
        }
    }

    #[test]
    fn import_chatgpt_payload_returns_compact_response() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();
        let response = import_chatgpt_payload(
            &store,
            &object_store,
            single_conversation_export().as_bytes(),
        )
        .unwrap();

        assert_eq!(response.import_id, "import-test");
        assert_eq!(response.import_status, "completed");
        assert_eq!(response.artifacts.len(), 1);
        assert_eq!(response.artifacts[0].enrichment_status, "pending");
        assert_eq!(response.artifacts[0].ingest_result, "created");
    }

    #[test]
    fn import_chatgpt_payload_builds_multi_conversation_write_set() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();
        import_chatgpt_payload(
            &store,
            &object_store,
            multi_conversation_export().as_bytes(),
        )
        .unwrap();

        let captured = store.captured.lock().unwrap();
        let import_set = captured.first().unwrap();
        assert_eq!(import_set.import.conversation_count_detected, 2);
        assert_eq!(import_set.artifact_sets.len(), 2);
        assert_eq!(
            import_set.import.payload_object_id,
            import_set.payload_object.object_id
        );
        assert_eq!(
            import_set.artifact_sets[0].job.job_status,
            JobStatus::Pending
        );
        assert_eq!(
            import_set.artifact_sets[1].artifact.enrichment_status,
            EnrichmentStatus::Pending
        );

        // Verify job payload matches ArtifactExtractPayload v1 contract
        for artifact_set in &import_set.artifact_sets {
            let payload =
                crate::storage::ArtifactExtractPayload::from_json(&artifact_set.job.payload_json)
                    .expect("payload must deserialize to ArtifactExtractPayload");
            assert_eq!(payload.schema_version, "1");
            assert_eq!(
                payload.source_type,
                super::SourceType::ChatGptExport.as_str()
            );
            assert!(payload.conversation_windows.is_empty());
            assert!(payload.topic_threads.is_empty());
        }
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
        let object_store = MockObjectStore::new();
        let err = import_chatgpt_payload(&store, &object_store, br#"{"bad":true}"#).unwrap_err();
        assert!(matches!(err, OpenArchiveError::Parser(_)));
        assert!(
            store.captured.lock().unwrap().is_empty(),
            "parser failures must not call the storage boundary"
        );
    }

    #[test]
    fn import_chatgpt_payload_parser_failure_does_not_mutate_prior_store_state() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();
        import_chatgpt_payload(
            &store,
            &object_store,
            single_conversation_export().as_bytes(),
        )
        .unwrap();

        let err = import_chatgpt_payload(&store, &object_store, br#"{"bad":true}"#).unwrap_err();
        assert!(matches!(err, OpenArchiveError::Parser(_)));

        let captured = store.captured.lock().unwrap();
        assert_eq!(
            captured.len(),
            1,
            "malformed payloads must not enqueue a second write"
        );
    }

    #[test]
    fn import_chatgpt_payload_cleans_up_new_object_on_safe_precommit_failure() {
        let store = FailingImportStore;
        let object_store = MockObjectStore::new();

        let err = import_chatgpt_payload(
            &store,
            &object_store,
            single_conversation_export().as_bytes(),
        )
        .unwrap_err();

        assert!(matches!(err, OpenArchiveError::Storage(_)));
        assert_eq!(object_store.puts.lock().unwrap().len(), 1);
        assert_eq!(object_store.deletes.lock().unwrap().len(), 1);
    }

    #[test]
    fn import_claude_payload_uses_claude_source_metadata() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();

        let response = import_claude_payload(
            &store,
            &object_store,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/claude_export.json"
            )),
        )
        .unwrap();

        assert_eq!(response.artifacts.len(), 1);
        let captured = store.captured.lock().unwrap();
        let import_set = captured.last().unwrap();
        assert_eq!(import_set.import.source_type, SourceType::ClaudeExport);
        assert_eq!(
            import_set.payload_object.payload_format,
            PayloadFormat::ClaudeExportJson
        );
        assert_eq!(
            import_set.artifact_sets[0].artifact.normalization_version,
            parser::claude::CLAUDE_NORMALIZATION_VERSION
        );
    }

    #[test]
    fn import_grok_payload_uses_grok_source_metadata() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();

        let response = import_grok_payload(
            &store,
            &object_store,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/grok_export.json"
            )),
        )
        .unwrap();

        assert_eq!(response.artifacts.len(), 1);
        let captured = store.captured.lock().unwrap();
        let import_set = captured.last().unwrap();
        assert_eq!(import_set.import.source_type, SourceType::GrokExport);
        assert_eq!(
            import_set.payload_object.payload_format,
            PayloadFormat::GrokExportJson
        );
        assert_eq!(
            import_set.artifact_sets[0].artifact.content_hash_version,
            parser::grok::GROK_CONTENT_HASH_VERSION
        );
    }

    #[test]
    fn import_gemini_payload_skips_empty_items_and_imports_usable_entries() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();

        let response = import_gemini_payload(
            &store,
            &object_store,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/gemini_export.json"
            )),
        )
        .unwrap();

        assert_eq!(response.artifacts.len(), 1);
        let captured = store.captured.lock().unwrap();
        let import_set = captured.last().unwrap();
        assert_eq!(import_set.import.source_type, SourceType::GeminiTakeout);
        assert_eq!(
            import_set.payload_object.payload_format,
            PayloadFormat::GeminiTakeoutJson
        );
        assert_eq!(import_set.artifact_sets[0].segments.len(), 2);
    }

    #[test]
    fn import_gemini_payload_rejects_payloads_without_usable_entries() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();
        let err = import_gemini_payload(
            &store,
            &object_store,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/gemini_empty_export.json"
            )),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            OpenArchiveError::Parser(ParserError::EmptyExport)
        ));
    }
}
