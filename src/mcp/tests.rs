use std::io::{self, Cursor};
use std::sync::Arc;

use serde_json::{json, Value};

use super::*;
use crate::app::artifact_detail::ArtifactDetailService;
use crate::app::context_pack::ContextPackService;
use crate::app::retrieval::ArchiveRetrievalService;
use crate::app::review::ReviewService;
use crate::app::search::ArchiveSearchService;
use crate::app::ArchiveApplication;
use crate::storage::{ArchiveRetrievalStore, RetrievalIntent, RetrievedContextItem};

fn test_app() -> ArchiveApplication {
    let search = Some(ArchiveSearchService::new(Arc::new(MockSearchReadStore)));
    let artifact_detail = Some(ArtifactDetailService::new(Arc::new(
        MockArtifactDetailStore,
    )));
    let context_pack = Some(ContextPackService::new(Arc::new(MockContextPackStore)));
    let object_search = Some(crate::app::search::ObjectSearchService::new(
        Arc::new(MockObjectSearchStore),
        None,
    ));
    let writeback = Some(crate::app::writeback::WritebackService::new(Arc::new(
        MockWritebackStore,
    )));
    let review = Some(ReviewService::new(Arc::new(MockReviewStore)));
    ArchiveApplication {
        artifacts: crate::app::artifacts::ArtifactQueryService::new(Arc::new(
            MockArtifactReadStore,
        )),
        imports: crate::app::imports::ImportApplicationService::new(
            Arc::new(MockImportWriteStore),
            Arc::new(MockObjectStore),
        ),
        retrieval: Arc::new(ArchiveRetrievalService::new(Arc::new(MockRetrievalStore))),
        search,
        artifact_detail,
        context_pack,
        object_search,
        review,
        writeback,
    }
}

#[test]
fn tools_call_returns_found_false_for_missing_artifact() {
    let response = call_tool(
        &test_app(),
        json!({
            "name": "get_artifact",
            "arguments": {
                "artifact_id": "missing"
            }
        }),
    );

    assert_eq!(response["isError"], Value::Bool(false));
    assert_eq!(
        response["structuredContent"],
        json!({
            "found": false
        })
    );
}

#[test]
fn search_archive_clamps_limit() {
    let response = call_tool(
        &test_app(),
        json!({
            "name": "search_archive",
            "arguments": {
                "query": "abc",
                "limit": 999
            }
        }),
    );

    assert_eq!(response["isError"], Value::Bool(false));
    assert!(response["structuredContent"]["hits"].is_array());
}

#[test]
fn store_memory_requires_artifact_id() {
    let response = call_tool(
        &test_app(),
        json!({
            "name": "store_memory",
            "arguments": {
                "title": "Memory",
                "body_text": "Remember this",
                "memory_type": "fact"
            }
        }),
    );

    assert_eq!(response["isError"], Value::Bool(true));
    assert_eq!(
        response["structuredContent"]["message"],
        "store_memory requires artifact_id"
    );
}

#[test]
fn link_objects_rejects_self_links() {
    let response = call_tool(
        &test_app(),
        json!({
            "name": "link_objects",
            "arguments": {
                "source_object_id": "dobj-1",
                "target_object_id": "dobj-1",
                "link_type": "same_as"
            }
        }),
    );

    assert_eq!(response["isError"], Value::Bool(true));
    assert_eq!(
        response["structuredContent"]["message"],
        "link_objects: source and target must be different objects"
    );
}

#[test]
fn read_jsonrpc_message_accepts_newline_delimited_json() {
    let mut reader = Cursor::new(
        br#"{"jsonrpc":"2.0","id":0,"method":"initialize","params":{}}
"#
        .to_vec(),
    );

    let request = read_jsonrpc_message(&mut reader)
        .expect("line-delimited request should parse")
        .expect("request should be present");

    assert_eq!(request.method, "initialize");
    assert_eq!(request.id, Some(json!(0)));
}

#[test]
fn read_jsonrpc_message_rejects_header_framed_input() {
    let payload = br#"{"jsonrpc":"2.0","id":0,"method":"initialize"}"#;
    let framed = format!("Content-Length: {}\r\n\r\n", payload.len()).into_bytes();
    let mut bytes = framed;
    bytes.extend_from_slice(payload);
    let mut reader = Cursor::new(bytes);

    let err = read_jsonrpc_message(&mut reader).expect_err("header framing should be rejected");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(err.to_string().contains("newline-delimited JSON-RPC input"));
}

#[test]
fn tool_success_uses_compact_summary_text() {
    let response = tool_success(json!({
        "results": [
            { "id": 1 },
            { "id": 2 }
        ]
    }));

    assert_eq!(response["content"][0]["text"], "2 results");
    assert_eq!(
        response["structuredContent"]["results"]
            .as_array()
            .map(Vec::len),
        Some(2)
    );
}

#[test]
fn get_artifact_summary_text_prefers_payload_shape_over_found_flag() {
    let response = call_tool(
        &test_app(),
        json!({
            "name": "get_artifact",
            "arguments": {
                "artifact_id": "artifact-1"
            }
        }),
    );

    assert_ne!(
        response["content"][0]["text"],
        Value::String("found".to_string())
    );
    assert_eq!(
        response["content"][0]["text"],
        Value::String(
            "artifact: 1 summaries, 1 classifications, 1 memories, 1 segments".to_string()
        )
    );
}

#[test]
fn get_context_pack_summary_text_prefers_payload_shape_over_found_flag() {
    let response = call_tool(
        &test_app(),
        json!({
            "name": "get_context_pack",
            "arguments": {
                "artifact_id": "artifact-1"
            }
        }),
    );

    assert_ne!(
        response["content"][0]["text"],
        Value::String("found".to_string())
    );
    assert_eq!(
        response["content"][0]["text"],
        Value::String(
            "context pack: 0 summaries, 0 classifications, 0 memories, 0 relationships".to_string()
        )
    );
}

#[test]
fn list_review_items_returns_items_with_compact_summary_text() {
    let response = call_tool(
        &test_app(),
        json!({
            "name": "list_review_items",
            "arguments": {
                "kinds": ["artifact_needs_attention"],
                "limit": 10
            }
        }),
    );

    assert_eq!(response["isError"], Value::Bool(false));
    assert_eq!(response["content"][0]["text"], "1 review items");
    assert_eq!(
        response["structuredContent"]["items"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );
}

#[test]
fn record_review_decision_surfaces_invalid_params() {
    let response = call_tool(
        &test_app(),
        json!({
            "name": "record_review_decision",
            "arguments": {
                "kind": "artifact_needs_attention",
                "artifact_id": "artifact-1",
                "decision_status": "noted"
            }
        }),
    );

    assert_eq!(response["isError"], Value::Bool(true));
    assert_eq!(
        response["structuredContent"]["message"],
        "review notes require non-empty note_text"
    );
}

#[test]
fn retry_artifact_enrichment_returns_job_id() {
    let response = call_tool(
        &test_app(),
        json!({
            "name": "retry_artifact_enrichment",
            "arguments": {
                "artifact_id": "artifact-9"
            }
        }),
    );

    assert_eq!(response["isError"], Value::Bool(false));
    assert_eq!(
        response["structuredContent"],
        json!({
            "queued": true,
            "job_id": "job-retry-artifact-9"
        })
    );
    assert_eq!(response["content"][0]["text"], "queued");
}

struct MockSearchReadStore;

impl crate::storage::ArchiveSearchReadStore for MockSearchReadStore {
    fn search_candidates(
        &self,
        _query_text: &str,
        _limit: usize,
        _filters: &crate::storage::SearchFilters,
    ) -> crate::error::StorageResult<Vec<crate::storage::ArchiveSearchCandidate>> {
        Ok(vec![crate::storage::ArchiveSearchCandidate {
            artifact_id: "artifact-1".to_string(),
            match_record_id: "artifact-1".to_string(),
            match_kind: crate::storage::SearchCandidateKind::ArtifactTitle,
            snippet: "title".to_string(),
            score_hint: 300,
        }])
    }
}

struct MockRetrievalStore;

impl ArchiveRetrievalStore for MockRetrievalStore {
    fn retrieve_for_intents(
        &self,
        _artifact_id: &str,
        _intents: &[RetrievalIntent],
        _limit_per_intent: usize,
    ) -> crate::error::StorageResult<Vec<RetrievedContextItem>> {
        Ok(Vec::new())
    }
}

struct MockArtifactDetailStore;

impl crate::storage::ArtifactDetailReadStore for MockArtifactDetailStore {
    fn load_artifact_detail(
        &self,
        artifact_id: &str,
    ) -> crate::error::StorageResult<Option<crate::storage::ArtifactDetailView>> {
        if artifact_id == "missing" {
            return Ok(None);
        }
        Ok(Some(crate::storage::ArtifactDetailView {
            artifact: crate::storage::ArtifactDetailRecord {
                artifact_id: artifact_id.to_string(),
                title: Some("Artifact".to_string()),
                source_type: crate::storage::SourceType::ChatGptExport,
                enrichment_status: crate::storage::EnrichmentStatus::Completed,
            },
            segments: vec![crate::storage::ArtifactDetailSegment {
                segment_id: "seg-1".to_string(),
                participant_id: None,
                participant_role: None,
                sequence_no: 1,
                text_content: "hello".to_string(),
            }],
            derived_objects: vec![
                crate::storage::ArtifactDetailDerivedObject {
                    derived_object_id: "sum-1".to_string(),
                    derived_object_type: crate::storage::DerivedObjectType::Summary,
                    title: Some("Summary".to_string()),
                    body_text: Some("Summary body".to_string()),
                    confidence_score: None,
                },
                crate::storage::ArtifactDetailDerivedObject {
                    derived_object_id: "cls-1".to_string(),
                    derived_object_type: crate::storage::DerivedObjectType::Classification,
                    title: Some("Classification".to_string()),
                    body_text: Some("Classification body".to_string()),
                    confidence_score: None,
                },
                crate::storage::ArtifactDetailDerivedObject {
                    derived_object_id: "mem-1".to_string(),
                    derived_object_type: crate::storage::DerivedObjectType::Memory,
                    title: Some("Memory".to_string()),
                    body_text: Some("Memory body".to_string()),
                    confidence_score: None,
                },
            ],
        }))
    }
}

struct MockContextPackStore;

impl crate::storage::ArtifactContextPackReadStore for MockContextPackStore {
    fn load_artifact_context_pack_material(
        &self,
        artifact_id: &str,
    ) -> crate::error::StorageResult<Option<crate::storage::ArtifactContextPackMaterial>> {
        if artifact_id == "missing" {
            return Ok(None);
        }
        Ok(Some(crate::storage::ArtifactContextPackMaterial {
            artifact: crate::storage::ArtifactDetailRecord {
                artifact_id: artifact_id.to_string(),
                title: Some("Artifact".to_string()),
                source_type: crate::storage::SourceType::ChatGptExport,
                enrichment_status: crate::storage::EnrichmentStatus::Completed,
            },
            segments: vec![],
            derived_objects: vec![],
            evidence_links: vec![],
        }))
    }
}

struct MockObjectSearchStore;

impl crate::storage::DerivedObjectSearchStore for MockObjectSearchStore {
    fn search_objects(
        &self,
        _filters: &crate::storage::ObjectSearchFilters,
        _limit: usize,
    ) -> crate::error::StorageResult<Vec<crate::storage::DerivedObjectSearchResult>> {
        Ok(vec![])
    }

    fn search_objects_by_embedding(
        &self,
        _filters: &crate::storage::ObjectSearchFilters,
        _query_embedding: &[f32],
        _limit: usize,
    ) -> crate::error::StorageResult<Vec<crate::storage::DerivedObjectSearchResult>> {
        Ok(vec![])
    }

    fn get_related_objects(
        &self,
        _derived_object_id: &str,
        _limit: usize,
    ) -> crate::error::StorageResult<Vec<crate::storage::GraphRelatedEntry>> {
        Ok(vec![])
    }
}

struct MockArtifactReadStore;

impl crate::storage::ArtifactReadStore for MockArtifactReadStore {
    fn list_artifacts(&self) -> crate::error::StorageResult<Vec<crate::storage::ArtifactListItem>> {
        Ok(vec![])
    }

    fn list_artifacts_filtered(
        &self,
        _filters: &crate::storage::ArtifactListFilters,
        _limit: usize,
        _offset: usize,
    ) -> crate::error::StorageResult<Vec<crate::storage::ArtifactListItem>> {
        Ok(vec![])
    }

    fn get_timeline(
        &self,
        _filters: &crate::storage::TimelineFilters,
        _limit: usize,
        _offset: usize,
    ) -> crate::error::StorageResult<Vec<crate::storage::TimelineEntry>> {
        Ok(vec![])
    }

    fn load_artifact_for_enrichment(
        &self,
        _artifact_id: &str,
    ) -> crate::error::StorageResult<Option<crate::storage::LoadedArtifactForEnrichment>> {
        Ok(None)
    }
}

struct MockImportWriteStore;

impl crate::storage::ImportWriteStore for MockImportWriteStore {
    fn write_import(
        &self,
        _import_set: crate::storage::WriteImportSet,
    ) -> crate::error::StorageResult<crate::storage::ImportWriteResult> {
        unreachable!()
    }
}

struct MockObjectStore;

impl crate::object_store::ObjectStore for MockObjectStore {
    fn put_object(
        &self,
        _object: crate::object_store::NewObject,
    ) -> crate::error::ObjectStoreResult<crate::object_store::PutObjectResult> {
        unreachable!()
    }

    fn get_object_bytes(
        &self,
        _object: &crate::object_store::StoredObject,
    ) -> crate::error::ObjectStoreResult<Vec<u8>> {
        unreachable!()
    }

    fn delete_object(
        &self,
        _object: &crate::object_store::StoredObject,
    ) -> crate::error::ObjectStoreResult<()> {
        unreachable!()
    }
}

struct MockWritebackStore;

impl crate::storage::WritebackStore for MockWritebackStore {
    fn store_agent_memory(
        &self,
        _memory: &crate::storage::NewAgentMemory,
    ) -> crate::error::StorageResult<()> {
        Ok(())
    }

    fn store_archive_link(
        &self,
        _link: &crate::storage::NewArchiveLink,
    ) -> crate::error::StorageResult<()> {
        Ok(())
    }

    fn update_object_status(
        &self,
        _update: &crate::storage::UpdateObjectStatus,
    ) -> crate::error::StorageResult<()> {
        Ok(())
    }

    fn store_agent_entity(
        &self,
        _entity: &crate::storage::NewAgentEntity,
    ) -> crate::error::StorageResult<()> {
        Ok(())
    }
}

struct MockReviewStore;

impl crate::storage::ReviewReadStore for MockReviewStore {
    fn list_review_candidates(
        &self,
        filters: &crate::storage::ReviewQueueFilters,
        limit: usize,
    ) -> crate::error::StorageResult<Vec<crate::storage::ReviewCandidate>> {
        let mut items = vec![
            crate::storage::ReviewCandidate {
                kind: crate::storage::ReviewItemKind::ArtifactNeedsAttention,
                artifact_id: "artifact-1".to_string(),
                derived_object_id: None,
                source_type: crate::storage::SourceType::ChatGptExport,
                captured_at: "2026-03-24T10:00:00.000000+00".to_string(),
                title: Some("Artifact failed".to_string()),
                body_text: None,
                derived_object_type: None,
                candidate_key: None,
                enrichment_status: Some(crate::storage::EnrichmentStatus::Failed),
                confidence_score: None,
                related_artifact_count: None,
            },
            crate::storage::ReviewCandidate {
                kind: crate::storage::ReviewItemKind::ObjectLowConfidence,
                artifact_id: "artifact-2".to_string(),
                derived_object_id: Some("obj-2".to_string()),
                source_type: crate::storage::SourceType::ClaudeExport,
                captured_at: "2026-03-23T10:00:00.000000+00".to_string(),
                title: Some("Weak object".to_string()),
                body_text: Some("body".to_string()),
                derived_object_type: Some(crate::storage::DerivedObjectType::Memory),
                candidate_key: None,
                enrichment_status: None,
                confidence_score: Some(0.42),
                related_artifact_count: None,
            },
        ];
        if let Some(kinds) = filters.kinds.as_ref() {
            items.retain(|item| kinds.contains(&item.kind));
        }
        items.truncate(limit);
        Ok(items)
    }
}

impl crate::storage::ReviewWriteStore for MockReviewStore {
    fn record_review_decision(
        &self,
        _decision: &crate::storage::NewReviewDecision,
    ) -> crate::error::StorageResult<()> {
        Ok(())
    }

    fn retry_artifact_enrichment(&self, artifact_id: &str) -> crate::error::StorageResult<String> {
        Ok(format!("job-retry-{artifact_id}"))
    }
}
