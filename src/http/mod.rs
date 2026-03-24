use std::io::Cursor;

use tiny_http::{Header, Method, Request, Response, StatusCode};
use url::form_urlencoded;

use crate::app::{review, ArchiveApplication};
use crate::error::OpenArchiveError;

pub fn build_response(
    request: &mut Request,
    app: &ArchiveApplication,
) -> Response<Cursor<Vec<u8>>> {
    let (path, query) = split_request_url(request.url());
    match (request.method(), path) {
        (&Method::Post, "/imports/chatgpt") => handle_post_imports_chatgpt(request, app),
        (&Method::Post, "/imports/claude") => handle_post_imports_claude(request, app),
        (&Method::Post, "/imports/grok") => handle_post_imports_grok(request, app),
        (&Method::Post, "/imports/gemini") => handle_post_imports_gemini(request, app),
        (&Method::Post, "/imports/text") => handle_post_imports_text(request, app),
        (&Method::Post, "/imports/markdown") => handle_post_imports_markdown(request, app),
        (&Method::Get, "/artifacts") => handle_get_artifacts(app),
        (&Method::Get, "/review/items") => handle_get_review_items(query, app),
        (&Method::Post, "/review/decisions") => handle_post_review_decisions(request, app),
        (&Method::Post, "/review/artifacts/retry") => {
            handle_post_review_artifact_retry(request, app)
        }
        _ => text_response(StatusCode(404), "not found"),
    }
}

fn handle_post_imports_chatgpt(
    request: &mut Request,
    app: &ArchiveApplication,
) -> Response<Cursor<Vec<u8>>> {
    match read_request_body(request).and_then(|body| {
        app.imports
            .import_chatgpt_payload(&body)
            .map_err(HttpError::from)
    }) {
        Ok(result) => json_response(StatusCode(200), &result),
        Err(err) => err.into_response(),
    }
}

fn handle_post_imports_claude(
    request: &mut Request,
    app: &ArchiveApplication,
) -> Response<Cursor<Vec<u8>>> {
    match read_request_body(request).and_then(|body| {
        app.imports
            .import_claude_payload(&body)
            .map_err(HttpError::from)
    }) {
        Ok(result) => json_response(StatusCode(200), &result),
        Err(err) => err.into_response(),
    }
}

fn handle_post_imports_grok(
    request: &mut Request,
    app: &ArchiveApplication,
) -> Response<Cursor<Vec<u8>>> {
    match read_request_body(request).and_then(|body| {
        app.imports
            .import_grok_payload(&body)
            .map_err(HttpError::from)
    }) {
        Ok(result) => json_response(StatusCode(200), &result),
        Err(err) => err.into_response(),
    }
}

fn handle_post_imports_gemini(
    request: &mut Request,
    app: &ArchiveApplication,
) -> Response<Cursor<Vec<u8>>> {
    match read_request_body(request).and_then(|body| {
        app.imports
            .import_gemini_payload(&body)
            .map_err(HttpError::from)
    }) {
        Ok(result) => json_response(StatusCode(200), &result),
        Err(err) => err.into_response(),
    }
}

fn handle_post_imports_text(
    request: &mut Request,
    app: &ArchiveApplication,
) -> Response<Cursor<Vec<u8>>> {
    match read_request_body(request).and_then(|body| {
        app.imports
            .import_text_payload(&body)
            .map_err(HttpError::from)
    }) {
        Ok(result) => json_response(StatusCode(200), &result),
        Err(err) => err.into_response(),
    }
}

fn handle_post_imports_markdown(
    request: &mut Request,
    app: &ArchiveApplication,
) -> Response<Cursor<Vec<u8>>> {
    match read_request_body(request).and_then(|body| {
        app.imports
            .import_markdown_payload(&body)
            .map_err(HttpError::from)
    }) {
        Ok(result) => json_response(StatusCode(200), &result),
        Err(err) => err.into_response(),
    }
}

fn handle_get_artifacts(app: &ArchiveApplication) -> Response<Cursor<Vec<u8>>> {
    #[derive(serde::Serialize)]
    struct ArtifactListResponse {
        artifacts: Vec<crate::storage::ArtifactListItem>,
    }

    match app.artifacts.list_artifacts().map_err(HttpError::from) {
        Ok(artifacts) => json_response(StatusCode(200), &ArtifactListResponse { artifacts }),
        Err(err) => err.into_response(),
    }
}

fn handle_get_review_items(
    query: Option<&str>,
    app: &ArchiveApplication,
) -> Response<Cursor<Vec<u8>>> {
    let service = match app.review.as_ref() {
        Some(service) => service,
        None => {
            return HttpError::ServiceUnavailable(
                "review service is unavailable for the configured provider".to_string(),
            )
            .into_response()
        }
    };

    match parse_review_query_request(query)
        .and_then(|request| service.list(request).map_err(review_http_error))
    {
        Ok(response) => json_response(StatusCode(200), &response),
        Err(err) => err.into_response(),
    }
}

fn handle_post_review_decisions(
    request: &mut Request,
    app: &ArchiveApplication,
) -> Response<Cursor<Vec<u8>>> {
    #[derive(serde::Serialize)]
    struct RecordDecisionResponse {
        recorded: bool,
        review_decision_id: String,
    }

    let service = match app.review.as_ref() {
        Some(service) => service,
        None => {
            return HttpError::ServiceUnavailable(
                "review service is unavailable for the configured provider".to_string(),
            )
            .into_response()
        }
    };

    match read_json_body::<review::ReviewDecisionRequest>(request)
        .and_then(|decision| service.record_decision(decision).map_err(review_http_error))
    {
        Ok(review_decision_id) => json_response(
            StatusCode(200),
            &RecordDecisionResponse {
                recorded: true,
                review_decision_id,
            },
        ),
        Err(err) => err.into_response(),
    }
}

fn handle_post_review_artifact_retry(
    request: &mut Request,
    app: &ArchiveApplication,
) -> Response<Cursor<Vec<u8>>> {
    #[derive(serde::Serialize)]
    struct RetryArtifactResponse {
        queued: bool,
        job_id: String,
    }

    let service = match app.review.as_ref() {
        Some(service) => service,
        None => {
            return HttpError::ServiceUnavailable(
                "review service is unavailable for the configured provider".to_string(),
            )
            .into_response()
        }
    };

    match read_json_body::<review::RetryArtifactRequest>(request)
        .and_then(|retry| service.retry_artifact(retry).map_err(review_http_error))
    {
        Ok(job_id) => json_response(
            StatusCode(200),
            &RetryArtifactResponse {
                queued: true,
                job_id,
            },
        ),
        Err(err) => err.into_response(),
    }
}

fn read_request_body(request: &mut Request) -> Result<Vec<u8>, HttpError> {
    let mut bytes = Vec::with_capacity(request.body_length().unwrap_or(0));
    request
        .as_reader()
        .read_to_end(&mut bytes)
        .map_err(HttpError::ReadBody)?;

    if bytes.is_empty() {
        return Err(HttpError::EmptyBody);
    }

    Ok(bytes)
}

fn read_json_body<T>(request: &mut Request) -> Result<T, HttpError>
where
    T: serde::de::DeserializeOwned,
{
    let bytes = read_request_body(request)?;
    serde_json::from_slice(&bytes)
        .map_err(|err| HttpError::BadRequest(format!("invalid JSON body: {err}")))
}

fn split_request_url(url: &str) -> (&str, Option<&str>) {
    match url.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (url, None),
    }
}

fn parse_review_query_request(
    query: Option<&str>,
) -> Result<review::ReviewQueueRequest, HttpError> {
    let mut limit = 20_usize;
    let mut kinds = Vec::new();

    if let Some(query) = query {
        for (key, value) in form_urlencoded::parse(query.as_bytes()) {
            match key.as_ref() {
                "limit" => {
                    limit = value
                        .parse::<usize>()
                        .map_err(|_| HttpError::BadRequest(format!("invalid limit: {value}")))?;
                }
                "kind" => kinds.push(parse_review_kind(value.as_ref())?),
                "kinds" => {
                    for raw_kind in value
                        .split(',')
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                    {
                        kinds.push(parse_review_kind(raw_kind)?);
                    }
                }
                unknown => {
                    return Err(HttpError::BadRequest(format!(
                        "unknown review query parameter: {unknown}"
                    )))
                }
            }
        }
    }

    Ok(review::ReviewQueueRequest {
        filters: crate::storage::ReviewQueueFilters {
            kinds: if kinds.is_empty() { None } else { Some(kinds) },
        },
        limit,
    })
}

fn parse_review_kind(value: &str) -> Result<crate::storage::ReviewItemKind, HttpError> {
    crate::storage::ReviewItemKind::from_str(value)
        .ok_or_else(|| HttpError::BadRequest(format!("invalid review kind: {value}")))
}

fn review_http_error(err: OpenArchiveError) -> HttpError {
    match err {
        OpenArchiveError::Invariant(detail) => HttpError::BadRequest(detail),
        other => HttpError::from(other),
    }
}

fn json_response<T>(status: StatusCode, value: &T) -> Response<Cursor<Vec<u8>>>
where
    T: serde::Serialize,
{
    let body = serde_json::to_vec(value).unwrap_or_else(|err| {
        format!(r#"{{"error":"failed to serialize response: {err}"}}"#).into_bytes()
    });
    let mut response = Response::from_data(body).with_status_code(status);
    response.add_header(json_content_type_header());
    response
}

fn text_response(status: StatusCode, body: impl Into<String>) -> Response<Cursor<Vec<u8>>> {
    Response::from_string(body).with_status_code(status)
}

fn json_content_type_header() -> Header {
    Header::from_bytes("Content-Type", "application/json").expect("valid JSON content-type header")
}

enum HttpError {
    EmptyBody,
    ReadBody(std::io::Error),
    BadRequest(String),
    ServiceUnavailable(String),
    Internal(String),
}

impl From<OpenArchiveError> for HttpError {
    fn from(value: OpenArchiveError) -> Self {
        match value {
            OpenArchiveError::Parser(err) => Self::BadRequest(err.to_string()),
            OpenArchiveError::Config(err) => Self::Internal(err.to_string()),
            OpenArchiveError::Db(err) => Self::Internal(err.to_string()),
            OpenArchiveError::Migrations(err) => Self::Internal(err.to_string()),
            OpenArchiveError::ObjectStore(err) => Self::Internal(err.to_string()),
            OpenArchiveError::Storage(err) => Self::Internal(err.to_string()),
            OpenArchiveError::Embedding(err) => Self::Internal(err.to_string()),
            OpenArchiveError::Invariant(err) => Self::BadRequest(err),
        }
    }
}

impl From<crate::error::StorageError> for HttpError {
    fn from(value: crate::error::StorageError) -> Self {
        Self::Internal(value.to_string())
    }
}

impl HttpError {
    fn into_response(self) -> Response<Cursor<Vec<u8>>> {
        match self {
            Self::EmptyBody => text_response(StatusCode(400), "request body is required"),
            Self::ReadBody(err) => text_response(
                StatusCode(400),
                format!("failed to read request body: {err}"),
            ),
            Self::BadRequest(detail) => text_response(StatusCode(400), detail),
            Self::ServiceUnavailable(detail) => text_response(StatusCode(503), detail),
            Self::Internal(detail) => text_response(StatusCode(500), detail),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::ArchiveApplication;
    use crate::object_store::{NewObject, ObjectStore, PutObjectResult, StoredObject};
    use crate::storage::{
        ArchiveRetrievalStore, ArtifactIngestResult, ArtifactListItem, ArtifactReadStore,
        EnrichmentStatus, ImportStatus, ImportWriteResult, ImportWriteStore, ImportedArtifact,
        NewReviewDecision, RetrievalIntent, RetrievedContextItem, ReviewCandidate, ReviewItemKind,
        ReviewQueueFilters, ReviewReadStore, ReviewWriteStore, SourceType, WriteImportSet,
    };
    use std::io::Read;
    use std::sync::Arc;
    use tiny_http::TestRequest;

    struct MockStore {
        artifacts: Vec<ArtifactListItem>,
        review_candidates: Vec<ReviewCandidate>,
    }

    impl ImportWriteStore for MockStore {
        fn write_import(
            &self,
            import_set: WriteImportSet,
        ) -> crate::error::StorageResult<ImportWriteResult> {
            Ok(ImportWriteResult {
                import_id: "import-http".to_string(),
                import_status: ImportStatus::Completed,
                artifacts: import_set
                    .artifact_sets
                    .iter()
                    .map(|artifact| ImportedArtifact {
                        artifact_id: artifact.artifact.artifact_id.clone(),
                        enrichment_status: EnrichmentStatus::Pending,
                        ingest_result: ArtifactIngestResult::Created,
                    })
                    .collect(),
                failed_artifact_ids: Vec::new(),
                segments_written: 0,
            })
        }
    }

    impl ArtifactReadStore for MockStore {
        fn list_artifacts(&self) -> crate::error::StorageResult<Vec<ArtifactListItem>> {
            Ok(self.artifacts.clone())
        }

        fn list_artifacts_filtered(
            &self,
            _filters: &crate::storage::ArtifactListFilters,
            _limit: usize,
            _offset: usize,
        ) -> crate::error::StorageResult<Vec<ArtifactListItem>> {
            Ok(self.artifacts.clone())
        }

        fn get_timeline(
            &self,
            _filters: &crate::storage::TimelineFilters,
            _limit: usize,
            _offset: usize,
        ) -> crate::error::StorageResult<Vec<crate::storage::TimelineEntry>> {
            Ok(Vec::new())
        }

        fn load_artifact_for_enrichment(
            &self,
            _artifact_id: &str,
        ) -> crate::error::StorageResult<Option<crate::storage::LoadedArtifactForEnrichment>>
        {
            Ok(None)
        }
    }

    impl ArchiveRetrievalStore for MockStore {
        fn retrieve_for_intents(
            &self,
            _artifact_id: &str,
            _intents: &[RetrievalIntent],
            _limit_per_intent: usize,
        ) -> crate::error::StorageResult<Vec<RetrievedContextItem>> {
            Ok(Vec::new())
        }
    }

    impl ObjectStore for MockStore {
        fn put_object(
            &self,
            object: NewObject,
        ) -> crate::error::ObjectStoreResult<PutObjectResult> {
            Ok(PutObjectResult {
                stored_object: StoredObject {
                    object_id: object.object_id,
                    provider: "mock".to_string(),
                    storage_key: "mock-key".to_string(),
                    mime_type: object.mime_type,
                    size_bytes: object.bytes.len() as i64,
                    sha256: object.sha256,
                },
                was_created: true,
            })
        }

        fn get_object_bytes(
            &self,
            object: &StoredObject,
        ) -> crate::error::ObjectStoreResult<Vec<u8>> {
            Ok(object.storage_key.as_bytes().to_vec())
        }

        fn delete_object(&self, _object: &StoredObject) -> crate::error::ObjectStoreResult<()> {
            Ok(())
        }
    }

    impl ReviewReadStore for MockStore {
        fn list_review_candidates(
            &self,
            filters: &ReviewQueueFilters,
            limit: usize,
        ) -> crate::error::StorageResult<Vec<ReviewCandidate>> {
            let mut items = self.review_candidates.clone();
            if let Some(kinds) = filters.kinds.as_ref() {
                items.retain(|item| kinds.contains(&item.kind));
            }
            items.truncate(limit);
            Ok(items)
        }
    }

    impl ReviewWriteStore for MockStore {
        fn record_review_decision(
            &self,
            _decision: &NewReviewDecision,
        ) -> crate::error::StorageResult<()> {
            Ok(())
        }

        fn retry_artifact_enrichment(
            &self,
            artifact_id: &str,
        ) -> crate::error::StorageResult<String> {
            Ok(format!("job-retry-{artifact_id}"))
        }
    }

    #[test]
    fn post_imports_chatgpt_returns_json_payload() {
        let store = MockStore {
            artifacts: Vec::new(),
            review_candidates: Vec::new(),
        };
        let mut request = TestRequest::new()
            .with_method(Method::Post)
            .with_path("/imports/chatgpt")
            .with_body(valid_export())
            .into();

        let app = test_app(store);
        let response = build_response(&mut request, app.as_ref());
        assert_eq!(response.status_code(), StatusCode(200));
    }

    #[test]
    fn post_imports_chatgpt_rejects_malformed_json() {
        let store = MockStore {
            artifacts: Vec::new(),
            review_candidates: Vec::new(),
        };
        let mut request = TestRequest::new()
            .with_method(Method::Post)
            .with_path("/imports/chatgpt")
            .with_body(r#"{"bad":true}"#)
            .into();

        let app = test_app(store);
        let response = build_response(&mut request, app.as_ref());
        assert_eq!(response.status_code(), StatusCode(400));
    }

    #[test]
    fn post_imports_claude_returns_json_payload() {
        let store = MockStore {
            artifacts: Vec::new(),
            review_candidates: Vec::new(),
        };
        let mut request = TestRequest::new()
            .with_method(Method::Post)
            .with_path("/imports/claude")
            .with_body(include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/claude_export.json"
            )))
            .into();

        let app = test_app(store);
        let response = build_response(&mut request, app.as_ref());
        assert_eq!(response.status_code(), StatusCode(200));
    }

    #[test]
    fn post_imports_grok_returns_json_payload() {
        let store = MockStore {
            artifacts: Vec::new(),
            review_candidates: Vec::new(),
        };
        let mut request = TestRequest::new()
            .with_method(Method::Post)
            .with_path("/imports/grok")
            .with_body(include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/grok_export.json"
            )))
            .into();

        let app = test_app(store);
        let response = build_response(&mut request, app.as_ref());
        assert_eq!(response.status_code(), StatusCode(200));
    }

    #[test]
    fn post_imports_gemini_returns_json_payload() {
        let store = MockStore {
            artifacts: Vec::new(),
            review_candidates: Vec::new(),
        };
        let mut request = TestRequest::new()
            .with_method(Method::Post)
            .with_path("/imports/gemini")
            .with_body(include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/gemini_export.json"
            )))
            .into();

        let app = test_app(store);
        let response = build_response(&mut request, app.as_ref());
        assert_eq!(response.status_code(), StatusCode(200));
    }

    #[test]
    fn post_imports_text_returns_json_payload() {
        let store = MockStore {
            artifacts: Vec::new(),
            review_candidates: Vec::new(),
        };
        let mut request = TestRequest::new()
            .with_method(Method::Post)
            .with_path("/imports/text")
            .with_body("First paragraph.\n\nSecond paragraph.")
            .into();

        let app = test_app(store);
        let response = build_response(&mut request, app.as_ref());
        assert_eq!(response.status_code(), StatusCode(200));
    }

    #[test]
    fn post_imports_markdown_returns_json_payload() {
        let store = MockStore {
            artifacts: Vec::new(),
            review_candidates: Vec::new(),
        };
        let mut request = TestRequest::new()
            .with_method(Method::Post)
            .with_path("/imports/markdown")
            .with_body("# Title\n\nParagraph text.\n\n- one\n- two")
            .into();

        let app = test_app(store);
        let response = build_response(&mut request, app.as_ref());
        assert_eq!(response.status_code(), StatusCode(200));
    }

    #[test]
    fn unknown_route_returns_404() {
        let store = MockStore {
            artifacts: Vec::new(),
            review_candidates: Vec::new(),
        };
        let mut request = TestRequest::new().with_path("/missing").into();
        let app = test_app(store);
        let response = build_response(&mut request, app.as_ref());
        assert_eq!(response.status_code(), StatusCode(404));
    }

    #[test]
    fn get_artifacts_returns_empty_envelope() {
        let store = MockStore {
            artifacts: Vec::new(),
            review_candidates: Vec::new(),
        };
        let mut request = TestRequest::new()
            .with_method(Method::Get)
            .with_path("/artifacts")
            .into();

        let app = test_app(store);
        let response = build_response(&mut request, app.as_ref());
        assert_eq!(response.status_code(), StatusCode(200));
        assert_eq!(response_body_string(response), r#"{"artifacts":[]}"#);
    }

    #[test]
    fn get_artifacts_returns_machine_first_fields_in_order() {
        let store = MockStore {
            artifacts: vec![
                ArtifactListItem {
                    artifact_id: "artifact-b".to_string(),
                    title: Some("Newest".to_string()),
                    source_type: "chatgpt_export".to_string(),
                    created_at_source: None,
                    captured_at: "2026-03-10T14:00:00.000000000+00:00".to_string(),
                    enrichment_status: EnrichmentStatus::Running,
                },
                ArtifactListItem {
                    artifact_id: "artifact-a".to_string(),
                    title: Some("Older".to_string()),
                    source_type: "chatgpt_export".to_string(),
                    created_at_source: Some("2026-03-09T12:30:00.000000000+00:00".to_string()),
                    captured_at: "2026-03-09T13:00:00.000000000+00:00".to_string(),
                    enrichment_status: EnrichmentStatus::Pending,
                },
            ],
            review_candidates: Vec::new(),
        };
        let mut request = TestRequest::new()
            .with_method(Method::Get)
            .with_path("/artifacts")
            .into();

        let app = test_app(store);
        let response = build_response(&mut request, app.as_ref());
        assert_eq!(response.status_code(), StatusCode(200));
        assert_eq!(
            response_body_string(response),
            concat!(
                "{\"artifacts\":[",
                "{\"artifact_id\":\"artifact-b\",\"title\":\"Newest\",\"source_type\":\"chatgpt_export\",",
                "\"created_at_source\":null,\"captured_at\":\"2026-03-10T14:00:00.000000000+00:00\",",
                "\"enrichment_status\":\"running\"},",
                "{\"artifact_id\":\"artifact-a\",\"title\":\"Older\",\"source_type\":\"chatgpt_export\",",
                "\"created_at_source\":\"2026-03-09T12:30:00.000000000+00:00\",",
                "\"captured_at\":\"2026-03-09T13:00:00.000000000+00:00\",",
                "\"enrichment_status\":\"pending\"}",
                "]}"
            )
        );
    }

    fn test_app(store: MockStore) -> Arc<ArchiveApplication> {
        let store = Arc::new(store);
        let import_store: Arc<dyn ImportWriteStore + Send + Sync> = store.clone();
        let read_store: Arc<dyn ArtifactReadStore + Send + Sync> = store.clone();
        let retrieval_store: Arc<dyn crate::storage::ArchiveRetrievalStore + Send + Sync> =
            store.clone();
        let review_store: Arc<dyn crate::storage::ReviewStore + Send + Sync> = store.clone();
        let object_store: Arc<dyn ObjectStore + Send + Sync> = store;
        Arc::new(ArchiveApplication::new(
            import_store,
            read_store,
            retrieval_store,
            None,
            None,
            None,
            None,
            None,
            Some(review_store),
            None,
            object_store,
            None,
        ))
    }

    #[test]
    fn get_review_items_returns_filtered_review_queue() {
        let store = MockStore {
            artifacts: Vec::new(),
            review_candidates: vec![
                ReviewCandidate {
                    kind: ReviewItemKind::ArtifactNeedsAttention,
                    artifact_id: "artifact-1".to_string(),
                    derived_object_id: None,
                    source_type: SourceType::ChatGptExport,
                    captured_at: "2026-03-23T10:00:00.000000+00".to_string(),
                    title: Some("Failed artifact".to_string()),
                    body_text: None,
                    derived_object_type: None,
                    candidate_key: None,
                    enrichment_status: Some(EnrichmentStatus::Failed),
                    confidence_score: None,
                    related_artifact_count: None,
                },
                ReviewCandidate {
                    kind: ReviewItemKind::ObjectLowConfidence,
                    artifact_id: "artifact-2".to_string(),
                    derived_object_id: Some("obj-2".to_string()),
                    source_type: SourceType::ClaudeExport,
                    captured_at: "2026-03-22T10:00:00.000000+00".to_string(),
                    title: Some("Weak object".to_string()),
                    body_text: Some("body".to_string()),
                    derived_object_type: Some(crate::storage::DerivedObjectType::Memory),
                    candidate_key: None,
                    enrichment_status: None,
                    confidence_score: Some(0.42),
                    related_artifact_count: None,
                },
            ],
        };
        let mut request = TestRequest::new()
            .with_method(Method::Get)
            .with_path("/review/items?kind=artifact_needs_attention&limit=5")
            .into();

        let app = test_app(store);
        let response = build_response(&mut request, app.as_ref());
        assert_eq!(response.status_code(), StatusCode(200));

        let body: serde_json::Value =
            serde_json::from_str(&response_body_string(response)).expect("review items json");
        assert_eq!(body["items"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            body["items"][0]["item_id"],
            serde_json::Value::String("review:artifact_needs_attention:artifact-1".to_string())
        );
    }

    #[test]
    fn post_review_decisions_rejects_invalid_noted_request() {
        let store = MockStore {
            artifacts: Vec::new(),
            review_candidates: Vec::new(),
        };
        let mut request = TestRequest::new()
            .with_method(Method::Post)
            .with_path("/review/decisions")
            .with_body(
                r#"{"kind":"artifact_needs_attention","artifact_id":"artifact-1","decision_status":"noted"}"#,
            )
            .into();

        let app = test_app(store);
        let response = build_response(&mut request, app.as_ref());
        assert_eq!(response.status_code(), StatusCode(400));
        assert_eq!(
            response_body_string(response),
            "review notes require non-empty note_text"
        );
    }

    #[test]
    fn post_review_artifact_retry_returns_job_id() {
        let store = MockStore {
            artifacts: Vec::new(),
            review_candidates: Vec::new(),
        };
        let mut request = TestRequest::new()
            .with_method(Method::Post)
            .with_path("/review/artifacts/retry")
            .with_body(r#"{"artifact_id":"artifact-77"}"#)
            .into();

        let app = test_app(store);
        let response = build_response(&mut request, app.as_ref());
        assert_eq!(response.status_code(), StatusCode(200));
        assert_eq!(
            response_body_string(response),
            r#"{"queued":true,"job_id":"job-retry-artifact-77"}"#
        );
    }

    fn response_body_string(response: Response<Cursor<Vec<u8>>>) -> String {
        let mut body = String::new();
        let mut reader = response.into_reader();
        reader.read_to_string(&mut body).unwrap();
        body
    }

    fn valid_export() -> &'static str {
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
}
