use std::io::Cursor;

use tiny_http::{Header, Method, Request, Response, StatusCode};

use crate::error::OpenArchiveError;
use crate::import_service::import_chatgpt_payload;
use crate::storage::ImportWriteStore;

pub fn build_response<S>(request: &mut Request, store: &S) -> Response<Cursor<Vec<u8>>>
where
    S: ImportWriteStore,
{
    match (request.method(), request.url()) {
        (&Method::Post, "/imports/chatgpt") => handle_post_imports_chatgpt(request, store),
        _ => text_response(StatusCode(404), "not found"),
    }
}

fn handle_post_imports_chatgpt<S>(request: &mut Request, store: &S) -> Response<Cursor<Vec<u8>>>
where
    S: ImportWriteStore,
{
    match read_request_body(request)
        .and_then(|body| import_chatgpt_payload(store, &body).map_err(HttpError::from))
    {
        Ok(result) => json_response(StatusCode(200), &result),
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
    Internal(String),
}

impl From<OpenArchiveError> for HttpError {
    fn from(value: OpenArchiveError) -> Self {
        match value {
            OpenArchiveError::Parser(err) => Self::BadRequest(err.to_string()),
            OpenArchiveError::Config(err) => Self::Internal(err.to_string()),
            OpenArchiveError::Db(err) => Self::Internal(err.to_string()),
            OpenArchiveError::Migrations(err) => Self::Internal(err.to_string()),
            OpenArchiveError::Storage(err) => Self::Internal(err.to_string()),
            OpenArchiveError::Invariant(err) => Self::Internal(err),
        }
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
            Self::Internal(detail) => text_response(StatusCode(500), detail),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{
        ArtifactIngestResult, EnrichmentStatus, ImportStatus, ImportWriteResult, ImportedArtifact,
        WriteImportSet,
    };
    use tiny_http::TestRequest;

    struct MockStore;

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

    #[test]
    fn post_imports_chatgpt_returns_json_payload() {
        let mut request = TestRequest::new()
            .with_method(Method::Post)
            .with_path("/imports/chatgpt")
            .with_body(valid_export())
            .into();

        let response = build_response(&mut request, &MockStore);
        assert_eq!(response.status_code(), StatusCode(200));
    }

    #[test]
    fn post_imports_chatgpt_rejects_malformed_json() {
        let mut request = TestRequest::new()
            .with_method(Method::Post)
            .with_path("/imports/chatgpt")
            .with_body(r#"{"bad":true}"#)
            .into();

        let response = build_response(&mut request, &MockStore);
        assert_eq!(response.status_code(), StatusCode(400));
    }

    #[test]
    fn unknown_route_returns_404() {
        let mut request = TestRequest::new().with_path("/missing").into();
        let response = build_response(&mut request, &MockStore);
        assert_eq!(response.status_code(), StatusCode(404));
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
