use tiny_http::StatusCode;

use crate::app::ArchiveApplication;

use super::super::error::HttpError;
use super::super::response::{json_response, HttpResponse};

pub(in crate::http) fn handle_get_artifacts(app: &ArchiveApplication) -> HttpResponse {
    #[derive(serde::Serialize)]
    struct ArtifactListResponse {
        artifacts: Vec<crate::storage::ArtifactListItem>,
    }

    match app.artifacts.list_artifacts().map_err(HttpError::from) {
        Ok(artifacts) => json_response(StatusCode(200), &ArtifactListResponse { artifacts }),
        Err(err) => err.into_response(),
    }
}
