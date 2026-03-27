use tiny_http::{Request, StatusCode};

use crate::app::{review, ArchiveApplication};

use super::super::error::{review_http_error, HttpError};
use super::super::request::{parse_review_query_request, read_json_body};
use super::super::response::{json_response, HttpResponse};

pub(in crate::http) fn handle_get_review_items(
    query: Option<&str>,
    app: &ArchiveApplication,
) -> HttpResponse {
    let service = match review_service(app) {
        Ok(service) => service,
        Err(err) => return err.into_response(),
    };

    match parse_review_query_request(query)
        .and_then(|request| service.list(request).map_err(review_http_error))
    {
        Ok(response) => json_response(StatusCode(200), &response),
        Err(err) => err.into_response(),
    }
}

pub(in crate::http) fn handle_post_review_decisions(
    request: &mut Request,
    app: &ArchiveApplication,
) -> HttpResponse {
    #[derive(serde::Serialize)]
    struct RecordDecisionResponse {
        recorded: bool,
        review_decision_id: String,
    }

    let service = match review_service(app) {
        Ok(service) => service,
        Err(err) => return err.into_response(),
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

pub(in crate::http) fn handle_post_review_artifact_retry(
    request: &mut Request,
    app: &ArchiveApplication,
) -> HttpResponse {
    #[derive(serde::Serialize)]
    struct RetryArtifactResponse {
        queued: bool,
        job_id: String,
    }

    let service = match review_service(app) {
        Ok(service) => service,
        Err(err) => return err.into_response(),
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

fn review_service(
    app: &ArchiveApplication,
) -> Result<&crate::app::review::ReviewService, HttpError> {
    app.review.as_ref().ok_or_else(|| {
        HttpError::ServiceUnavailable(
            "review service is unavailable for the configured provider".to_string(),
        )
    })
}
