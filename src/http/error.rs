use crate::error::OpenArchiveError;

use super::response::{text_response, HttpResponse};

pub(in crate::http) enum HttpError {
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
    pub(in crate::http) fn into_response(self) -> HttpResponse {
        match self {
            Self::EmptyBody => {
                text_response(tiny_http::StatusCode(400), "request body is required")
            }
            Self::ReadBody(err) => text_response(
                tiny_http::StatusCode(400),
                format!("failed to read request body: {err}"),
            ),
            Self::BadRequest(detail) => text_response(tiny_http::StatusCode(400), detail),
            Self::ServiceUnavailable(detail) => text_response(tiny_http::StatusCode(503), detail),
            Self::Internal(detail) => text_response(tiny_http::StatusCode(500), detail),
        }
    }
}

pub(in crate::http) fn review_http_error(err: OpenArchiveError) -> HttpError {
    match err {
        OpenArchiveError::Invariant(detail) => HttpError::BadRequest(detail),
        other => HttpError::from(other),
    }
}
