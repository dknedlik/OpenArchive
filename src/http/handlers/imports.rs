use tiny_http::{Request, StatusCode};

use crate::app::ArchiveApplication;

use super::super::error::HttpError;
use super::super::request::read_request_body;
use super::super::response::{json_response, HttpResponse};

pub(in crate::http) fn handle_post_imports_chatgpt(
    request: &mut Request,
    app: &ArchiveApplication,
) -> HttpResponse {
    handle_import_request(request, |body| app.imports.import_chatgpt_payload(body))
}

pub(in crate::http) fn handle_post_imports_claude(
    request: &mut Request,
    app: &ArchiveApplication,
) -> HttpResponse {
    handle_import_request(request, |body| app.imports.import_claude_payload(body))
}

pub(in crate::http) fn handle_post_imports_grok(
    request: &mut Request,
    app: &ArchiveApplication,
) -> HttpResponse {
    handle_import_request(request, |body| app.imports.import_grok_payload(body))
}

pub(in crate::http) fn handle_post_imports_gemini(
    request: &mut Request,
    app: &ArchiveApplication,
) -> HttpResponse {
    handle_import_request(request, |body| app.imports.import_gemini_payload(body))
}

pub(in crate::http) fn handle_post_imports_text(
    request: &mut Request,
    app: &ArchiveApplication,
) -> HttpResponse {
    handle_import_request(request, |body| app.imports.import_text_payload(body))
}

pub(in crate::http) fn handle_post_imports_markdown(
    request: &mut Request,
    app: &ArchiveApplication,
) -> HttpResponse {
    handle_import_request(request, |body| app.imports.import_markdown_payload(body))
}

pub(in crate::http) fn handle_post_imports_obsidian(
    request: &mut Request,
    app: &ArchiveApplication,
) -> HttpResponse {
    handle_import_request(request, |body| {
        app.imports.import_obsidian_vault_payload(body)
    })
}

fn handle_import_request<T, F>(request: &mut Request, import: F) -> HttpResponse
where
    T: serde::Serialize,
    F: FnOnce(&[u8]) -> Result<T, crate::error::OpenArchiveError>,
{
    match read_request_body(request).and_then(|body| import(&body).map_err(HttpError::from)) {
        Ok(result) => json_response(StatusCode(200), &result),
        Err(err) => err.into_response(),
    }
}
