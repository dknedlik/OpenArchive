use tiny_http::{Method, Request, StatusCode};

use crate::app::ArchiveApplication;

use super::handlers::{
    handle_get_artifacts, handle_get_review_items, handle_post_imports_chatgpt,
    handle_post_imports_claude, handle_post_imports_gemini, handle_post_imports_grok,
    handle_post_imports_markdown, handle_post_imports_obsidian, handle_post_imports_text,
    handle_post_review_artifact_retry, handle_post_review_decisions,
};
use super::request::split_request_url;
use super::response::{text_response, HttpResponse};

pub fn build_response(request: &mut Request, app: &ArchiveApplication) -> HttpResponse {
    let (path, query) = split_request_url(request.url());
    match (request.method(), path) {
        (&Method::Post, "/imports/chatgpt") => handle_post_imports_chatgpt(request, app),
        (&Method::Post, "/imports/claude") => handle_post_imports_claude(request, app),
        (&Method::Post, "/imports/grok") => handle_post_imports_grok(request, app),
        (&Method::Post, "/imports/gemini") => handle_post_imports_gemini(request, app),
        (&Method::Post, "/imports/text") => handle_post_imports_text(request, app),
        (&Method::Post, "/imports/markdown") => handle_post_imports_markdown(request, app),
        (&Method::Post, "/imports/obsidian") => handle_post_imports_obsidian(request, app),
        (&Method::Get, "/artifacts") => handle_get_artifacts(app),
        (&Method::Get, "/review/items") => handle_get_review_items(query, app),
        (&Method::Post, "/review/decisions") => handle_post_review_decisions(request, app),
        (&Method::Post, "/review/artifacts/retry") => {
            handle_post_review_artifact_retry(request, app)
        }
        _ => text_response(StatusCode(404), "not found"),
    }
}
