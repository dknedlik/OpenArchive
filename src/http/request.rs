use tiny_http::Request;
use url::form_urlencoded;

use crate::app::review;

use super::error::HttpError;

pub(in crate::http) fn read_request_body(request: &mut Request) -> Result<Vec<u8>, HttpError> {
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

pub(in crate::http) fn read_json_body<T>(request: &mut Request) -> Result<T, HttpError>
where
    T: serde::de::DeserializeOwned,
{
    let bytes = read_request_body(request)?;
    serde_json::from_slice(&bytes)
        .map_err(|err| HttpError::BadRequest(format!("invalid JSON body: {err}")))
}

pub(in crate::http) fn split_request_url(url: &str) -> (&str, Option<&str>) {
    match url.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (url, None),
    }
}

pub(in crate::http) fn parse_review_query_request(
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
                    )));
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
