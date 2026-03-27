use std::io::Cursor;

use tiny_http::{Header, Response, StatusCode};

pub(in crate::http) type HttpResponse = Response<Cursor<Vec<u8>>>;

pub(in crate::http) fn json_response<T>(status: StatusCode, value: &T) -> HttpResponse
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

pub(in crate::http) fn text_response(status: StatusCode, body: impl Into<String>) -> HttpResponse {
    Response::from_string(body).with_status_code(status)
}

fn json_content_type_header() -> Header {
    Header::from_bytes("Content-Type", "application/json").expect("valid JSON content-type header")
}
