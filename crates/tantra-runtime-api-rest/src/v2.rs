//! REST API v2 routes and handlers (Latest & Recommended).

use std::sync::Arc;

use axum::Router;
use axum::http::HeaderValue;
use axum::response::{IntoResponse, Response};

use crate::{AppState, v1};

/// Build the v2 router with attached state.
pub fn routes(state: Arc<AppState>) -> Router {
    router().with_state(state)
}

/// Build the v2 router expecting State<Arc<AppState>>.
pub fn router() -> Router<Arc<AppState>> {
    v1::router()
        .merge(crate::library_routes::v2_only_routes())
        .merge(crate::chat_threads::v2_only_routes())
        .layer(axum::middleware::from_fn(v2_response_envelope))
}

/// Largest JSON body the envelope middleware will buffer to rewrite `api_version`.
const MAX_ENVELOPE_BYTES: usize = 32 * 1024 * 1024;

fn is_json(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.to_ascii_lowercase().starts_with("application/json"))
}

/// Middleware that updates the response envelope and headers for API v2.
///
/// Only `application/json` responses within the buffering cap are rewritten; anything else
/// (streams, oversized bodies) passes through untouched.
pub async fn v2_response_envelope(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let res = next.run(req).await;
    let (mut parts, body) = res.into_parts();
    parts.headers.insert(
        axum::http::HeaderName::from_static("x-tantra-version"),
        HeaderValue::from_static("2"),
    );

    let declared_len = parts
        .headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok());
    if !is_json(&parts.headers) || declared_len.is_some_and(|n| n > MAX_ENVELOPE_BYTES) {
        return Response::from_parts(parts, body);
    }

    let bytes = match axum::body::to_bytes(body, MAX_ENVELOPE_BYTES).await {
        Ok(b) => b,
        Err(_) => {
            let detail = tantra_runtime_api::ErrorDetail::new(
                tantra_runtime_api::ErrorCode::InternalError,
                "response body could not be buffered for the v2 envelope",
            );
            let mut body: tantra_runtime_api::ResponseEnvelope<serde_json::Value> =
                tantra_runtime_api::ApiResponse::error(detail);
            body.api_version = tantra_runtime_api::ApiVersion::from("v2");
            return (
                axum::http::StatusCode::BAD_GATEWAY,
                axum::response::Json(body),
            )
                .into_response();
        }
    };

    if let Ok(mut val) = serde_json::from_slice::<serde_json::Value>(&bytes)
        && val.is_object()
        && val.get("api_version").is_some()
    {
        val["api_version"] = serde_json::json!("v2");
        if let Ok(new_body) = serde_json::to_vec(&val) {
            parts.headers.insert(
                axum::http::header::CONTENT_LENGTH,
                HeaderValue::from(new_body.len()),
            );
            return Response::from_parts(parts, axum::body::Body::from(new_body));
        }
    }
    Response::from_parts(parts, axum::body::Body::from(bytes))
}
