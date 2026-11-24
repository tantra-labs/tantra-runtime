//! REST API v2: the only API version. Merges the canonical route table with the v2-only
//! sub-tables and applies the response envelope.

use std::sync::Arc;

use axum::Router;
use axum::http::HeaderValue;
use axum::response::{IntoResponse, Response};

use crate::{AppState, routes};

/// Build the v2 router with attached state.
pub fn routes(state: Arc<AppState>) -> Router {
    router().with_state(state)
}

/// Build the v2 router expecting State<Arc<AppState>>.
pub fn router() -> Router<Arc<AppState>> {
    routes::router()
        .merge(crate::library_routes::v2_only_routes())
        .merge(crate::chat_threads::v2_only_routes())
        .merge(crate::v2_canonical::v2_only_routes())
        .merge(crate::alerts_routes::v2_only_routes())
        .merge(crate::stream::v2_only_routes())
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

    let status = parts.status;
    if status.is_redirection()
        || status == axum::http::StatusCode::SWITCHING_PROTOCOLS
        || status == axum::http::StatusCode::NO_CONTENT
        || status == axum::http::StatusCode::NOT_MODIFIED
    {
        return Response::from_parts(parts, body);
    }
    let ok = status.is_success();
    let json = is_json(&parts.headers);
    if ok && !json {
        return Response::from_parts(parts, body);
    }

    let declared_len = parts
        .headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok());
    if declared_len.is_some_and(|n| n > MAX_ENVELOPE_BYTES) {
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

    let parsed = if json {
        serde_json::from_slice::<serde_json::Value>(&bytes).ok()
    } else {
        None
    };

    let rewritten: Option<serde_json::Value> = match parsed {
        Some(mut val) if val.is_object() && val.get("api_version").is_some() => {
            val["api_version"] = serde_json::json!("v2");
            if status == axum::http::StatusCode::PAYLOAD_TOO_LARGE
                && val.get("error").is_some_and(|e| e.is_object())
            {
                if !val["error"]["context"].is_object() {
                    val["error"]["context"] = serde_json::json!({});
                }
                if val["error"]["context"].get("reason").is_none() {
                    val["error"]["context"]["reason"] = serde_json::json!("payload_too_large");
                }
            }
            Some(val)
        }
        Some(val) if ok => {
            tracing::debug!("v2 envelope: wrapping bare JSON success body");
            let mut env: tantra_runtime_api::ResponseEnvelope<serde_json::Value> =
                tantra_runtime_api::ApiResponse::success(val);
            env.api_version = tantra_runtime_api::ApiVersion::from("v2");
            serde_json::to_value(env).ok()
        }
        _ if ok => None,
        _ => {
            let code = tantra_runtime_api::ErrorCode::from_status(status.as_u16());
            let text = String::from_utf8_lossy(&bytes);
            let trimmed: String = text.trim().chars().take(512).collect();
            let message = if trimmed.is_empty() {
                status.canonical_reason().unwrap_or("error").to_string()
            } else {
                trimmed
            };
            let mut ctx = serde_json::json!({"wrapped": true});
            if status == axum::http::StatusCode::PAYLOAD_TOO_LARGE {
                ctx["reason"] = serde_json::json!("payload_too_large");
            }
            tracing::debug!(
                status = status.as_u16(),
                "v2 envelope: wrapping non-envelope error"
            );
            let detail = tantra_runtime_api::ErrorDetail::new(code, message).with_context(ctx);
            let mut env: tantra_runtime_api::ResponseEnvelope<serde_json::Value> =
                tantra_runtime_api::ApiResponse::error(detail);
            env.api_version = tantra_runtime_api::ApiVersion::from("v2");
            serde_json::to_value(env).ok()
        }
    };

    if let Some(val) = rewritten
        && let Ok(new_body) = serde_json::to_vec(&val)
    {
        parts.headers.insert(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        parts.headers.insert(
            axum::http::header::CONTENT_LENGTH,
            HeaderValue::from(new_body.len()),
        );
        return Response::from_parts(parts, axum::body::Body::from(new_body));
    }
    Response::from_parts(parts, axum::body::Body::from(bytes))
}
