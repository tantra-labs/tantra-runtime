//! v2-only study routes backed by the Python library MCP server
//! (`docs/plans/0010-library-rest-and-signal-study.md`), plus the canonical
//! `/studies/experimental-analysis/*` aliases.
//!
//! Nothing here fabricates data: when the library client is absent or failing the route answers
//! with a structured error (503/429/504/502/4xx), never a stand-in payload.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tantra_runtime_api::{ErrorCode, ErrorDetail};

use crate::caller::Caller;
use crate::library_client::LibraryError;
use crate::market::{ApiQuery, error, failure, success};
use crate::route_table::{RouteEntry, RouteTable};
use crate::{ApiJson, AppState, RouteScope, experimental_analysis};

/// Maximum request body for the library routes.
const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;
/// Maximum inline bars rows.
pub const MAX_INLINE_BARS: usize = 5000;
/// Maximum `tail` rows.
pub const MAX_TAIL: u32 = 2000;
/// Maximum horizons per signal study.
pub const MAX_HORIZONS: usize = 10;

/// Query for `GET /studies/library/entries`.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LibrarySearchQuery {
    /// Free-text query.
    pub query: Option<String>,
    /// Entry kind filter.
    pub kind: Option<String>,
    /// Family filter.
    pub family: Option<String>,
    /// Maximum results.
    pub limit: Option<u32>,
}

/// Query for `GET /studies/library/entries/{id}`.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LibraryDescribeQuery {
    /// `markdown` (default) or `json`.
    pub format: Option<String>,
}

/// Body of `POST /studies/indicators/compute`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IndicatorComputeRequest {
    /// Library entry id.
    pub id: String,
    /// Indicator parameters.
    #[serde(default)]
    pub params: Map<String, Value>,
    /// Instrument label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instrument: Option<String>,
    /// Bar interval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval: Option<String>,
    /// Inline bar rows (at most 5000). Exactly one of `bars` / `bars_file`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bars: Option<Vec<Value>>,
    /// Lakehouse ref or relative CSV path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bars_file: Option<String>,
    /// Trailing rows to return (at most 2000).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail: Option<u32>,
    /// Also return an overlay scene.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overlay_scene: Option<bool>,
}

/// Body of `POST /studies/signal-study`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SignalStudyRequest {
    /// Library entry id.
    pub id: String,
    /// Indicator parameters.
    #[serde(default)]
    pub params: Map<String, Value>,
    /// Instrument label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instrument: Option<String>,
    /// Bar interval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval: Option<String>,
    /// Inline bar rows (at most 5000). Exactly one of `bars` / `bars_file`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bars: Option<Vec<Value>>,
    /// Lakehouse ref or relative CSV path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bars_file: Option<String>,
    /// Column carrying the event/signal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_column: Option<String>,
    /// Forward horizons in bars (at most 10).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub horizons: Option<Vec<u32>>,
    /// Trade direction, `1` or `-1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direction: Option<i8>,
    /// Optional event filter expression/object, forwarded as-is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Value>,
    /// Also return an overlay scene.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overlay_scene: Option<bool>,
}

/// Routes mounted only under `/api/v2`.
pub(crate) fn v2_only_routes() -> Router<Arc<AppState>> {
    v2_only_table().finish().0
}

/// Every route [`v2_only_routes`] serves, with its scope and body cap (`:param` spelling).
pub(crate) fn v2_only_manifest() -> Vec<RouteEntry> {
    v2_only_table().finish().1
}

fn v2_only_table() -> RouteTable {
    const READ: RouteScope = RouteScope::Required("read");
    RouteTable::new()
        .get("/studies/library/entries", get_entries, READ)
        .get("/studies/library/entries/:id", get_entry, READ)
        .post_capped(
            "/studies/indicators/compute",
            post_compute,
            READ,
            MAX_BODY_BYTES,
        )
        .post_capped(
            "/studies/signal-study",
            post_signal_study,
            READ,
            MAX_BODY_BYTES,
        )
        .get(
            "/studies/experimental-analysis/catalog",
            experimental_analysis::get_catalog,
            READ,
        )
        .post_capped(
            "/studies/experimental-analysis/returns",
            experimental_analysis::post_returns,
            READ,
            MAX_BODY_BYTES,
        )
        .post_capped(
            "/studies/experimental-analysis/intraday",
            experimental_analysis::post_intraday,
            READ,
            MAX_BODY_BYTES,
        )
        .post_capped(
            "/studies/experimental-analysis/correlation",
            experimental_analysis::post_correlation,
            READ,
            MAX_BODY_BYTES,
        )
        .post_capped(
            "/studies/experimental-analysis/options-overlay",
            experimental_analysis::post_options_overlay,
            READ,
            MAX_BODY_BYTES,
        )
        .post_capped(
            "/studies/experimental-analysis/forecast",
            experimental_analysis::post_forecast,
            READ,
            MAX_BODY_BYTES,
        )
        .post_capped(
            "/studies/experimental-analysis/events",
            experimental_analysis::post_events,
            READ,
            MAX_BODY_BYTES,
        )
        .post_capped(
            "/studies/experimental-analysis/outlier-news",
            experimental_analysis::post_outlier_news,
            READ,
            MAX_BODY_BYTES,
        )
}

fn bad_request(msg: impl Into<String>) -> Response {
    error(ErrorDetail::new(ErrorCode::ValidationInvalidRequest, msg))
}

fn invalid(msg: impl Into<String>) -> Response {
    failure(
        StatusCode::UNPROCESSABLE_ENTITY,
        ErrorDetail::new(ErrorCode::ValidationInvalidRequest, msg),
    )
}

fn unavailable() -> Response {
    failure(
        StatusCode::SERVICE_UNAVAILABLE,
        ErrorDetail::new(ErrorCode::ServiceUnavailable, "library_unavailable"),
    )
}

/// Maps a library client failure onto the HTTP contract.
pub(crate) fn map_library_error(err: LibraryError) -> Response {
    let (status, code, msg) = match err {
        LibraryError::Unavailable(m) => (
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::ServiceUnavailable,
            format!("library_unavailable: {m}"),
        ),
        LibraryError::Busy => (
            StatusCode::TOO_MANY_REQUESTS,
            ErrorCode::RateLimited,
            "library_busy".to_owned(),
        ),
        LibraryError::Timeout => (
            StatusCode::GATEWAY_TIMEOUT,
            ErrorCode::Timeout,
            "library_timeout".to_owned(),
        ),
        LibraryError::PayloadTooLarge => (
            StatusCode::BAD_GATEWAY,
            ErrorCode::BadGateway,
            "library_payload_too_large".to_owned(),
        ),
        LibraryError::Protocol(m) => (
            StatusCode::BAD_GATEWAY,
            ErrorCode::BadGateway,
            format!("library_protocol_error: {m}"),
        ),
        LibraryError::Tool { code, message } => {
            let (status, ec) = match code.as_str() {
                "unknown_entry" => (StatusCode::NOT_FOUND, ErrorCode::NotFound),
                "limit_exceeded" => (
                    StatusCode::PAYLOAD_TOO_LARGE,
                    ErrorCode::ValidationInvalidRequest,
                ),
                "invalid_param" | "unknown_param" | "not_computable" | "input_quality"
                | "dependency_error" => (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    ErrorCode::ValidationInvalidRequest,
                ),
                _ => (StatusCode::BAD_GATEWAY, ErrorCode::BadGateway),
            };
            (status, ec, format!("{code}: {message}"))
        }
    };
    let mut detail = ErrorDetail::new(code, msg);
    detail.retryable = code.retryable() || status == StatusCode::TOO_MANY_REQUESTS;
    failure(status, detail)
}

async fn call(state: &AppState, tool: &str, args: Value) -> Result<Value, Response> {
    let client = state.library().ok_or_else(unavailable)?;
    client
        .call_tool(tool, args)
        .await
        .map_err(map_library_error)
}

/// `GET /studies/library/entries`.
pub(crate) async fn get_entries(
    _caller: Caller,
    State(state): State<Arc<AppState>>,
    ApiQuery(q): ApiQuery<LibrarySearchQuery>,
) -> Response {
    let mut args = Map::new();
    if let Some(v) = q.query {
        args.insert("query".into(), json!(v));
    }
    if let Some(v) = q.kind {
        args.insert("kind".into(), json!(v));
    }
    if let Some(v) = q.family {
        args.insert("family".into(), json!(v));
    }
    if let Some(v) = q.limit {
        args.insert("limit".into(), json!(v));
    }
    match call(&state, "tantra_library_search", Value::Object(args)).await {
        Ok(v) => success(v),
        Err(r) => r,
    }
}

/// `GET /studies/library/entries/{id}`.
pub(crate) async fn get_entry(
    _caller: Caller,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    ApiQuery(q): ApiQuery<LibraryDescribeQuery>,
) -> Response {
    if id.is_empty() || id.len() > 256 || id.contains('\0') {
        return bad_request("invalid entry id");
    }
    let format = q.format.unwrap_or_else(|| "markdown".to_owned());
    if format != "markdown" && format != "json" {
        return invalid("format must be 'markdown' or 'json'");
    }
    match call(
        &state,
        "tantra_library_describe",
        json!({"id": id, "format": format}),
    )
    .await
    {
        Ok(Value::String(md)) => success(json!({"id": id, "markdown": md})),
        Ok(v) => success(v),
        Err(r) => r,
    }
}

/// Validates the shared bars / tail fields; `Err` is the response to return.
fn validate_bars(
    bars: &Option<Vec<Value>>,
    bars_file: &Option<String>,
    tail: Option<u32>,
) -> Option<Response> {
    if let Some(f) = bars_file
        && (f.is_empty()
            || f.contains(['\0', ':', '\\'])
            || f.starts_with('/')
            || f.split('/').any(|seg| seg == ".."))
    {
        return Some(bad_request("bars_file must be a relative reference"));
    }
    match (bars, bars_file) {
        (Some(_), Some(_)) | (None, None) => {
            return Some(invalid("exactly one of bars / bars_file is required"));
        }
        (Some(b), None) if b.len() > MAX_INLINE_BARS => {
            return Some(invalid(format!(
                "at most {MAX_INLINE_BARS} inline bars are accepted"
            )));
        }
        _ => {}
    }
    if tail.is_some_and(|t| t > MAX_TAIL) {
        return Some(invalid(format!("tail must be at most {MAX_TAIL}")));
    }
    None
}

fn to_args<T: Serialize>(req: &T) -> Value {
    serde_json::to_value(req).unwrap_or(Value::Null)
}

/// `POST /studies/indicators/compute`.
pub(crate) async fn post_compute(
    _caller: Caller,
    State(state): State<Arc<AppState>>,
    ApiJson(req): ApiJson<IndicatorComputeRequest>,
) -> Response {
    if req.id.is_empty() {
        return invalid("id is required");
    }
    if let Some(r) = validate_bars(&req.bars, &req.bars_file, req.tail) {
        return r;
    }
    match call(&state, "tantra_indicator_compute", to_args(&req)).await {
        Ok(v) => success(v),
        Err(r) => r,
    }
}

/// `POST /studies/signal-study`.
pub(crate) async fn post_signal_study(
    _caller: Caller,
    State(state): State<Arc<AppState>>,
    ApiJson(req): ApiJson<SignalStudyRequest>,
) -> Response {
    if req.id.is_empty() {
        return invalid("id is required");
    }
    if let Some(r) = validate_bars(&req.bars, &req.bars_file, None) {
        return r;
    }
    if req
        .horizons
        .as_ref()
        .is_some_and(|h| h.len() > MAX_HORIZONS)
    {
        return invalid(format!("at most {MAX_HORIZONS} horizons are accepted"));
    }
    if req.direction.is_some_and(|d| d != 1 && d != -1) {
        return invalid("direction must be 1 or -1");
    }
    match call(&state, "tantra_signal_study", to_args(&req)).await {
        Ok(v) => success(v),
        Err(r) => r,
    }
}
