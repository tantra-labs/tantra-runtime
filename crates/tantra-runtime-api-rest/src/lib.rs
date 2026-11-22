#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(unsafe_code)]

//! REST API server for Tantra platform using axum.
//!
//! Provides multi-version API routing (`/api/v1`, `/api/v2`, and root fallback)
//! as specified in Phase 1 of the Versioning Design Plan.
//! All responses are wrapped in the versioned envelope from `tantra-api`.

use crate::auth::AuthRouterExt;
use axum::http::{HeaderName, HeaderValue, Method};
use axum::{
    Router, async_trait,
    extract::{FromRequest, Request, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use std::sync::Arc;
use tantra_runtime_api::{ApiResponse, ErrorCode, ErrorDetail};
use tower_http::{
    compression::CompressionLayer,
    cors::{AllowOrigin, CorsLayer},
    trace::TraceLayer,
};

mod alerts_routes;
mod approval;
mod auth;
mod bar_pump;
pub mod bootstrap;
mod caller;
mod catalog;
pub mod chat;
pub mod chat_alerts;
pub mod chat_calendar;
pub mod chat_content;
pub mod chat_fund;
pub mod chat_fundamental;
pub mod chat_ipo;
pub mod chat_market;
pub mod chat_strategy_store;
pub mod chat_tax;
pub mod chat_threads;
pub mod chat_trade;
mod dispatch;
mod exec;
mod executor;
mod experimental_analysis;
mod ingest;
mod journal;
pub mod library_client;
mod library_routes;
mod market;
pub mod market_feed;
pub mod openapi;
mod operator_journal;
pub mod orders;
pub mod paper_serve;
mod portfolio;
mod portfolio_store;
mod profile;
mod profile_store;
mod registry;
mod reports;
mod retention;
mod route_table;
mod run_store;
mod runs;
mod screener;
mod service;
mod state;
mod strategy_loop;
mod streams;
pub mod ticks_file;
mod trade_journal;
mod trades;
mod trading_state;
mod user_doc;
pub mod v1;
pub mod v2;
mod v2_canonical;
pub mod ws;
mod ws_live;

pub use approval::ApprovalQueue;
pub use auth::{
    AuthConfig, AuthLayer, FileTokenStore, JwtClaims, JwtConfig, RouteScope, TokenInfo, scope_for,
    unscoped_routes,
};
pub use bar_pump::BarPumpReport;
pub use bootstrap::{
    PreparedServer, ServeAuthConfig, ServeConfig, ServeFillConfig, ServeIngestConfig, ServeOutcome,
    ServeRiskConfig, ServeRunsConfig, TickSource, load_risk_limits, non_loopback_warning,
    parse_fill_model, parse_trading_state, parse_trading_state_command, prepare_server,
    run_server_blocking, to_api_trading_state,
};
pub use catalog::{CATALOG_DIR, CatalogBars, CatalogLoadError, load_catalog, load_lakehouse_v3};
pub use chat::{
    ChatAttachmentInput, ChatClientMessage, ChatRuntime, LlmProviderRegistry, acp_ws_handler,
    chat_scope_name, chat_ws_handler,
};
pub use chat_threads::{
    ChatThreadDetailDto, ChatThreadDto, ChatThreadMessageDto, ChatThreadsListDto,
    CreateChatThreadRequest, DeleteChatThreadResponse, PatchChatThreadRequest,
};
pub use dispatch::{DispatchError, build_target, dispatch, dispatch_stream};
pub use executor::BacktestExecutor;
pub use journal::{FileJournalWriter, NdjsonJournal, recover_interrupted_runs};
pub use market::{ApiQuery, ApiQueryRejection, MAX_BAR_ROWS};
pub use orders::OrderLedger;
pub use portfolio_store::{
    PortfolioAccountRecord, PortfolioDocument, PortfolioError, PortfolioHeldPosition,
    PortfolioStore,
};
pub use profile_store::{DEFAULT_USER_ID, Module, Profile, ProfileError, ProfileStore, Source};
pub use registry::StrategyRegistry;
pub use retention::{RetentionPolicy, evictions};
pub use route_table::{RouteEntry, V2Disposition};
pub use run_store::{OsEntropy, RecoveryReport, RunClock, RunEntropy, RunStore, SystemClock};
pub use service::{
    DEFAULT_MAX_QUEUED, DEFAULT_SHUTDOWN_GRACE, RunService, RunServiceConfig, ShutdownReport,
};
pub use state::{AppState, LoopSessionStore};
pub use strategy_loop::RuntimeStrategyLoopPort;
pub use strategy_loop::RuntimeStrategyRunner;
pub use streams::get_backtest_events;
pub use tantra_runtime_api::JournalWriter;
pub use ticks_file::{
    ParquetBarsSummary, equity_curve, import_ticks_ndjson, load_parquet_bars_summary,
    load_ticks_ndjson, pair_fills, parse_ticks_ndjson,
};
pub use ws::ws_handler;
pub use ws_live::ws_live_handler;

pub use v1::{
    delete_order, get_capabilities, get_health, get_orders, get_schema, get_strategies,
    get_sweep_by_id, post_close_positions, post_orders, post_strategies, post_sweeps,
    post_verify_strategy,
};

/// Serialized OpenAPI 3.1 document (v2) for codegen file emission (`tantra schema --openapi`).
pub fn openapi_json() -> serde_json::Value {
    openapi::OpenApiDocument::openapi_json()
}

/// JSON body extractor whose rejections are the standard error envelope
/// (`validation_invalid_request`) rather than axum's plain-text message.
#[derive(Debug)]
pub struct ApiJson<T>(pub T);

/// Rejection of [`ApiJson`]: keeps axum's status, wraps its message in an envelope.
#[derive(Debug)]
pub struct ApiJsonRejection(JsonRejection);

impl IntoResponse for ApiJsonRejection {
    fn into_response(self) -> Response {
        let status = self.0.status();
        let detail = ErrorDetail::new(ErrorCode::ValidationInvalidRequest, self.0.body_text());
        (
            status,
            Json(ApiResponse::error::<serde_json::Value>(detail)),
        )
            .into_response()
    }
}

#[async_trait]
impl<S, T> FromRequest<S> for ApiJson<T>
where
    Json<T>: FromRequest<S, Rejection = JsonRejection>,
    S: Send + Sync,
{
    type Rejection = ApiJsonRejection;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        Json::<T>::from_request(req, state)
            .await
            .map(|Json(v)| Self(v))
            .map_err(ApiJsonRejection)
    }
}

/// Server configuration beyond the data: today only the CORS allow-list and auth.
#[derive(Clone, Debug, Default)]
pub struct ApiConfig {
    cors_origins: Vec<HeaderValue>,
    /// Auth configuration; when `tokens_file` is Some, Bearer token auth is enforced
    pub auth: AuthConfig,
}

impl ApiConfig {
    /// The allowed CORS origins; empty means CORS is off.
    pub fn cors_origins(&self) -> &[HeaderValue] {
        &self.cors_origins
    }

    /// Allows exactly these origins (e.g. `https://app.example`); an entry that cannot be a
    /// header value, or a bare `*`, is an error.
    pub fn with_cors_origins<I, S>(mut self, origins: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for origin in origins {
            let origin = origin.as_ref();
            if origin == "*" {
                return Err("cors origin '*' is not allowed; list explicit origins".to_owned());
            }
            let value = HeaderValue::from_str(origin)
                .map_err(|_| format!("cors origin {origin:?} is not a valid header value"))?;
            self.cors_origins.push(value);
        }
        Ok(self)
    }
}

/// Middleware providing RFC 8594 deprecation and canonical link headers for legacy un-prefixed requests.
/// Supports content negotiation via `Accept: application/vnd.tantra.v2+json` or `X-Tantra-Version: 2`.
pub async fn legacy_v1_deprecation_notice(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = req.uri().path().to_owned();
    let wants_v2 = req
        .headers()
        .get("x-tantra-version")
        .and_then(|v| v.to_str().ok())
        .map(|v| v == "2")
        .unwrap_or(false)
        || req
            .headers()
            .get(axum::http::header::ACCEPT)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.contains("application/vnd.tantra.v2+json"))
            .unwrap_or(false);

    let res = next.run(req).await;
    let (mut parts, body) = res.into_parts();

    if wants_v2 {
        parts.headers.insert(
            HeaderName::from_static("x-tantra-version"),
            HeaderValue::from_static("2"),
        );
        let link = format!("</api/v2{path}>; rel=\"canonical\"");
        if let Ok(val) = HeaderValue::from_str(&link) {
            parts.headers.insert(axum::http::header::LINK, val);
        }
        let bytes = match axum::body::to_bytes(body, usize::MAX).await {
            Ok(b) => b,
            Err(_) => return Response::from_parts(parts, axum::body::Body::empty()),
        };
        if let Ok(mut val) = serde_json::from_slice::<serde_json::Value>(&bytes) {
            if val.is_object() && val.get("api_version").is_some() {
                val["api_version"] = serde_json::json!("v2");
                let new_body = serde_json::to_vec(&val).unwrap_or_default();
                parts.headers.insert(
                    axum::http::header::CONTENT_LENGTH,
                    HeaderValue::from(new_body.len()),
                );
                return Response::from_parts(parts, axum::body::Body::from(new_body));
            }
        }
        Response::from_parts(parts, axum::body::Body::from(bytes))
    } else {
        parts.headers.insert(
            HeaderName::from_static("deprecation"),
            HeaderValue::from_static("true"),
        );
        parts.headers.insert(
            HeaderName::from_static("x-tantra-version"),
            HeaderValue::from_static("1"),
        );
        let link = format!("</api/v1{path}>; rel=\"canonical\"");
        if let Ok(val) = HeaderValue::from_str(&link) {
            parts.headers.insert(axum::http::header::LINK, val);
        }
        Response::from_parts(parts, body)
    }
}

/// Adds RFC 8594 `Deprecation` + successor `Link` headers to the superseded study paths
/// (`/api/{v1,v2}/experimental-analysis/*`, `/api/v1/studies/*`); canonical home is
/// `/api/v2/studies/...`.
pub async fn legacy_studies_deprecation_notice(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = req.uri().path().to_owned();
    let successor = ["/api/v1/", "/api/v2/"].iter().find_map(|p| {
        let rest = path.strip_prefix(p)?;
        if rest.starts_with("experimental-analysis/") {
            Some(format!("/api/v2/studies/{rest}"))
        } else if *p == "/api/v1/" && rest.starts_with("studies/") {
            Some(format!("/api/v2/{rest}"))
        } else {
            None
        }
    });
    let mut res = next.run(req).await;
    if let Some(successor) = successor {
        res.headers_mut().insert(
            HeaderName::from_static("deprecation"),
            HeaderValue::from_static("true"),
        );
        if let Ok(val) = HeaderValue::from_str(&format!("<{successor}>; rel=\"successor-version\""))
        {
            res.headers_mut().insert(axum::http::header::LINK, val);
        }
    }
    res
}

/// Create the API router over an empty catalogue.
pub fn api_router() -> Router {
    api_router_with(AppState::default())
}

/// Create the API router serving `state`.
pub fn api_router_with(state: AppState) -> Router {
    api_router_with_config(state, &ApiConfig::default())
}

/// Create the API router serving `state` under `config`.
pub fn api_router_with_config(state: AppState, config: &ApiConfig) -> Router {
    let state = Arc::new(state);
    if let Some(chat) = state.chat.as_ref() {
        chat.bind_state(&state);
    }

    let v1_router = v1::router();
    let v2_router = v2::router();
    let (root_routes, _) = root_table().finish();

    let router = Router::new()
        .nest("/api/v1", v1_router.clone())
        .nest("/api/v2", v2_router)
        // Root fallback defaults to v1 for existing un-prefixed clients with deprecation warning
        .nest(
            "/",
            v1_router.layer(axum::middleware::from_fn(legacy_v1_deprecation_notice)),
        )
        // OpenAPI specifications per version
        .merge(root_routes)
        .fallback(unknown_route)
        .method_not_allowed_fallback(unsupported_method)
        .with_state(state)
        .layer(axum::middleware::from_fn(legacy_studies_deprecation_notice))
        .layer(CompressionLayer::new())
        .layer(cors_layer(config))
        .layer(TraceLayer::new_for_http());

    // Add auth middleware if configured
    if config.auth.is_enabled() {
        let audit_log_path = config.auth.audit_log_path.clone();
        let store = config
            .auth
            .tokens_file
            .as_ref()
            .map(|f| FileTokenStore::new(f.clone()).expect("failed to load token file"));
        let auth_layer = Arc::new(AuthLayer::from_parts(
            store,
            audit_log_path,
            config.auth.jwt_config.clone(),
        ));
        router.with_auth(auth_layer)
    } else {
        router
    }
}

/// Root-level documentation routes (GET only), shared by the router and [`served_routes`].
fn root_table() -> route_table::RouteTable {
    route_table::RouteTable::new()
        .get(
            "/schema/openapi/v1.json",
            openapi::get_openapi_v1,
            RouteScope::Public,
        )
        .get(
            "/schema/openapi/v2.json",
            openapi::get_openapi_v2,
            RouteScope::Public,
        )
        .get(
            "/schema/openapi",
            openapi::get_openapi_index,
            RouteScope::Public,
        )
        .get("/docs", openapi::get_swagger_ui, RouteScope::Public)
        .get("/swagger-ui", openapi::get_swagger_ui, RouteScope::Public)
}

/// The v1, v2-only and root route entries, each as registered.
pub(crate) fn registry_parts() -> (Vec<RouteEntry>, Vec<RouteEntry>, Vec<RouteEntry>) {
    (
        v1::manifest(),
        library_routes::v2_only_manifest()
            .into_iter()
            .chain(chat_threads::v2_only_manifest())
            .chain(v2_canonical::v2_only_manifest())
            .chain(alerts_routes::v2_only_manifest())
            .collect(),
        root_table().finish().1,
    )
}

/// The route registry: every route the router serves with its auth scope and body cap
/// (design 0006 R1a). It is the single source of truth `scope_for` and the drift guards read.
/// Paths are relative to the version prefix, in the router's `:param` spelling.
pub fn route_registry() -> &'static [RouteEntry] {
    static REGISTRY: std::sync::OnceLock<Vec<RouteEntry>> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| {
        let (v1, v2_only, root) = registry_parts();
        v1.into_iter().chain(v2_only).chain(root).collect()
    })
}

/// `(METHOD, full_path)` for every route the real router serves: v1 under `/api/v1` and the
/// legacy root, v1 plus the v2-only routes under `/api/v2`, and the root documentation routes.
/// The auth drift guard checks each of these has a scope row (design 0006 R0).
pub fn served_routes() -> Vec<(String, String)> {
    let (v1, v2_only, root) = registry_parts();
    let mut out = Vec::new();
    let join = |prefix: &str, path: &str| {
        if prefix.is_empty() {
            path.to_owned()
        } else {
            format!("{prefix}{path}")
        }
    };
    for prefix in ["/api/v1", ""] {
        for e in &v1 {
            out.push((e.method.to_owned(), join(prefix, e.path)));
        }
    }
    for e in v1.iter().chain(v2_only.iter()) {
        out.push((e.method.to_owned(), join("/api/v2", e.path)));
    }
    for e in &root {
        out.push((e.method.to_owned(), e.path.to_owned()));
    }
    out
}

fn versioned_failure(uri: &axum::http::Uri, status: StatusCode, detail: ErrorDetail) -> Response {
    let is_v2 = uri.path() == "/api/v2" || uri.path().starts_with("/api/v2/");
    if is_v2 {
        let mut body: tantra_runtime_api::ResponseEnvelope<serde_json::Value> =
            ApiResponse::error(detail);
        body.api_version = tantra_runtime_api::ApiVersion::from("v2");
        let mut res = (status, Json(body)).into_response();
        res.headers_mut().insert(
            HeaderName::from_static("x-tantra-version"),
            HeaderValue::from_static("2"),
        );
        res
    } else {
        market::failure(status, detail)
    }
}

/// An unknown route is a 404 `not_found` envelope, never an empty body.
async fn unknown_route(uri: axum::http::Uri) -> Response {
    versioned_failure(
        &uri,
        StatusCode::NOT_FOUND,
        ErrorDetail::new(
            ErrorCode::NotFound,
            format!("no such route: {}", uri.path()),
        ),
    )
}

/// A known path with a method it does not serve is a 405 `unsupported` envelope.
async fn unsupported_method(method: Method, uri: axum::http::Uri) -> Response {
    versioned_failure(
        &uri,
        StatusCode::METHOD_NOT_ALLOWED,
        ErrorDetail::new(
            ErrorCode::NotImplemented,
            format!("{method} is not supported on {}", uri.path()),
        ),
    )
}

/// No CORS headers unless origins are listed; then only for those, read methods only.
fn cors_layer(config: &ApiConfig) -> CorsLayer {
    if config.cors_origins.is_empty() {
        return CorsLayer::new();
    }
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(config.cors_origins.clone()))
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([axum::http::header::CONTENT_TYPE])
}

/// Serves [`api_router_with`]`(state)` on `listener` until `shutdown` completes.
pub async fn serve(
    listener: tokio::net::TcpListener,
    state: AppState,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    serve_with_config(listener, state, &ApiConfig::default(), shutdown).await
}

/// [`serve`] under an explicit [`ApiConfig`].
pub async fn serve_with_config(
    listener: tokio::net::TcpListener,
    state: AppState,
    config: &ApiConfig,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let router = api_router_with_config(state, config);
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
}

/// Routes still answering 501 `not_implemented`.
pub const NOT_IMPLEMENTED_ENDPOINTS: &[(&str, &str)] = &[];

fn endpoint_key((method, path): &(&str, &str)) -> String {
    format!("{method} {path}")
}

/// The capability manifest: endpoints come from the registry, never a hand-kept list.
pub(crate) fn capability_manifest() -> tantra_runtime_api::CapabilityManifest {
    tantra_runtime_api::CapabilityManifest {
        crates: vec![
            "tantra-api".to_string(),
            "tantra-api-rest".to_string(),
            "tantra-messages".to_string(),
            "tantra-entities".to_string(),
        ],
        market_packs: vec!["india".to_string(), "null".to_string()],
        endpoints: tantra_runtime_api::ENDPOINTS
            .iter()
            .map(endpoint_key)
            .collect(),
        not_implemented: NOT_IMPLEMENTED_ENDPOINTS.iter().map(endpoint_key).collect(),
        toolsets: vec!["strategies".to_string(), "indicators".to_string()],
        adapters: vec![],
        features: std::collections::BTreeMap::new(),
    }
}
