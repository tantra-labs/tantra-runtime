//! REST API v1 routes and handlers (Maintained for existing software).

use std::sync::{Arc, PoisonError};

use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{Json, Response};
use tantra_runtime_api::{
    ApiResponse, Capabilities, CapabilitiesResponse, CompiledStrategy, OrdersRequest,
    ResponseEnvelope, StrategiesResponse, SweepRequest, VerifyStrategyRequest,
    VerifyStrategyResponse,
};

use crate::chat::{acp_ws_handler, chat_ws_handler};
use crate::market::{self, ApiQuery};
use crate::reports::{
    get_concentration_risk, get_equity_curve, get_fees_report, get_ledger, get_order_fills,
    get_portfolio_snapshots, get_position_audit, get_tax_report, get_weighted_average_cost,
};
use crate::route_table::{RouteEntry, RouteTable};
use crate::trade_journal::{
    get_trade_journal_accounts, get_trade_journal_calendar, get_trade_journal_compare,
    get_trade_journal_notes, get_trade_journal_overview, get_trade_journal_trades,
    patch_trade_journal_trade, post_trade_journal_account, post_trade_journal_note,
};
use crate::ws::ws_handler;
use crate::ws_live::ws_live_handler;
use crate::{
    ApiJson, AppState, RouteScope, capability_manifest, experimental_analysis, ingest, openapi,
    orders, portfolio, profile, runs, screener, strategy_loop, streams::get_backtest_events,
    trading_state,
};

/// Get API capabilities for v1.
pub async fn get_capabilities(
    State(_state): State<Arc<AppState>>,
) -> Json<ResponseEnvelope<CapabilitiesResponse>> {
    Json(ApiResponse::success(Capabilities {
        capabilities: capability_manifest(),
    }))
}

/// Get health status.
pub async fn get_health(
    State(state): State<Arc<AppState>>,
) -> Json<ResponseEnvelope<serde_json::Value>> {
    let library = match state.library() {
        None => "disabled",
        Some(c) if c.is_healthy() => "healthy",
        Some(_) => "unhealthy",
    };
    Json(ApiResponse::success(
        serde_json::json!({"status": "ok", "library": library}),
    ))
}

/// Get schema bundle — full OpenAPI 3.1 document.
pub async fn get_schema() -> Json<ResponseEnvelope<serde_json::Value>> {
    let doc = openapi::OpenApiDocument::generate_v1();
    Json(ApiResponse::success(serde_json::to_value(&doc).unwrap()))
}

/// Lists the strategies compiled in this process, ordered by id.
pub async fn get_strategies(
    State(state): State<Arc<AppState>>,
) -> Json<ResponseEnvelope<StrategiesResponse>> {
    let catalog = state
        .strategies
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    Json(ApiResponse::success(tantra_runtime_api::list_strategies(
        &catalog,
    )))
}

/// Compiles a manifest (the same step as verify) and keeps it in the session catalog.
pub async fn post_strategies(
    State(state): State<Arc<AppState>>,
    ApiJson(body): ApiJson<serde_json::Value>,
) -> (StatusCode, Json<ResponseEnvelope<CompiledStrategy>>) {
    let compiled = tantra_runtime_api::parse_compile_request(body).and_then(|req| {
        let mut catalog = state
            .strategies
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        tantra_runtime_api::compile_strategy(&mut catalog, req.manifest)
    });
    match compiled {
        Ok(strategy) => (StatusCode::OK, Json(ApiResponse::success(strategy))),
        Err(detail) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(ApiResponse::error(detail)),
        ),
    }
}

/// Verifies a manifest and returns its IR.
pub async fn post_verify_strategy(
    ApiJson(manifest): ApiJson<VerifyStrategyRequest>,
) -> (StatusCode, Json<ResponseEnvelope<VerifyStrategyResponse>>) {
    match tantra_runtime_api::verify_strategy(manifest) {
        Ok(ir) => (StatusCode::OK, Json(ApiResponse::success(ir))),
        Err(detail) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(ApiResponse::error(detail)),
        ),
    }
}

/// Post sweeps.
pub async fn post_sweeps(
    State(state): State<Arc<AppState>>,
    ApiJson(request): ApiJson<SweepRequest>,
) -> Response {
    runs::post_sweeps(State(state), ApiJson(request)).await
}

/// Get sweep by id.
pub async fn get_sweep_by_id(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    runs::get_sweep_by_id(State(state), Path(id)).await
}

/// Get orders.
pub async fn get_orders(
    State(state): State<Arc<AppState>>,
    caller: crate::caller::Caller,
    query: ApiQuery<tantra_runtime_api::OrdersQuery>,
) -> Response {
    orders::get_orders(State(state), caller, query).await
}

/// Post orders.
///
/// Ledger key, in order: the `Idempotency-Key` header, the body's `client_order_id`, else a
/// fresh UUID (`.agents/rules/security-and-secrets.md` §2.3: never a constant key, which would
/// make every keyless order a duplicate of the first). Differing header and body keys are a 422.
pub async fn post_orders(
    State(state): State<Arc<AppState>>,
    caller: crate::caller::Caller,
    headers: axum::http::HeaderMap,
    ApiJson(mut body): ApiJson<OrdersRequest>,
) -> Response {
    let bad_key = |why: &str| {
        market::failure(
            StatusCode::UNPROCESSABLE_ENTITY,
            tantra_runtime_api::ErrorDetail::new(
                tantra_runtime_api::ErrorCode::ValidationInvalidRequest,
                why.to_string(),
            )
            .with_context(serde_json::json!({"field": "Idempotency-Key"})),
        )
    };
    if let Some(raw) = headers.get("idempotency-key") {
        let key = match raw.to_str().map(str::trim) {
            Ok(k) if (1..=128).contains(&k.chars().count()) => k.to_string(),
            _ => return bad_key("Idempotency-Key must be 1-128 visible characters"),
        };
        match body.client_order_id.as_deref() {
            Some(existing) if existing != key => {
                return bad_key("Idempotency-Key header differs from client_order_id in the body");
            }
            _ => body.client_order_id = Some(key),
        }
    }
    let order_id = uuid::Uuid::new_v4().to_string();
    orders::post_orders(State(state), ApiJson(body), order_id, &caller.0).await
}

/// Delete order.
pub async fn delete_order(
    State(state): State<Arc<AppState>>,
    caller: crate::caller::Caller,
    Path(id): Path<String>,
) -> Response {
    orders::delete_order(State(state), caller, Path(id)).await
}

/// Close positions.
pub async fn post_close_positions(
    State(state): State<Arc<AppState>>,
    caller: crate::caller::Caller,
) -> Response {
    orders::post_close_positions(State(state), caller).await
}

/// Adds v1 version header to response.
pub async fn v1_headers(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let mut res = next.run(req).await;
    res.headers_mut().insert(
        axum::http::HeaderName::from_static("x-tantra-version"),
        axum::http::HeaderValue::from_static("1"),
    );
    res
}

/// Build the v1 router with attached state.
pub fn routes(state: Arc<AppState>) -> Router {
    router().with_state(state)
}

/// Build the v1 router expecting State<Arc<AppState>>.
pub fn router() -> Router<Arc<AppState>> {
    let (router, _) = table().finish();
    router.layer(axum::middleware::from_fn(v1_headers))
}

/// The `/api/v2` view of the v1 table: routes with a canonical successor answer `308` (or
/// carry deprecation headers); everything else is served as in v1.
pub fn v2_router() -> Router<Arc<AppState>> {
    table().finish_both().1
}

/// Every route [`router`] serves, with its scope and body cap (`:param` spelling).
pub fn manifest() -> Vec<RouteEntry> {
    table().finish().1
}

const PUBLIC: RouteScope = RouteScope::Public;
const AUTHED: RouteScope = RouteScope::Authenticated;
const READ: RouteScope = RouteScope::Required("read");
const TRADE: RouteScope = RouteScope::Required("trade");
const ADMIN: RouteScope = RouteScope::Required("admin");
const RESEARCH: RouteScope = RouteScope::Required("research");

/// Body cap of the `POST /bars` ingest route.
const BARS_BODY_BYTES: usize = 16 * 1024 * 1024;
/// Body cap of strategy, backtest, sweep and study POST routes.
const STUDY_BODY_BYTES: usize = 4 * 1024 * 1024;

fn table() -> RouteTable {
    RouteTable::new()
        .get("/capabilities", get_capabilities, PUBLIC)
        .get("/health", get_health, PUBLIC)
        .get("/schema", get_schema, PUBLIC)
        .get("/instruments", market::get_instruments, READ)
        .get("/instruments/:id", market::get_instrument_by_id, READ)
        .get("/quotes", market::get_quotes, READ)
        .get("/bars/:id", market::get_bars, READ)
        .get("/depth/:id", market::get_depth, READ)
        // Market reference data (Phase 3): symbol profile + calendar, backed
        // by tantra-data's symbol-profile / calendar ingestion.
        .v2_redirect("/instruments/:id/profile")
        .get(
            "/market-data/symbol-profile/:id",
            market::get_symbol_profile,
            READ,
        )
        .v2_redirect("/calendar")
        .get("/market-data/calendar", market::get_market_calendar, READ)
        .v2_redirect("/block-deals")
        .get("/market-data/block-deals", market::get_block_deals, READ)
        .post_capped("/bars", ingest::post_bars, TRADE, BARS_BODY_BYTES)
        .get("/trading-state", trading_state::get_trading_state, ADMIN)
        .put("/trading-state", trading_state::put_trading_state, ADMIN)
        .get("/profile", profile::get_profile, AUTHED)
        .put("/profile", profile::put_profile, AUTHED)
        .get("/profile/layouts", profile::get_layouts, AUTHED)
        .put("/profile/layouts/:name", profile::put_layout, AUTHED)
        .delete("/profile/layouts/:name", profile::delete_layout, AUTHED)
        .get("/profile/modules", profile::get_modules, AUTHED)
        .put("/profile/modules/:id", profile::put_module, AUTHED)
        .delete("/profile/modules/:id", profile::delete_module, AUTHED)
        .get("/portfolio", portfolio::get_portfolio, READ)
        .get("/portfolio/accounts", portfolio::get_accounts, READ)
        .get("/portfolio/positions", portfolio::get_positions, READ)
        .get("/portfolio/holdings", portfolio::get_holdings, READ)
        .get("/portfolio/trades", portfolio::get_trades, READ)
        .v2_redirect("/reports/equity-curve")
        .get("/portfolio/chart", get_equity_curve, READ)
        .v2_redirect("/reports/concentration")
        .get("/portfolio/concentration", get_concentration_risk, READ)
        .v2_redirect("/reports/wac")
        .get("/portfolio/wac", get_weighted_average_cost, READ)
        .v2_redirect("/reports/ledger")
        .get("/ledger", get_ledger, READ)
        .v2_redirect("/reports/tax")
        .get("/tax/report", get_tax_report, READ)
        .v2_redirect("/reports/fees")
        .get("/fees", get_fees_report, READ)
        .v2_redirect("/portfolio/positions")
        .get("/positions", portfolio::get_positions, READ)
        .v2_redirect("/reports/audit")
        .get("/positions/audit", get_position_audit, READ)
        .v2_redirect("/portfolio/trades")
        .get("/trades", portfolio::get_trades, READ)
        .get("/reports/ledger", get_ledger, READ)
        .get("/reports/tax", get_tax_report, READ)
        .get("/reports/snapshots", get_portfolio_snapshots, READ)
        .get("/reports/audit", get_position_audit, READ)
        .get("/reports/concentration", get_concentration_risk, READ)
        .get("/reports/wac", get_weighted_average_cost, READ)
        .get("/reports/fees", get_fees_report, READ)
        .get("/reports/equity-curve", get_equity_curve, READ)
        .v2_redirect("/orders/:order_id/fills")
        .get("/reports/fills/:order_id", get_order_fills, READ)
        .post_capped("/strategies", post_strategies, RESEARCH, STUDY_BODY_BYTES)
        .get("/strategies", get_strategies, RESEARCH)
        .post_capped(
            "/strategies/verify",
            post_verify_strategy,
            RESEARCH,
            STUDY_BODY_BYTES,
        )
        .post_capped(
            "/backtests",
            runs::post_backtests,
            RESEARCH,
            STUDY_BODY_BYTES,
        )
        .get("/backtests", runs::get_backtests, RESEARCH)
        .get("/backtests/:id", runs::get_backtest_by_id, RESEARCH)
        .get(
            "/backtests/:id/journal",
            runs::get_backtest_journal,
            RESEARCH,
        )
        .get("/backtests/:id/events", get_backtest_events, RESEARCH)
        .post_capped("/sweeps", post_sweeps, RESEARCH, STUDY_BODY_BYTES)
        .get("/sweeps", runs::get_sweeps, RESEARCH)
        .get("/sweeps/:id", get_sweep_by_id, RESEARCH)
        .get("/sweeps/:id/journal", runs::get_sweep_journal, RESEARCH)
        .post("/orders", post_orders, TRADE)
        .get("/orders", get_orders, TRADE)
        .v2_deprecated("/orders/:id/cancel")
        .delete("/orders/:id", delete_order, TRADE)
        .get("/orders/:id/fills", get_order_fills, READ)
        .post("/positions/close", post_close_positions, TRADE)
        .v2_redirect("/approvals")
        .get("/approval/queue", orders::get_approval_queue, ADMIN)
        .v2_deprecated("/approvals/:id/approve")
        .post("/approval/approve", orders::post_approve_order, ADMIN)
        .v2_deprecated("/approvals/:id/reject")
        .post("/approval/reject", orders::post_reject_order, ADMIN)
        .v2_redirect("/screener/scans")
        .get("/screener/scan", screener::get_screener_scan, READ)
        .v2_redirect("/studies/experimental-analysis/catalog")
        .get(
            "/experimental-analysis/catalog",
            experimental_analysis::get_catalog,
            READ,
        )
        .v2_redirect("/studies/experimental-analysis/returns")
        .post_capped(
            "/experimental-analysis/returns",
            experimental_analysis::post_returns,
            READ,
            STUDY_BODY_BYTES,
        )
        .v2_redirect("/studies/experimental-analysis/intraday")
        .post_capped(
            "/experimental-analysis/intraday",
            experimental_analysis::post_intraday,
            READ,
            STUDY_BODY_BYTES,
        )
        .v2_redirect("/studies/experimental-analysis/correlation")
        .post_capped(
            "/experimental-analysis/correlation",
            experimental_analysis::post_correlation,
            READ,
            STUDY_BODY_BYTES,
        )
        .v2_redirect("/studies/experimental-analysis/options-overlay")
        .post_capped(
            "/experimental-analysis/options-overlay",
            experimental_analysis::post_options_overlay,
            READ,
            STUDY_BODY_BYTES,
        )
        .v2_redirect("/studies/experimental-analysis/forecast")
        .post_capped(
            "/experimental-analysis/forecast",
            experimental_analysis::post_forecast,
            READ,
            STUDY_BODY_BYTES,
        )
        .v2_redirect("/studies/experimental-analysis/events")
        .post_capped(
            "/experimental-analysis/events",
            experimental_analysis::post_events,
            READ,
            STUDY_BODY_BYTES,
        )
        .v2_redirect("/studies/experimental-analysis/outlier-news")
        .post_capped(
            "/experimental-analysis/outlier-news",
            experimental_analysis::post_outlier_news,
            READ,
            STUDY_BODY_BYTES,
        )
        .post_capped(
            "/studies/strategy-loop/evaluate",
            strategy_loop::post_evaluate,
            READ,
            STUDY_BODY_BYTES,
        )
        .post_capped(
            "/studies/strategy-loop/sweep",
            strategy_loop::post_sweep,
            READ,
            STUDY_BODY_BYTES,
        )
        .post_capped(
            "/studies/strategy-loop/recommend-hedge",
            strategy_loop::post_recommend_hedge,
            READ,
            STUDY_BODY_BYTES,
        )
        .v2_deprecated("/backtests/:id/journal")
        .get("/journals/:id", runs::get_journal_by_id, RESEARCH)
        // Trade-performance journal (distinct from run/backtest journals and
        // the operator journal): backed by tantra-ledger-performance.
        .get("/trade-journal/accounts", get_trade_journal_accounts, READ)
        .post("/trade-journal/accounts", post_trade_journal_account, TRADE)
        .get("/trade-journal/overview", get_trade_journal_overview, READ)
        .get("/trade-journal/trades", get_trade_journal_trades, READ)
        .patch(
            "/trade-journal/trades/:id",
            patch_trade_journal_trade,
            TRADE,
        )
        .get("/trade-journal/calendar", get_trade_journal_calendar, READ)
        .get("/trade-journal/notes", get_trade_journal_notes, READ)
        .post("/trade-journal/notes", post_trade_journal_note, TRADE)
        .get("/trade-journal/compare", get_trade_journal_compare, READ)
        .v2_deprecated("/stream")
        .get("/ws", ws_handler, PUBLIC)
        .v2_deprecated("/stream")
        .get("/ws/market", ws_handler, PUBLIC)
        // Paper-exchange live socket: accepts place/cancel order commands.
        .v2_deprecated("/stream")
        .get("/ws/live", ws_live_handler, TRADE)
        // Chat WebSocket: hits a real LLM, unlike the public /ws and /ws/market feeds, so it
        // requires any valid token rather than being public.
        .get("/ws/chat", chat_ws_handler, AUTHED)
        // ACP agent WebSocket (agent plan 0019 Phase 2): real LLM + tools, any valid token.
        .get("/acp", acp_ws_handler, AUTHED)
}
