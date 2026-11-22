//! REST API v1 routes and handlers (Maintained for existing software).

use std::sync::{Arc, PoisonError};

use axum::Router;
use axum::extract::{Path, State};
use axum::handler::Handler as _;
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
use crate::route_table::RouteTable;
use crate::trade_journal::{
    get_trade_journal_accounts, get_trade_journal_calendar, get_trade_journal_compare,
    get_trade_journal_notes, get_trade_journal_overview, get_trade_journal_trades,
    patch_trade_journal_trade, post_trade_journal_account, post_trade_journal_note,
};
use crate::ws::ws_handler;
use crate::ws_live::ws_live_handler;
use crate::{
    ApiJson, AppState, capability_manifest, experimental_analysis, ingest, openapi, orders,
    portfolio, profile, runs, screener, strategy_loop, streams::get_backtest_events, trading_state,
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
    query: ApiQuery<tantra_runtime_api::OrdersQuery>,
) -> Response {
    orders::get_orders(State(state), query).await
}

/// Post orders.
pub async fn post_orders(
    State(state): State<Arc<AppState>>,
    req: ApiJson<OrdersRequest>,
) -> Response {
    orders::post_orders(State(state), req, "rest".to_string()).await
}

/// Delete order.
pub async fn delete_order(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    orders::delete_order(State(state), Path(id)).await
}

/// Close positions.
pub async fn post_close_positions(State(state): State<Arc<AppState>>) -> Response {
    orders::post_close_positions(State(state)).await
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

/// `(METHOD, path)` of every route [`router`] serves (`:param` spelling).
pub fn manifest() -> Vec<(&'static str, &'static str)> {
    table().finish().1
}

fn table() -> RouteTable {
    RouteTable::new()
        .get("/capabilities", get_capabilities)
        .get("/health", get_health)
        .get("/schema", get_schema)
        .get("/instruments", market::get_instruments)
        .get("/instruments/:id", market::get_instrument_by_id)
        .get("/quotes", market::get_quotes)
        .get("/bars/:id", market::get_bars)
        .get("/depth/:id", market::get_depth)
        // Market reference data (Phase 3): symbol profile + calendar, backed
        // by tantra-data's symbol-profile / calendar ingestion.
        .get(
            "/market-data/symbol-profile/:id",
            market::get_symbol_profile,
        )
        .get("/market-data/calendar", market::get_market_calendar)
        .get("/market-data/block-deals", market::get_block_deals)
        .post("/bars", ingest::post_bars)
        .get("/trading-state", trading_state::get_trading_state)
        .put("/trading-state", trading_state::put_trading_state)
        .get("/profile", profile::get_profile)
        .put(
            "/profile",
            profile::put_profile.layer(profile::body_limit()),
        )
        .get("/profile/layouts", profile::get_layouts)
        .put(
            "/profile/layouts/:name",
            profile::put_layout.layer(profile::body_limit()),
        )
        .delete("/profile/layouts/:name", profile::delete_layout)
        .get("/profile/modules", profile::get_modules)
        .put(
            "/profile/modules/:id",
            profile::put_module.layer(profile::body_limit()),
        )
        .delete("/profile/modules/:id", profile::delete_module)
        .get("/portfolio", portfolio::get_portfolio)
        .get("/portfolio/accounts", portfolio::get_accounts)
        .get("/portfolio/positions", portfolio::get_positions)
        .get("/portfolio/holdings", portfolio::get_holdings)
        .get("/portfolio/trades", portfolio::get_trades)
        .get("/portfolio/chart", get_equity_curve)
        .get("/portfolio/concentration", get_concentration_risk)
        .get("/portfolio/wac", get_weighted_average_cost)
        .get("/ledger", get_ledger)
        .get("/tax/report", get_tax_report)
        .get("/fees", get_fees_report)
        .get("/positions", portfolio::get_positions)
        .get("/positions/audit", get_position_audit)
        .get("/trades", portfolio::get_trades)
        .get("/reports/ledger", get_ledger)
        .get("/reports/tax", get_tax_report)
        .get("/reports/snapshots", get_portfolio_snapshots)
        .get("/reports/audit", get_position_audit)
        .get("/reports/concentration", get_concentration_risk)
        .get("/reports/wac", get_weighted_average_cost)
        .get("/reports/fees", get_fees_report)
        .get("/reports/equity-curve", get_equity_curve)
        .get("/reports/fills/:order_id", get_order_fills)
        .post("/strategies", post_strategies)
        .get("/strategies", get_strategies)
        .post("/strategies/verify", post_verify_strategy)
        .post("/backtests", runs::post_backtests)
        .get("/backtests", runs::get_backtests)
        .get("/backtests/:id", runs::get_backtest_by_id)
        .get("/backtests/:id/journal", runs::get_backtest_journal)
        .get("/backtests/:id/events", get_backtest_events)
        .post("/sweeps", post_sweeps)
        .get("/sweeps", runs::get_sweeps)
        .get("/sweeps/:id", get_sweep_by_id)
        .get("/sweeps/:id/journal", runs::get_sweep_journal)
        .post("/orders", post_orders)
        .get("/orders", get_orders)
        .delete("/orders/:id", delete_order)
        .get("/orders/:id/fills", get_order_fills)
        .post("/positions/close", post_close_positions)
        .get("/approval/queue", orders::get_approval_queue)
        .post("/approval/approve", orders::post_approve_order)
        .post("/approval/reject", orders::post_reject_order)
        .get("/screener/scan", screener::get_screener_scan)
        .get(
            "/experimental-analysis/catalog",
            experimental_analysis::get_catalog,
        )
        .post(
            "/experimental-analysis/returns",
            experimental_analysis::post_returns,
        )
        .post(
            "/experimental-analysis/intraday",
            experimental_analysis::post_intraday,
        )
        .post(
            "/experimental-analysis/correlation",
            experimental_analysis::post_correlation,
        )
        .post(
            "/experimental-analysis/options-overlay",
            experimental_analysis::post_options_overlay,
        )
        .post(
            "/experimental-analysis/forecast",
            experimental_analysis::post_forecast,
        )
        .post(
            "/experimental-analysis/events",
            experimental_analysis::post_events,
        )
        .post(
            "/experimental-analysis/outlier-news",
            experimental_analysis::post_outlier_news,
        )
        .post(
            "/studies/strategy-loop/evaluate",
            strategy_loop::post_evaluate,
        )
        .post("/studies/strategy-loop/sweep", strategy_loop::post_sweep)
        .post(
            "/studies/strategy-loop/recommend-hedge",
            strategy_loop::post_recommend_hedge,
        )
        .get("/journals/:id", runs::get_journal_by_id)
        // Trade-performance journal (distinct from run/backtest journals and
        // the operator journal): backed by tantra-ledger-performance.
        .get("/trade-journal/accounts", get_trade_journal_accounts)
        .post("/trade-journal/accounts", post_trade_journal_account)
        .get("/trade-journal/overview", get_trade_journal_overview)
        .get("/trade-journal/trades", get_trade_journal_trades)
        .patch("/trade-journal/trades/:id", patch_trade_journal_trade)
        .get("/trade-journal/calendar", get_trade_journal_calendar)
        .get("/trade-journal/notes", get_trade_journal_notes)
        .post("/trade-journal/notes", post_trade_journal_note)
        .get("/trade-journal/compare", get_trade_journal_compare)
        .get("/ws", ws_handler)
        .get("/ws/market", ws_handler)
        .get("/ws/live", ws_live_handler)
        .get("/ws/chat", chat_ws_handler)
        .get("/acp", acp_ws_handler)
}
