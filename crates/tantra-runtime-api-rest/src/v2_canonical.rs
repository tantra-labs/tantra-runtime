//! Canonical `/api/v2` resource routes added by design 0006 R2a. They are mounted only under
//! `/api/v2`; the old v1-era spellings answer `308` to these (see `v1::table`).

use axum::Router;
use std::sync::Arc;

use crate::route_table::{RouteEntry, RouteTable};
use crate::{AppState, RouteScope, market, options_chain, orders, screener};

pub(crate) fn v2_only_routes() -> Router<Arc<AppState>> {
    v2_only_table().finish().0
}

/// Manifest of the canonical v2-only routes (`:param` spelling).
pub(crate) fn v2_only_manifest() -> Vec<RouteEntry> {
    v2_only_table().finish().1
}

fn v2_only_table() -> RouteTable {
    const READ: RouteScope = RouteScope::Required("read");
    const TRADE: RouteScope = RouteScope::Required("trade");
    const ADMIN: RouteScope = RouteScope::Required("admin");
    RouteTable::new_v2_only()
        .get("/orders/:id", orders::get_order_by_id, TRADE)
        .post("/orders/:id/cancel", orders::delete_order, TRADE)
        .get("/approvals", orders::get_approval_queue, ADMIN)
        .post(
            "/approvals/:id/approve",
            orders::post_approve_order_by_id,
            ADMIN,
        )
        .post(
            "/approvals/:id/reject",
            orders::post_reject_order_by_id,
            ADMIN,
        )
        .get("/screener/scans", screener::get_screener_scan, READ)
        .get("/instruments/:id/profile", market::get_symbol_profile, READ)
        .get("/calendar", market::get_market_calendar, READ)
        .get("/block-deals", market::get_block_deals, READ)
        .get("/options/chain", options_chain::get_options_chain, READ)
}
