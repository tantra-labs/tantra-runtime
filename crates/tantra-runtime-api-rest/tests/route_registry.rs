//! The route registry is the single source of truth for each route's auth scope and
//! request-body cap (design 0006 R1a).

use std::collections::HashSet;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tantra_runtime_api_rest::{RouteScope, api_router, route_registry, served_routes};
use tower::util::ServiceExt;

const PUBLIC: RouteScope = RouteScope::Public;
const AUTHED: RouteScope = RouteScope::Authenticated;
const READ: RouteScope = RouteScope::Required("read");
const TRADE: RouteScope = RouteScope::Required("trade");
const ADMIN: RouteScope = RouteScope::Required("admin");
const RESEARCH: RouteScope = RouteScope::Required("research");

/// `(METHOD, path, scope, v2_only)` snapshot, one or more rows per scope class.
const SNAPSHOT: &[(&str, &str, RouteScope, bool)] = &[
    ("GET", "/health", PUBLIC, false),
    ("GET", "/bars/:id", READ, false),
    ("POST", "/orders", TRADE, false),
    ("PUT", "/trading-state", ADMIN, false),
    ("GET", "/profile", AUTHED, false),
    ("POST", "/backtests", RESEARCH, false),
    ("GET", "/acp", AUTHED, false),
    ("GET", "/ws/live", TRADE, false),
    ("POST", "/trade-journal/notes", TRADE, false),
    ("GET", "/market-data/calendar", READ, false),
    ("GET", "/studies/library/entries", READ, true),
    ("GET", "/ws", PUBLIC, false),
    ("POST", "/approval/approve", ADMIN, false),
    ("GET", "/docs", PUBLIC, false),
];

#[test]
fn scope_snapshot_matches_registry() {
    for (method, path, scope, v2_only) in SNAPSHOT {
        let entry = route_registry()
            .iter()
            .find(|e| e.method == *method && e.path == *path && e.v2_only == *v2_only)
            .unwrap_or_else(|| panic!("{method} {path} is not in the registry"));
        assert_eq!(entry.scope, *scope, "{method} {path}");
    }
}

#[test]
fn every_body_method_has_a_cap() {
    for e in route_registry() {
        let has_body = matches!(e.method, "POST" | "PUT" | "PATCH");
        assert_eq!(
            e.body_limit.is_some(),
            has_body,
            "{} {} body cap",
            e.method,
            e.path
        );
    }
}

#[test]
fn no_duplicate_entries() {
    let mut seen = HashSet::new();
    for e in route_registry() {
        assert!(
            seen.insert((e.method, e.path, e.v2_only)),
            "duplicate registry entry {} {}",
            e.method,
            e.path
        );
    }
}

#[test]
fn served_routes_length_follows_registry() {
    let reg = route_registry();
    let v2_only = reg.iter().filter(|e| e.v2_only).count();
    let v1 = reg
        .iter()
        .filter(|e| {
            !e.v2_only
                && !e.path.starts_with("/schema/openapi")
                && !e.path.starts_with("/docs")
                && e.path != "/swagger-ui"
        })
        .count();
    // Root doc routes: the five registered by `root_table`.
    let root = reg.len() - v2_only - v1;
    assert_eq!(root, 5, "expected five root documentation routes");
    assert_eq!(served_routes().len(), 2 * v1 + (v1 + v2_only) + root);
}

async fn post_status(uri: &str, len: usize) -> StatusCode {
    let res = api_router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(vec![b' '; len]))
                .unwrap(),
        )
        .await
        .unwrap();
    res.status()
}

#[tokio::test]
async fn default_body_cap_is_enforced() {
    assert_eq!(
        post_status("/api/v2/orders", 1024 * 1024 + 1).await,
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[tokio::test]
async fn raised_body_cap_admits_larger_bodies() {
    assert_ne!(
        post_status("/api/v2/backtests", 2 * 1024 * 1024).await,
        StatusCode::PAYLOAD_TOO_LARGE
    );
}
