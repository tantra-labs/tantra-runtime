//! Conformance tests: the generated OpenAPI 3.1 document must cover every v1
//! route, register the wire/profile component schemas, and match the
//! checked-in static artifact (`schema/openapi.v2.json`).

use tantra_runtime_api_rest::openapi::OpenApiDocument;

/// Every route in `src/v1.rs::router()`, as (method, OpenAPI path template).
const ROUTES: &[(&str, &str)] = &[
    ("get", "/capabilities"),
    ("get", "/health"),
    ("get", "/schema"),
    ("get", "/instruments"),
    ("get", "/instruments/{id}"),
    ("get", "/quotes"),
    ("get", "/bars/{id}"),
    ("get", "/depth/{id}"),
    ("get", "/market-data/symbol-profile/{id}"),
    ("get", "/market-data/calendar"),
    ("get", "/market-data/block-deals"),
    ("post", "/bars"),
    ("get", "/trading-state"),
    ("put", "/trading-state"),
    ("get", "/profile"),
    ("put", "/profile"),
    ("get", "/profile/layouts"),
    ("put", "/profile/layouts/{name}"),
    ("delete", "/profile/layouts/{name}"),
    ("get", "/profile/modules"),
    ("put", "/profile/modules/{id}"),
    ("delete", "/profile/modules/{id}"),
    ("get", "/portfolio"),
    ("get", "/portfolio/accounts"),
    ("get", "/portfolio/positions"),
    ("get", "/portfolio/holdings"),
    ("get", "/portfolio/trades"),
    ("get", "/portfolio/chart"),
    ("get", "/portfolio/concentration"),
    ("get", "/portfolio/wac"),
    ("get", "/ledger"),
    ("get", "/tax/report"),
    ("get", "/fees"),
    ("get", "/positions"),
    ("get", "/positions/audit"),
    ("get", "/trades"),
    ("get", "/reports/ledger"),
    ("get", "/reports/tax"),
    ("get", "/reports/snapshots"),
    ("get", "/reports/audit"),
    ("get", "/reports/concentration"),
    ("get", "/reports/wac"),
    ("get", "/reports/fees"),
    ("get", "/reports/equity-curve"),
    ("get", "/reports/fills/{order_id}"),
    ("post", "/strategies"),
    ("get", "/strategies"),
    ("post", "/strategies/verify"),
    ("post", "/backtests"),
    ("get", "/backtests"),
    ("get", "/backtests/{id}"),
    ("get", "/backtests/{id}/journal"),
    ("get", "/backtests/{id}/events"),
    ("post", "/sweeps"),
    ("get", "/sweeps"),
    ("get", "/sweeps/{id}"),
    ("get", "/sweeps/{id}/journal"),
    ("post", "/orders"),
    ("get", "/orders"),
    ("delete", "/orders/{id}"),
    ("get", "/orders/{id}/fills"),
    ("post", "/positions/close"),
    ("get", "/approval/queue"),
    ("post", "/approval/approve"),
    ("post", "/approval/reject"),
    ("get", "/screener/scan"),
    ("get", "/journals/{id}"),
    ("get", "/trade-journal/accounts"),
    ("post", "/trade-journal/accounts"),
    ("get", "/trade-journal/overview"),
    ("get", "/trade-journal/trades"),
    ("patch", "/trade-journal/trades"),
    ("get", "/trade-journal/calendar"),
    ("get", "/trade-journal/notes"),
    ("post", "/trade-journal/notes"),
    ("get", "/trade-journal/compare"),
    ("get", "/ws"),
    ("get", "/ws/market"),
    ("get", "/ws/chat"),
];

/// Every v1 route has an operation entry in the generated v2 document.
#[test]
fn every_v1_route_has_an_operation() {
    let doc = OpenApiDocument::generate_v2();
    for (method, path) in ROUTES {
        let item = doc
            .paths
            .get(*path)
            .unwrap_or_else(|| panic!("missing OpenAPI path {path}"));
        let present = match *method {
            "get" => item.get.is_some(),
            "post" => item.post.is_some(),
            "put" => item.put.is_some(),
            "patch" => item.patch.is_some(),
            "delete" => item.delete.is_some(),
            other => panic!("unknown method {other}"),
        };
        assert!(present, "missing operation {method} {path}");
    }
}

/// The gap's listed wire models are registered as component schemas.
#[test]
fn wire_schemas_are_registered() {
    let doc = OpenApiDocument::generate_v2();
    for name in [
        "ResponseEnvelope",
        "ErrorDetail",
        "Profile",
        "QuoteTick",
        "TradingState",
    ] {
        assert!(
            doc.components.schemas.contains_key(name),
            "missing component schema {name}"
        );
    }
}

/// The checked-in static artifact matches the generated v2 document
/// (compared as JSON values, so formatting differences are tolerated).
#[test]
fn static_artifact_is_current() {
    let artifact_path = concat!(env!("CARGO_MANIFEST_DIR"), "/schema/openapi.v2.json");
    let artifact = std::fs::read_to_string(artifact_path)
        .unwrap_or_else(|e| panic!("failed to read {artifact_path}: {e}"));
    let artifact_value: serde_json::Value = serde_json::from_str(&artifact)
        .unwrap_or_else(|e| panic!("artifact is not valid JSON: {e}"));
    let generated = serde_json::to_value(OpenApiDocument::generate_v2())
        .expect("generated document serializes to JSON");
    assert_eq!(
        artifact_value, generated,
        "static artifact is stale; regenerate it from OpenApiDocument::v2_json()"
    );
}
