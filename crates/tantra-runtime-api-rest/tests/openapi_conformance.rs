//! Conformance tests: the generated OpenAPI 3.1 document must cover every v1
//! route, register the wire/profile component schemas, and match the
//! checked-in static artifact (`schema/openapi.v2.json`).

use tantra_runtime_api_rest::openapi::{OpenApiDocument, Operation};
use tantra_runtime_api_rest::{RouteScope, route_registry};

fn is_root_doc_route(path: &str) -> bool {
    path.starts_with("/schema/openapi") || path == "/docs" || path == "/swagger-ui"
}

fn brace(path: &str) -> String {
    path.split('/')
        .map(|s| match s.strip_prefix(':') {
            Some(n) => format!("{{{n}}}"),
            None => s.to_string(),
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn op<'a>(doc: &'a OpenApiDocument, method: &str, path: &str) -> Option<&'a Operation> {
    let item = doc.paths.get(&brace(path))?;
    match method {
        "GET" => item.get.as_ref(),
        "POST" => item.post.as_ref(),
        "PUT" => item.put.as_ref(),
        "PATCH" => item.patch.as_ref(),
        "DELETE" => item.delete.as_ref(),
        other => panic!("unknown method {other}"),
    }
}

fn all_ops(doc: &OpenApiDocument) -> Vec<&Operation> {
    doc.paths
        .values()
        .flat_map(|i| [&i.get, &i.post, &i.put, &i.patch, &i.delete])
        .filter_map(|o| o.as_ref())
        .collect()
}

/// Every registry entry (minus root doc routes) has an operation in v2; non-v2-only in v1 too.
#[test]
fn every_registry_route_has_an_operation() {
    let v1 = OpenApiDocument::generate_v1();
    let v2 = OpenApiDocument::generate_v2();
    for e in route_registry()
        .iter()
        .filter(|e| !is_root_doc_route(e.path))
    {
        assert!(
            op(&v2, e.method, e.path).is_some(),
            "v2 missing {} {}",
            e.method,
            e.path
        );
        if !e.v2_only {
            assert!(
                op(&v1, e.method, e.path).is_some(),
                "v1 missing {} {}",
                e.method,
                e.path
            );
        }
    }
}

/// The v1 document contains no v2-only path.
#[test]
fn v1_document_has_no_v2_only_path() {
    let v1 = OpenApiDocument::generate_v1();
    for e in route_registry().iter().filter(|e| e.v2_only) {
        let still_v1 = route_registry()
            .iter()
            .any(|o| !o.v2_only && o.path == e.path && o.method == e.method);
        if !still_v1 {
            assert!(
                op(&v1, e.method, e.path).is_none(),
                "v1 has v2-only {} {}",
                e.method,
                e.path
            );
        }
    }
}

/// Every operation's security comes from its route's scope.
#[test]
fn operation_security_matches_scope() {
    let v2 = OpenApiDocument::generate_v2();
    for e in route_registry()
        .iter()
        .filter(|e| !is_root_doc_route(e.path))
    {
        let o = op(&v2, e.method, e.path).expect("operation exists");
        let got = serde_json::to_value(&o.security).expect("serializes");
        let want = match e.scope {
            RouteScope::Public => serde_json::json!([]),
            RouteScope::Required(s) => serde_json::json!([{"Bearer": [s]}]),
            RouteScope::Authenticated => serde_json::json!([{"Bearer": []}]),
        };
        assert_eq!(got, want, "security mismatch for {} {}", e.method, e.path);
    }
}

/// No hand-written operation lacks a route.
#[test]
fn no_phantom_operations() {
    assert_eq!(OpenApiDocument::phantom_operations(), Vec::<String>::new());
}

/// operationIds are unique within each document.
#[test]
fn operation_ids_are_unique() {
    for doc in [
        OpenApiDocument::generate_v1(),
        OpenApiDocument::generate_v2(),
    ] {
        let mut seen = std::collections::BTreeSet::new();
        for o in all_ops(&doc) {
            assert!(
                seen.insert(o.operation_id.clone()),
                "duplicate operationId {}",
                o.operation_id
            );
        }
    }
}

/// `x-tantra-max-body-bytes` equals the registry body limit.
#[test]
fn max_body_bytes_matches_registry() {
    let v2 = OpenApiDocument::generate_v2();
    for e in route_registry()
        .iter()
        .filter(|e| !is_root_doc_route(e.path))
    {
        let o = op(&v2, e.method, e.path).expect("operation exists");
        assert_eq!(
            o.max_body_bytes, e.body_limit,
            "body cap {} {}",
            e.method, e.path
        );
    }
}

/// Regenerates `schema/openapi.v2.json`; run with `-- --ignored write_static_artifact`.
#[test]
#[ignore = "writes schema/openapi.v2.json"]
fn write_static_artifact() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/schema/openapi.v2.json");
    std::fs::write(path, OpenApiDocument::v2_json()).expect("write artifact");
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
        "StrategyLoopRequest",
        "StrategyLoopResponse",
        "StrategySweepRequest",
        "StrategySweepResponse",
        "ExpiryHedgeRequest",
        "ExpiryHedgeResponse",
        "OverlaySceneDto",
        "ChatThreadDto",
        "ChatThreadsListDto",
        "ChatThreadMessageDto",
        "ChatThreadDetailDto",
        "CreateChatThreadRequest",
        "PatchChatThreadRequest",
        "DeleteChatThreadResponse",
        "OptionsChainResponse",
        "OptionChainRowDto",
        "OptionLegSnapshotDto",
        "OptionExpiryDto",
        "OptionChainSource",
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
