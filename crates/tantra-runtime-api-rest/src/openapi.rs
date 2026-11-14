//! OpenAPI 3.1 schema generation for the Tantra REST API.
//!
//! Generates a comprehensive OpenAPI 3.1 document describing all REST endpoints,
//! request/response schemas, authentication schemes, and error responses.
//! Uses `schemars` for JSON Schema generation from Rust types.
//!
//! Public so the route-vs-spec conformance test (`tests/openapi_conformance.rs`)
//! can read the generated document; the structs below mirror the OpenAPI 3.1
//! wire format, so field-level docs add little beyond their names.
#![allow(missing_docs)]

use axum::response::IntoResponse;
use schemars::schema::*;
use schemars::schema_for;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tantra_core::{InstrumentId, OrderSide, OrderStatus, OrderType, Price, Quantity, TimeInForce};
use tantra_risk::{RiskLimits, RiskRefusal};
use tantra_runtime_api::profile::{
    LayoutDocument, LayoutsResponse, Module, ModulesResponse, Profile, ProfileRevision, Source,
};
use tantra_runtime_api::wire::{
    BarAggregation, ErrorCode, ErrorDetail, Instrument, InstrumentKind, PortfolioAccount,
    PortfolioHolding, PortfolioPosition, Position, PriceType, QuoteTick, ResponseEnvelope, Trade,
    TradingState,
};

/// OpenAPI 3.1 info block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenApiInfo {
    pub title: String,
    pub version: String,
    pub description: String,
}

/// OpenAPI 3.1 server object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenApiServer {
    pub url: String,
    pub description: String,
}

/// OpenAPI 3.1 security scheme.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SecurityScheme {
    Http {
        scheme: String,
        bearer_format: Option<String>,
    },
    ApiKey {
        name: String,
        in_: String,
    },
}

/// OpenAPI 3.1 security requirement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityRequirement {
    #[serde(flatten)]
    pub scheme: BTreeMap<String, Vec<String>>,
}

/// Complete OpenAPI 3.1 document for the Tantra REST API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenApiDocument {
    pub openapi: String,
    pub info: OpenApiInfo,
    pub servers: Vec<OpenApiServer>,
    pub paths: BTreeMap<String, PathItem>,
    pub components: Components,
    pub tags: Vec<Tag>,
}

/// A single path item in the OpenAPI document.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PathItem {
    pub get: Option<Operation>,
    pub post: Option<Operation>,
    pub put: Option<Operation>,
    pub patch: Option<Operation>,
    pub delete: Option<Operation>,
}

/// An OpenAPI operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Operation {
    pub tags: Vec<String>,
    pub summary: String,
    pub operation_id: String,
    pub parameters: Vec<Parameter>,
    pub request_body: Option<RequestBody>,
    pub responses: BTreeMap<String, Response>,
    pub security: Vec<SecurityRequirement>,
}

/// An OpenAPI parameter.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Parameter {
    pub name: String,
    pub r#in: String,
    pub required: bool,
    pub schema: Schema,
}

/// An OpenAPI request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestBody {
    pub required: bool,
    pub content: BTreeMap<String, MediaType>,
}

/// An OpenAPI media type.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaType {
    pub schema: Schema,
}

/// An OpenAPI response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    pub description: String,
    pub content: BTreeMap<String, MediaType>,
}

/// An OpenAPI tag.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    pub name: String,
    pub description: String,
}

/// Builds an OpenAPI operation with a JSON request/response body.
fn operation(
    tag: &str,
    summary: &str,
    operation_id: &str,
    parameters: Vec<Parameter>,
    request_body: Option<bool>,
    response_description: &str,
) -> Operation {
    Operation {
        tags: vec![tag.to_string()],
        summary: summary.to_string(),
        operation_id: operation_id.to_string(),
        parameters,
        request_body: request_body.map(|required| RequestBody {
            required,
            content: BTreeMap::from([(
                "application/json".to_string(),
                MediaType {
                    schema: schema_for!(serde_json::Value).schema.into(),
                },
            )]),
        }),
        responses: BTreeMap::from([(
            "200".to_string(),
            Response {
                description: response_description.to_string(),
                content: BTreeMap::from([(
                    "application/json".to_string(),
                    MediaType {
                        schema: schema_for!(serde_json::Value).schema.into(),
                    },
                )]),
            },
        )]),
        security: vec![SecurityRequirement {
            scheme: BTreeMap::from([("Bearer".to_string(), vec!["read".to_string()])]),
        }],
    }
}

fn query_param(name: &str) -> Parameter {
    Parameter {
        name: name.to_string(),
        r#in: "query".to_string(),
        required: false,
        schema: schema_for!(String).schema.into(),
    }
}

fn path_param(name: &str) -> Parameter {
    Parameter {
        name: name.to_string(),
        r#in: "path".to_string(),
        required: true,
        schema: schema_for!(String).schema.into(),
    }
}

/// OpenAPI components (schemas, security schemes, etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Components {
    pub schemas: BTreeMap<String, Schema>,
    pub security_schemes: BTreeMap<String, SecurityScheme>,
}

impl OpenApiDocument {
    /// Generate the complete OpenAPI 3.1 document for the Tantra REST API.
    pub fn generate() -> Self {
        let mut paths = BTreeMap::new();

        // Market data endpoints
        paths.insert(
            "/instruments".to_string(),
            PathItem {
                get: Some(Operation {
                    tags: vec!["market".to_string()],
                    summary: "List all instruments".to_string(),
                    operation_id: "getInstruments".to_string(),
                    parameters: vec![],
                    request_body: None,
                    responses: BTreeMap::from([(
                        "200".to_string(),
                        Response {
                            description: "List of instruments".to_string(),
                            content: BTreeMap::from([(
                                "application/json".to_string(),
                                MediaType {
                                    schema: schema_for!(Vec<InstrumentId>).schema.into(),
                                },
                            )]),
                        },
                    )]),
                    security: vec![SecurityRequirement {
                        scheme: BTreeMap::from([("Bearer".to_string(), vec!["read".to_string()])]),
                    }],
                }),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );

        paths.insert(
            "/quotes".to_string(),
            PathItem {
                get: Some(Operation {
                    tags: vec!["market".to_string()],
                    summary: "Get latest quotes".to_string(),
                    operation_id: "getQuotes".to_string(),
                    parameters: vec![],
                    request_body: None,
                    responses: BTreeMap::from([(
                        "200".to_string(),
                        Response {
                            description: "Latest quotes".to_string(),
                            content: BTreeMap::from([(
                                "application/json".to_string(),
                                MediaType {
                                    schema: schema_for!(serde_json::Value).schema.into(),
                                },
                            )]),
                        },
                    )]),
                    security: vec![SecurityRequirement {
                        scheme: BTreeMap::from([("Bearer".to_string(), vec!["read".to_string()])]),
                    }],
                }),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );

        paths.insert(
            "/orders".to_string(),
            PathItem {
                get: Some(Operation {
                    tags: vec!["trading".to_string()],
                    summary: "List orders".to_string(),
                    operation_id: "getOrders".to_string(),
                    parameters: vec![],
                    request_body: None,
                    responses: BTreeMap::from([(
                        "200".to_string(),
                        Response {
                            description: "List of orders".to_string(),
                            content: BTreeMap::from([(
                                "application/json".to_string(),
                                MediaType {
                                    schema: schema_for!(serde_json::Value).schema.into(),
                                },
                            )]),
                        },
                    )]),
                    security: vec![SecurityRequirement {
                        scheme: BTreeMap::from([("Bearer".to_string(), vec!["trade".to_string()])]),
                    }],
                }),
                post: Some(Operation {
                    tags: vec!["trading".to_string()],
                    summary: "Place a new order".to_string(),
                    operation_id: "postOrders".to_string(),
                    parameters: vec![],
                    request_body: Some(RequestBody {
                        required: true,
                        content: BTreeMap::from([(
                            "application/json".to_string(),
                            MediaType {
                                schema: schema_for!(serde_json::Value).schema.into(),
                            },
                        )]),
                    }),
                    responses: BTreeMap::from([
                        (
                            "200".to_string(),
                            Response {
                                description: "Order placed".to_string(),
                                content: BTreeMap::from([(
                                    "application/json".to_string(),
                                    MediaType {
                                        schema: schema_for!(serde_json::Value).schema.into(),
                                    },
                                )]),
                            },
                        ),
                        (
                            "422".to_string(),
                            Response {
                                description: "Risk refusal or validation error".to_string(),
                                content: BTreeMap::from([(
                                    "application/json".to_string(),
                                    MediaType {
                                        schema: schema_for!(serde_json::Value).schema.into(),
                                    },
                                )]),
                            },
                        ),
                    ]),
                    security: vec![SecurityRequirement {
                        scheme: BTreeMap::from([("Bearer".to_string(), vec!["trade".to_string()])]),
                    }],
                }),
                put: None,
                patch: None,
                delete: None,
            },
        );

        paths.insert(
            "/orders/{id}".to_string(),
            PathItem {
                get: None,
                post: None,
                put: None,
                patch: None,
                delete: Some(Operation {
                    tags: vec!["trading".to_string()],
                    summary: "Cancel an order".to_string(),
                    operation_id: "deleteOrder".to_string(),
                    parameters: vec![Parameter {
                        name: "id".to_string(),
                        r#in: "path".to_string(),
                        required: true,
                        schema: schema_for!(String).schema.into(),
                    }],
                    request_body: None,
                    responses: BTreeMap::from([(
                        "200".to_string(),
                        Response {
                            description: "Order cancelled".to_string(),
                            content: BTreeMap::from([(
                                "application/json".to_string(),
                                MediaType {
                                    schema: schema_for!(serde_json::Value).schema.into(),
                                },
                            )]),
                        },
                    )]),
                    security: vec![SecurityRequirement {
                        scheme: BTreeMap::from([("Bearer".to_string(), vec!["trade".to_string()])]),
                    }],
                }),
            },
        );

        paths.insert(
            "/portfolio".to_string(),
            PathItem {
                get: Some(Operation {
                    tags: vec!["portfolio".to_string()],
                    summary: "Get portfolio".to_string(),
                    operation_id: "getPortfolio".to_string(),
                    parameters: vec![],
                    request_body: None,
                    responses: BTreeMap::from([(
                        "200".to_string(),
                        Response {
                            description: "Portfolio state".to_string(),
                            content: BTreeMap::from([(
                                "application/json".to_string(),
                                MediaType {
                                    schema: schema_for!(serde_json::Value).schema.into(),
                                },
                            )]),
                        },
                    )]),
                    security: vec![SecurityRequirement {
                        scheme: BTreeMap::from([("Bearer".to_string(), vec!["read".to_string()])]),
                    }],
                }),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );

        paths.insert(
            "/schema".to_string(),
            PathItem {
                get: Some(Operation {
                    tags: vec!["meta".to_string()],
                    summary: "Get OpenAPI schema".to_string(),
                    operation_id: "getSchema".to_string(),
                    parameters: vec![],
                    request_body: None,
                    responses: BTreeMap::from([(
                        "200".to_string(),
                        Response {
                            description: "OpenAPI 3.1 schema".to_string(),
                            content: BTreeMap::from([(
                                "application/json".to_string(),
                                MediaType {
                                    schema: schema_for!(serde_json::Value).schema.into(),
                                },
                            )]),
                        },
                    )]),
                    security: vec![],
                }),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );

        // Trade-performance journal endpoints (/trade-journal/*), backed by
        // tantra-ledger-performance. Journal accounts are distinct from
        // /portfolio/accounts (live OMS/trading accounts).
        paths.insert(
            "/trade-journal/accounts".to_string(),
            PathItem {
                get: Some(operation(
                    "trade-journal",
                    "List journal accounts",
                    "listTradeJournalAccounts",
                    vec![],
                    None,
                    "List of journal accounts",
                )),
                post: Some(operation(
                    "trade-journal",
                    "Create a journal account",
                    "createTradeJournalAccount",
                    vec![],
                    Some(true),
                    "Created journal account",
                )),
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/trade-journal/overview".to_string(),
            PathItem {
                get: Some(operation(
                    "trade-journal",
                    "Performance metrics and edge score",
                    "getTradeJournalOverview",
                    vec![
                        query_param("from"),
                        query_param("to"),
                        query_param("compare"),
                    ],
                    None,
                    "Overview metrics and edge score",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/trade-journal/trades".to_string(),
            PathItem {
                get: Some(operation(
                    "trade-journal",
                    "List closed trades with annotations",
                    "listTradeJournalTrades",
                    vec![
                        query_param("from"),
                        query_param("to"),
                        query_param("limit"),
                        query_param("cursor"),
                    ],
                    None,
                    "Cursor-paginated closed trades",
                )),
                post: None,
                put: None,
                delete: None,
                patch: Some(operation(
                    "trade-journal",
                    "Set a trade's annotation",
                    "patchTradeJournalTrade",
                    vec![path_param("id")],
                    Some(true),
                    "Updated trade annotation",
                )),
            },
        );
        paths.insert(
            "/trade-journal/calendar".to_string(),
            PathItem {
                get: Some(operation(
                    "trade-journal",
                    "Daily P&L heatmap for a month",
                    "getTradeJournalCalendar",
                    vec![query_param("month")],
                    None,
                    "Monthly P&L calendar",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/trade-journal/notes".to_string(),
            PathItem {
                get: Some(operation(
                    "trade-journal",
                    "List day-keyed journal notes",
                    "listTradeJournalNotes",
                    vec![query_param("account"), query_param("limit")],
                    None,
                    "Journal notes",
                )),
                post: Some(operation(
                    "trade-journal",
                    "Create or replace a journal note",
                    "createTradeJournalNote",
                    vec![],
                    Some(true),
                    "Created journal note",
                )),
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/trade-journal/compare".to_string(),
            PathItem {
                get: Some(operation(
                    "trade-journal",
                    "Compare two performances",
                    "compareTradeJournalAccounts",
                    vec![
                        query_param("accountA"),
                        query_param("accountB"),
                        query_param("account"),
                        query_param("mode"),
                    ],
                    None,
                    "Two-sided metrics comparison",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );

        // Market reference data endpoints (/market-data/*), backed by
        // tantra-data's calendar and symbol-profile ingestion.
        paths.insert(
            "/market-data/symbol-profile/{id}".to_string(),
            PathItem {
                get: Some(operation(
                    "market-reference",
                    "Company symbol profile / fundamentals",
                    "getSymbolProfile",
                    vec![path_param("id")],
                    None,
                    "Symbol profile",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/market-data/calendar".to_string(),
            PathItem {
                get: Some(operation(
                    "market-reference",
                    "Economic / earnings / dividends / splits / IPO calendar",
                    "getMarketCalendar",
                    vec![
                        query_param("from"),
                        query_param("to"),
                        query_param("category"),
                    ],
                    None,
                    "Calendar events",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/market-data/block-deals".to_string(),
            PathItem {
                get: Some(operation(
                    "market-reference",
                    "Block / bulk deal disclosures",
                    "getBlockDeals",
                    vec![query_param("instrument"), query_param("date")],
                    None,
                    "Block deals",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );

        // Meta endpoints (capabilities, health)
        paths.insert(
            "/capabilities".to_string(),
            PathItem {
                get: Some(operation(
                    "meta",
                    "Platform capabilities and endpoint manifest",
                    "getCapabilities",
                    vec![],
                    None,
                    "Capability manifest",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/health".to_string(),
            PathItem {
                get: Some(operation(
                    "meta",
                    "Service health and runtime diagnostics",
                    "getHealth",
                    vec![],
                    None,
                    "Health status",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );

        // Instrument, bar and depth reference reads
        paths.insert(
            "/instruments/{id}".to_string(),
            PathItem {
                get: Some(operation(
                    "market",
                    "Fetch a single instrument reference",
                    "getInstrument",
                    vec![path_param("id")],
                    None,
                    "Instrument reference",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/bars/{id}".to_string(),
            PathItem {
                get: Some(operation(
                    "market",
                    "Historical bars for an instrument",
                    "getBars",
                    vec![
                        path_param("id"),
                        query_param("from"),
                        query_param("to"),
                        query_param("timeframe"),
                    ],
                    None,
                    "Historical bars",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/depth/{id}".to_string(),
            PathItem {
                get: Some(operation(
                    "market",
                    "Order book depth for an instrument",
                    "getDepth",
                    vec![path_param("id")],
                    None,
                    "Order book depth",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/bars".to_string(),
            PathItem {
                get: None,
                post: Some(operation(
                    "market",
                    "Ingest tick/bar data",
                    "postBars",
                    vec![],
                    Some(true),
                    "Ingest acknowledged",
                )),
                put: None,
                patch: None,
                delete: None,
            },
        );

        // Operator trading state
        paths.insert(
            "/trading-state".to_string(),
            PathItem {
                get: Some(operation(
                    "trading",
                    "Read the operator trading state",
                    "getTradingState",
                    vec![],
                    None,
                    "Current trading state",
                )),
                post: None,
                put: Some(operation(
                    "trading",
                    "Transition the operator trading state",
                    "putTradingState",
                    vec![],
                    Some(true),
                    "Updated trading state",
                )),
                patch: None,
                delete: None,
            },
        );

        // User profile endpoints
        paths.insert(
            "/profile".to_string(),
            PathItem {
                get: Some(operation(
                    "profile",
                    "Get the user profile",
                    "getProfile",
                    vec![],
                    None,
                    "User profile",
                )),
                post: None,
                put: Some(operation(
                    "profile",
                    "Replace the user profile",
                    "putProfile",
                    vec![],
                    Some(true),
                    "Updated profile revision",
                )),
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/profile/layouts".to_string(),
            PathItem {
                get: Some(operation(
                    "profile",
                    "List named workbench layouts",
                    "getProfileLayouts",
                    vec![],
                    None,
                    "Named layouts",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/profile/layouts/{name}".to_string(),
            PathItem {
                get: None,
                post: None,
                put: Some(operation(
                    "profile",
                    "Create or replace a named layout",
                    "putProfileLayout",
                    vec![path_param("name")],
                    Some(true),
                    "Layout stored",
                )),
                patch: None,
                delete: Some(operation(
                    "profile",
                    "Delete a named layout",
                    "deleteProfileLayout",
                    vec![path_param("name")],
                    None,
                    "Layout deleted",
                )),
            },
        );
        paths.insert(
            "/profile/modules".to_string(),
            PathItem {
                get: Some(operation(
                    "profile",
                    "List profile modules",
                    "getProfileModules",
                    vec![],
                    None,
                    "Profile modules",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/profile/modules/{id}".to_string(),
            PathItem {
                get: None,
                post: None,
                put: Some(operation(
                    "profile",
                    "Create or replace a profile module",
                    "putProfileModule",
                    vec![path_param("id")],
                    Some(true),
                    "Module stored",
                )),
                patch: None,
                delete: Some(operation(
                    "profile",
                    "Delete a profile module",
                    "deleteProfileModule",
                    vec![path_param("id")],
                    None,
                    "Module deleted",
                )),
            },
        );

        // Portfolio read endpoints
        paths.insert(
            "/portfolio/accounts".to_string(),
            PathItem {
                get: Some(operation(
                    "portfolio",
                    "List portfolio accounts",
                    "getPortfolioAccounts",
                    vec![],
                    None,
                    "Portfolio accounts",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/portfolio/positions".to_string(),
            PathItem {
                get: Some(operation(
                    "portfolio",
                    "Current open positions",
                    "getPortfolioPositions",
                    vec![],
                    None,
                    "Open positions",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/portfolio/holdings".to_string(),
            PathItem {
                get: Some(operation(
                    "portfolio",
                    "Current holdings",
                    "getPortfolioHoldings",
                    vec![],
                    None,
                    "Holdings",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/portfolio/trades".to_string(),
            PathItem {
                get: Some(operation(
                    "portfolio",
                    "Portfolio trade history",
                    "getPortfolioTrades",
                    vec![],
                    None,
                    "Trade history",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/portfolio/chart".to_string(),
            PathItem {
                get: Some(operation(
                    "portfolio",
                    "Portfolio equity curve",
                    "getEquityCurve",
                    vec![],
                    None,
                    "Equity curve",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/portfolio/concentration".to_string(),
            PathItem {
                get: Some(operation(
                    "portfolio",
                    "Portfolio concentration risk",
                    "getConcentrationRisk",
                    vec![],
                    None,
                    "Concentration risk",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/portfolio/wac".to_string(),
            PathItem {
                get: Some(operation(
                    "portfolio",
                    "Weighted average cost per instrument",
                    "getWeightedAverageCost",
                    vec![],
                    None,
                    "Weighted average cost",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );

        // Legacy report aliases
        paths.insert(
            "/ledger".to_string(),
            PathItem {
                get: Some(operation(
                    "reports",
                    "General ledger (legacy alias of /reports/ledger)",
                    "getLedger",
                    vec![],
                    None,
                    "Ledger entries",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/tax/report".to_string(),
            PathItem {
                get: Some(operation(
                    "reports",
                    "Tax report (legacy alias of /reports/tax)",
                    "getTaxReport",
                    vec![],
                    None,
                    "Tax report",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/fees".to_string(),
            PathItem {
                get: Some(operation(
                    "reports",
                    "Fees report (legacy alias of /reports/fees)",
                    "getFeesReport",
                    vec![],
                    None,
                    "Fees report",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/positions".to_string(),
            PathItem {
                get: Some(operation(
                    "trading",
                    "Current open positions",
                    "getPositions",
                    vec![],
                    None,
                    "Open positions",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/positions/audit".to_string(),
            PathItem {
                get: Some(operation(
                    "reports",
                    "Position audit trail",
                    "getPositionAudit",
                    vec![],
                    None,
                    "Position audit trail",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/trades".to_string(),
            PathItem {
                get: Some(operation(
                    "portfolio",
                    "Trade history",
                    "getTrades",
                    vec![],
                    None,
                    "Trade history",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );

        // Report endpoints
        paths.insert(
            "/reports/ledger".to_string(),
            PathItem {
                get: Some(operation(
                    "reports",
                    "General ledger report",
                    "getReportLedger",
                    vec![],
                    None,
                    "Ledger entries",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/reports/tax".to_string(),
            PathItem {
                get: Some(operation(
                    "reports",
                    "Tax report",
                    "getReportTax",
                    vec![],
                    None,
                    "Tax report",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/reports/snapshots".to_string(),
            PathItem {
                get: Some(operation(
                    "reports",
                    "Portfolio snapshots",
                    "getReportSnapshots",
                    vec![],
                    None,
                    "Portfolio snapshots",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/reports/audit".to_string(),
            PathItem {
                get: Some(operation(
                    "reports",
                    "Order and execution audit trail",
                    "getReportAudit",
                    vec![],
                    None,
                    "Audit trail",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/reports/concentration".to_string(),
            PathItem {
                get: Some(operation(
                    "reports",
                    "Portfolio concentration report",
                    "getReportConcentration",
                    vec![],
                    None,
                    "Concentration risk",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/reports/wac".to_string(),
            PathItem {
                get: Some(operation(
                    "reports",
                    "Weighted average cost report",
                    "getReportWac",
                    vec![],
                    None,
                    "Weighted average cost",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/reports/fees".to_string(),
            PathItem {
                get: Some(operation(
                    "reports",
                    "Fees report",
                    "getReportFees",
                    vec![],
                    None,
                    "Fees report",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/reports/equity-curve".to_string(),
            PathItem {
                get: Some(operation(
                    "reports",
                    "Equity curve report",
                    "getReportEquityCurve",
                    vec![],
                    None,
                    "Equity curve",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/reports/fills/{order_id}".to_string(),
            PathItem {
                get: Some(operation(
                    "reports",
                    "Fills report for one order",
                    "getReportFills",
                    vec![path_param("order_id")],
                    None,
                    "Fills for the order",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );

        // Strategy authoring endpoints
        paths.insert(
            "/strategies".to_string(),
            PathItem {
                get: Some(operation(
                    "strategies",
                    "List compiled strategies",
                    "getStrategies",
                    vec![],
                    None,
                    "Strategy catalog",
                )),
                post: Some(operation(
                    "strategies",
                    "Compile a strategy manifest",
                    "postStrategies",
                    vec![],
                    Some(true),
                    "Compiled strategy",
                )),
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/strategies/verify".to_string(),
            PathItem {
                get: None,
                post: Some(operation(
                    "strategies",
                    "Verify a strategy manifest",
                    "postVerifyStrategy",
                    vec![],
                    Some(true),
                    "Verification result",
                )),
                put: None,
                patch: None,
                delete: None,
            },
        );

        // Backtest runs
        paths.insert(
            "/backtests".to_string(),
            PathItem {
                get: Some(operation(
                    "runs",
                    "List backtest runs",
                    "getBacktests",
                    vec![],
                    None,
                    "Backtest runs",
                )),
                post: Some(operation(
                    "runs",
                    "Start a backtest run",
                    "postBacktests",
                    vec![],
                    Some(true),
                    "Backtest run started",
                )),
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/backtests/{id}".to_string(),
            PathItem {
                get: Some(operation(
                    "runs",
                    "Backtest run detail",
                    "getBacktestById",
                    vec![path_param("id")],
                    None,
                    "Backtest run",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/backtests/{id}/journal".to_string(),
            PathItem {
                get: Some(operation(
                    "runs",
                    "Backtest run journal",
                    "getBacktestJournal",
                    vec![path_param("id")],
                    None,
                    "Run journal",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/backtests/{id}/events".to_string(),
            PathItem {
                get: Some(operation(
                    "runs",
                    "Backtest run events",
                    "getBacktestEvents",
                    vec![path_param("id")],
                    None,
                    "Run events",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );

        // Sweep runs
        paths.insert(
            "/sweeps".to_string(),
            PathItem {
                get: Some(operation(
                    "runs",
                    "List sweep runs",
                    "getSweeps",
                    vec![],
                    None,
                    "Sweep runs",
                )),
                post: Some(operation(
                    "runs",
                    "Start a parameter sweep run",
                    "postSweeps",
                    vec![],
                    Some(true),
                    "Sweep run started",
                )),
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/sweeps/{id}".to_string(),
            PathItem {
                get: Some(operation(
                    "runs",
                    "Sweep run detail",
                    "getSweepById",
                    vec![path_param("id")],
                    None,
                    "Sweep run",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/sweeps/{id}/journal".to_string(),
            PathItem {
                get: Some(operation(
                    "runs",
                    "Sweep run journal",
                    "getSweepJournal",
                    vec![path_param("id")],
                    None,
                    "Run journal",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );

        // Order fills and close-all
        paths.insert(
            "/orders/{id}/fills".to_string(),
            PathItem {
                get: Some(operation(
                    "trading",
                    "Fills for one order",
                    "getOrderFills",
                    vec![path_param("id")],
                    None,
                    "Order fills",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/positions/close".to_string(),
            PathItem {
                get: None,
                post: Some(operation(
                    "trading",
                    "Close all open positions",
                    "postClosePositions",
                    vec![],
                    Some(true),
                    "Close request accepted",
                )),
                put: None,
                patch: None,
                delete: None,
            },
        );

        // Order approval workflow
        paths.insert(
            "/approval/queue".to_string(),
            PathItem {
                get: Some(operation(
                    "approval",
                    "Pending order approval queue",
                    "getApprovalQueue",
                    vec![],
                    None,
                    "Approval queue",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/approval/approve".to_string(),
            PathItem {
                get: None,
                post: Some(operation(
                    "approval",
                    "Approve a pending order",
                    "postApproveOrder",
                    vec![],
                    Some(true),
                    "Order approved",
                )),
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/approval/reject".to_string(),
            PathItem {
                get: None,
                post: Some(operation(
                    "approval",
                    "Reject a pending order",
                    "postRejectOrder",
                    vec![],
                    Some(true),
                    "Order rejected",
                )),
                put: None,
                patch: None,
                delete: None,
            },
        );

        // Screener and run journals
        paths.insert(
            "/screener/scan".to_string(),
            PathItem {
                get: Some(operation(
                    "screener",
                    "Run an AST-filtered screener scan",
                    "getScreenerScan",
                    vec![
                        query_param("symbols"),
                        query_param("min_price"),
                        query_param("max_price"),
                    ],
                    None,
                    "Screener result rows",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/journals/{id}".to_string(),
            PathItem {
                get: Some(operation(
                    "runs",
                    "Run journal by id",
                    "getJournalById",
                    vec![path_param("id")],
                    None,
                    "Run journal",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );

        // WebSocket upgrade endpoints
        paths.insert(
            "/ws".to_string(),
            PathItem {
                get: Some(operation(
                    "ws",
                    "WebSocket live stream",
                    "getWs",
                    vec![],
                    None,
                    "WebSocket upgrade",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/ws/market".to_string(),
            PathItem {
                get: Some(operation(
                    "ws",
                    "WebSocket market data stream",
                    "getWsMarket",
                    vec![],
                    None,
                    "WebSocket upgrade",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );
        paths.insert(
            "/ws/chat".to_string(),
            PathItem {
                get: Some(operation(
                    "ws",
                    "WebSocket chat stream",
                    "getWsChat",
                    vec![],
                    None,
                    "WebSocket upgrade",
                )),
                post: None,
                put: None,
                patch: None,
                delete: None,
            },
        );

        let mut schemas = BTreeMap::new();
        schemas.insert(
            "InstrumentId".to_string(),
            schema_for!(InstrumentId).schema.into(),
        );
        schemas.insert(
            "OrderSide".to_string(),
            schema_for!(OrderSide).schema.into(),
        );
        schemas.insert(
            "OrderStatus".to_string(),
            schema_for!(OrderStatus).schema.into(),
        );
        schemas.insert(
            "OrderType".to_string(),
            schema_for!(OrderType).schema.into(),
        );
        schemas.insert("Price".to_string(), schema_for!(Price).schema.into());
        schemas.insert("Quantity".to_string(), schema_for!(Quantity).schema.into());
        schemas.insert(
            "TimeInForce".to_string(),
            schema_for!(TimeInForce).schema.into(),
        );
        schemas.insert(
            "RiskLimits".to_string(),
            schema_for!(RiskLimits).schema.into(),
        );
        schemas.insert(
            "RiskRefusal".to_string(),
            schema_for!(RiskRefusal).schema.into(),
        );
        schemas.insert(
            "ResponseEnvelope".to_string(),
            schema_for!(ResponseEnvelope<serde_json::Value>)
                .schema
                .into(),
        );
        schemas.insert(
            "ErrorCode".to_string(),
            schema_for!(ErrorCode).schema.into(),
        );
        schemas.insert(
            "ErrorDetail".to_string(),
            schema_for!(ErrorDetail).schema.into(),
        );
        schemas.insert(
            "QuoteTick".to_string(),
            schema_for!(QuoteTick).schema.into(),
        );
        schemas.insert("Trade".to_string(), schema_for!(Trade).schema.into());
        schemas.insert("Position".to_string(), schema_for!(Position).schema.into());
        schemas.insert(
            "PortfolioAccount".to_string(),
            schema_for!(PortfolioAccount).schema.into(),
        );
        schemas.insert(
            "PortfolioPosition".to_string(),
            schema_for!(PortfolioPosition).schema.into(),
        );
        schemas.insert(
            "PortfolioHolding".to_string(),
            schema_for!(PortfolioHolding).schema.into(),
        );
        schemas.insert(
            "Instrument".to_string(),
            schema_for!(Instrument).schema.into(),
        );
        schemas.insert(
            "InstrumentKind".to_string(),
            schema_for!(InstrumentKind).schema.into(),
        );
        schemas.insert(
            "BarAggregation".to_string(),
            schema_for!(BarAggregation).schema.into(),
        );
        schemas.insert(
            "PriceType".to_string(),
            schema_for!(PriceType).schema.into(),
        );
        schemas.insert(
            "TradingState".to_string(),
            schema_for!(TradingState).schema.into(),
        );
        schemas.insert("Profile".to_string(), schema_for!(Profile).schema.into());
        schemas.insert("Module".to_string(), schema_for!(Module).schema.into());
        schemas.insert("Source".to_string(), schema_for!(Source).schema.into());
        schemas.insert(
            "LayoutDocument".to_string(),
            schema_for!(LayoutDocument).schema.into(),
        );
        schemas.insert(
            "LayoutsResponse".to_string(),
            schema_for!(LayoutsResponse).schema.into(),
        );
        schemas.insert(
            "ModulesResponse".to_string(),
            schema_for!(ModulesResponse).schema.into(),
        );
        schemas.insert(
            "ProfileRevision".to_string(),
            schema_for!(ProfileRevision).schema.into(),
        );

        let mut security_schemes = BTreeMap::new();
        security_schemes.insert(
            "Bearer".to_string(),
            SecurityScheme::Http {
                scheme: "bearer".to_string(),
                bearer_format: Some("JWT".to_string()),
            },
        );

        Self {
            openapi: "3.1.0".to_string(),
            info: OpenApiInfo {
                title: "Tantra REST API".to_string(),
                version: tantra_runtime_api::API_VERSION.to_string(),
                description:
                    "Tantra trading platform REST API with JWT authentication and OpenAPI 3.1"
                        .to_string(),
            },
            servers: vec![OpenApiServer {
                url: "http://localhost:8080".to_string(),
                description: "Tantra API server".to_string(),
            }],
            paths,
            components: Components {
                schemas,
                security_schemes,
            },
            tags: vec![
                Tag {
                    name: "market".to_string(),
                    description: "Market data endpoints (instruments, quotes, bars, depth)"
                        .to_string(),
                },
                Tag {
                    name: "trading".to_string(),
                    description: "Trading endpoints (orders, positions)".to_string(),
                },
                Tag {
                    name: "portfolio".to_string(),
                    description: "Portfolio endpoints (holdings, accounts, trades)".to_string(),
                },
                Tag {
                    name: "meta".to_string(),
                    description: "Meta endpoints (schema, health, capabilities)".to_string(),
                },
                Tag {
                    name: "trade-journal".to_string(),
                    description:
                        "Trade-performance journal (annotations, metrics, calendar, comparison)"
                            .to_string(),
                },
                Tag {
                    name: "market-reference".to_string(),
                    description: "Market reference data (symbol profile, economic calendar)"
                        .to_string(),
                },
                Tag {
                    name: "reports".to_string(),
                    description:
                        "Ledger, tax, fees, audit, concentration, WAC and equity-curve reports"
                            .to_string(),
                },
                Tag {
                    name: "profile".to_string(),
                    description: "User profile (layouts, modules, watchlists)".to_string(),
                },
                Tag {
                    name: "runs".to_string(),
                    description: "Backtest and parameter sweep runs, journals".to_string(),
                },
                Tag {
                    name: "strategies".to_string(),
                    description: "Strategy authoring (catalog, compile, verify)".to_string(),
                },
                Tag {
                    name: "screener".to_string(),
                    description: "AST-filtered stock screening".to_string(),
                },
                Tag {
                    name: "ws".to_string(),
                    description: "WebSocket streaming endpoints".to_string(),
                },
                Tag {
                    name: "approval".to_string(),
                    description: "Order approval workflow (queue, approve, reject)".to_string(),
                },
            ],
        }
    }

    /// Serialize the v2 spec to pretty JSON (the checked-in artifact format).
    pub fn v2_json() -> String {
        serde_json::to_string_pretty(&Self::generate_v2()).unwrap_or_default()
    }

    /// Serialize the generated OpenAPI 3.1 document as a JSON `Value`,
    /// suitable for writing to a file (`tantra schema --openapi`).
    pub fn openapi_json() -> serde_json::Value {
        serde_json::to_value(Self::generate()).unwrap_or(serde_json::Value::Null)
    }

    /// Generate the OpenAPI 3.1 document for v1 (legacy & supported).
    pub fn generate_v1() -> Self {
        let mut doc = Self::generate();
        doc.info.title = "Tantra REST API v1".to_string();
        doc.info.version = "1.0.0".to_string();
        doc.info.description =
            "Tantra trading platform REST API v1 (Supported for legacy software)".to_string();
        doc.servers = vec![
            OpenApiServer {
                url: "/api/v1".to_string(),
                description: "Tantra REST API v1 (Supported for legacy software)".to_string(),
            },
            OpenApiServer {
                url: "/".to_string(),
                description: "Legacy un-prefixed root (Deprecated)".to_string(),
            },
        ];
        doc
    }

    /// Generate the OpenAPI 3.1 document for v2 (latest & recommended).
    pub fn generate_v2() -> Self {
        let mut doc = Self::generate();
        doc.info.title = "Tantra REST API v2".to_string();
        doc.info.version = "2.0.0".to_string();
        doc.info.description =
            "Tantra trading platform REST API v2 (Latest & Recommended)".to_string();
        doc.servers = vec![OpenApiServer {
            url: "/api/v2".to_string(),
            description: "Tantra REST API v2 (Latest & Recommended)".to_string(),
        }];
        doc
    }
}

/// Handler for `GET /schema/openapi/v1.json`.
pub async fn get_openapi_v1() -> axum::response::Response {
    let doc = OpenApiDocument::generate_v1();
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        serde_json::to_string_pretty(&doc).unwrap_or_default(),
    )
        .into_response()
}

/// Handler for `GET /schema/openapi/v2.json`.
pub async fn get_openapi_v2() -> axum::response::Response {
    let doc = OpenApiDocument::generate_v2();
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        serde_json::to_string_pretty(&doc).unwrap_or_default(),
    )
        .into_response()
}

/// Handler for `GET /schema/openapi`.
pub async fn get_openapi_index() -> axum::response::Response {
    let index = serde_json::json!({
        "openapi_versions": [
            {
                "version": "v1",
                "status": "supported_legacy",
                "url": "/schema/openapi/v1.json",
                "description": "Tantra REST API v1 (Supported for legacy software)"
            },
            {
                "version": "v2",
                "status": "latest_recommended",
                "url": "/schema/openapi/v2.json",
                "description": "Tantra REST API v2 (Latest & Recommended)"
            }
        ],
        "latest": "v2",
        "documentation_url": "/docs"
    });
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        serde_json::to_string_pretty(&index).unwrap_or_default(),
    )
        .into_response()
}

/// Handler for `GET /docs` and `GET /swagger-ui`.
pub async fn get_swagger_ui() -> axum::response::Html<&'static str> {
    axum::response::Html(SWAGGER_UI_HTML)
}

/// Swagger UI HTML page with multi-version selector.
pub const SWAGGER_UI_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Tantra REST API Documentation</title>
  <link rel="stylesheet" type="text/css" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css" />
  <style>
    body { margin: 0; background: #fafafa; font-family: sans-serif; }
    .topbar { display: block; }
  </style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-standalone-preset.js"></script>
  <script>
    window.onload = function() {
      window.ui = SwaggerUIBundle({
        urls: [
          { url: "/schema/openapi/v2.json", name: "v2 (Latest & Recommended)" },
          { url: "/schema/openapi/v1.json", name: "v1 (Supported / Legacy)" }
        ],
        "urls.primaryName": "v2 (Latest & Recommended)",
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIStandalonePreset
        ],
        layout: "StandaloneLayout"
      });
    };
  </script>
</body>
</html>"#;
