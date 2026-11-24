//! `PreparedServer::into_router` serves the same `/api/v2` surface in process (root design 0006
//! §6.8, the `tantra-ffi` `Client.inproc` transport).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tantra_runtime_api_rest::{ServeConfig, prepare_server};
use tower::util::ServiceExt;

#[tokio::test]
async fn prepared_router_answers_v2_in_process() {
    let root = std::env::temp_dir().join(format!("tantra-inproc-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let mut cfg = ServeConfig::new(&root);
    cfg.runs.journals_dir = root.join("_journals");
    cfg.runs.profiles_dir = root.join("_profiles");
    cfg.runs.portfolios_dir = root.join("_portfolios");
    let (router, state) = prepare_server(&cfg).await.unwrap().into_router();

    let res = router
        .clone()
        .oneshot(Request::get("/api/v2/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["api_version"], "v2");

    let res = router
        .oneshot(Request::get("/api/v1/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    let _ = tokio::task::spawn_blocking(move || state.shutdown_runs()).await;
    let _ = std::fs::remove_dir_all(&root);
}
