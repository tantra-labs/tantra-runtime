//! Chat session persistence (docs/gaps/0006): `SqlxSessionStore` over a
//! migrated sqlite file proves real (non-in-memory) persistence, and
//! `AppState::with_database` proves the boot wiring path `prepare_server`
//! follows when `TANTRA_DATABASE_URL` is set.

use chrono::Utc;
use tantra_agent_core::RuntimeMode;
use tantra_agent_core::ports::{
    NewSession, PersistedMessage, SessionFilter, SessionId, SessionScope, SessionStore,
};
use tantra_runtime_api_rest::AppState;
use tantra_runtime_persistence::{Database, SqlxSessionStore, migrate};

/// Connects a file-backed sqlite database and applies the embedded migrations
/// (which create the `agent_chat_session`/`agent_chat_message` tables).
///
/// A pooled `sqlite::memory:` database would be per-connection, so a temp file
/// is what lets every pool connection share one schema and data image.
async fn migrated_sqlite(dir: &tempfile::TempDir) -> Database {
    let path = dir.path().join("sessions.db");
    let url = format!("sqlite:{}", path.display());
    let db = Database::connect(&url).await.unwrap();
    migrate(&db).await.unwrap();
    db
}

#[tokio::test]
async fn sqlx_session_store_persists_across_stores() {
    let dir = tempfile::tempdir().unwrap();
    let db = migrated_sqlite(&dir).await;
    assert!(db.is_sqlite());
    let store = SqlxSessionStore::new(&db);

    let id = SessionId::new();
    let created = store
        .create(NewSession {
            id,
            profile: "test-user".into(),
            mode: RuntimeMode::Live,
            provider: "test-provider".into(),
            model: "test-model".into(),
            title: Some("persistence test".into()),
        })
        .await
        .unwrap();
    assert_eq!(created, id);

    let msg = PersistedMessage {
        seq: 1,
        role: "user".into(),
        content: serde_json::json!({ "text": "hello from a persisted session" }),
        request_hash: Some("deadbeef".into()),
        input_tokens: Some(10),
        output_tokens: Some(2),
        created_at: Utc::now(),
    };
    let appended = store.append(id, std::slice::from_ref(&msg)).await.unwrap();
    assert_eq!(appended, vec![1]);

    // A fresh store over the same database file reads the session and message
    // back — real persistence, not an in-memory registry.
    let reloaded = SqlxSessionStore::new(&db);
    let messages = reloaded.messages(id).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].0, 1);
    assert_eq!(messages[0].1.role, "user");
    assert_eq!(messages[0].1.content, msg.content);

    let listed = reloaded
        .list(SessionFilter {
            profile: Some("test-user".into()),
            mode: Some(RuntimeMode::Live),
            ..SessionFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, id);

    let recent = reloaded
        .most_recent(&SessionScope::new("test-user", RuntimeMode::Live))
        .await
        .unwrap();
    assert_eq!(recent.unwrap().id, id);
}

#[tokio::test]
async fn with_database_wiring_attaches_the_migrated_database() {
    let dir = tempfile::tempdir().unwrap();
    let db = migrated_sqlite(&dir).await;
    let state = AppState::default().with_database(db);
    let attached = state
        .database
        .expect("with_database must attach the database");
    assert!(attached.is_sqlite());
}
