//! `LlmProviderRegistry::from_catalog` routing (provider-config pass B).

use tantra_agent_llm::openai_compat::OpenAiCompatibleProvider;
use tantra_agent_llm::{LlmError, StaticCatalog};
use tantra_runtime_api_rest::chat::LlmProviderRegistry;

use std::sync::Arc;

fn registry() -> LlmProviderRegistry {
    let a = OpenAiCompatibleProvider::new("http://127.0.0.1:9/v1", None, "upstream-a");
    let b = OpenAiCompatibleProvider::new("http://127.0.0.1:9/v1", None, "upstream-b");
    let catalog = StaticCatalog::new()
        .with_model("local/a", "A", Arc::new(a))
        .with_model("gw/b", "B", Arc::new(b));
    LlmProviderRegistry::from_catalog(&catalog).expect("catalog builds")
}

#[test]
fn default_and_aliases_resolve() {
    let r = registry();
    assert_eq!(r.default_model(), "upstream-a");
    assert_eq!(r.resolve(None).expect("default").model_name(), "upstream-a");
    assert_eq!(
        r.resolve(Some("gw/b")).expect("id").model_name(),
        "upstream-b"
    );
    assert_eq!(
        r.resolve(Some("upstream-b")).expect("alias").model_name(),
        "upstream-b"
    );
}

#[test]
fn unknown_model_lists_available() {
    let r = registry();
    match r.resolve(Some("nope")) {
        Err(LlmError::Capability(msg)) => {
            assert!(msg.contains("local/a") && msg.contains("gw/b"));
            assert!(msg.contains("tantra.jsonc"));
            assert!(!msg.contains("TANTRA_"));
        }
        Err(other) => panic!("unexpected error: {other}"),
        Ok(_) => panic!("unknown model must not resolve"),
    }
    assert_eq!(r.models(), vec!["gw/b".to_string(), "local/a".to_string()]);
}
