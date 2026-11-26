//! Architectural guard (gap 0028): strategy crate must not depend on concrete
//! brokers or own Tokio runtimes.

use std::fs;
use std::path::Path;

/// Exec crates the strategy crate may link: the port crate and the simulator.
const ALLOWED_EXEC: [&str; 2] = ["tantra-exec-core", "tantra-exec-paper"];

/// Every non-dev dependency table: `[dependencies]`, `[build-dependencies]`, and
/// their `[target.'cfg(..)'.*]` forms (dev-dependencies may use concrete brokers).
fn linked_dependency_tables(manifest: &toml::Table) -> Vec<&toml::Table> {
    let mut tables = Vec::new();
    for key in ["dependencies", "build-dependencies"] {
        if let Some(t) = manifest.get(key).and_then(toml::Value::as_table) {
            tables.push(t);
        }
    }
    if let Some(targets) = manifest.get("target").and_then(toml::Value::as_table) {
        for target in targets.values().filter_map(toml::Value::as_table) {
            for key in ["dependencies", "build-dependencies"] {
                if let Some(t) = target.get(key).and_then(toml::Value::as_table) {
                    tables.push(t);
                }
            }
        }
    }
    tables
}

#[test]
fn no_concrete_exec_dependencies() {
    let text = fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .expect("read Cargo.toml");
    let manifest: toml::Table = text.parse().expect("Cargo.toml parses");
    let tables = linked_dependency_tables(&manifest);
    assert!(!tables.is_empty(), "no [dependencies] table found");
    for table in tables {
        for (name, spec) in table {
            // A renamed dependency names the real crate in `package = "..."`.
            let real = spec
                .get("package")
                .and_then(toml::Value::as_str)
                .unwrap_or(name);
            for crate_name in [name.as_str(), real] {
                if crate_name.starts_with("tantra-exec-") {
                    assert!(
                        ALLOWED_EXEC.contains(&crate_name),
                        "forbidden concrete exec dependency: {crate_name}"
                    );
                }
            }
        }
    }
}

fn walk(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in fs::read_dir(dir).expect("read_dir") {
        let p = entry.expect("entry").path();
        if p.is_dir() {
            walk(&p, out);
        } else if p.extension().is_some_and(|e| e == "rs") {
            out.push(p);
        }
    }
}

#[test]
fn src_has_no_runtime_construction_or_concrete_broker() {
    let mut files = Vec::new();
    walk(
        Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src")),
        &mut files,
    );
    assert!(!files.is_empty());
    for f in files {
        let text = fs::read_to_string(&f).expect("read src file");
        // Every way to own a Tokio runtime, plus the concrete broker crate.
        for banned in [
            "Runtime::new",
            "Builder::new_multi_thread",
            "Builder::new_current_thread",
            "tantra_exec_zerodha",
        ] {
            assert!(!text.contains(banned), "{} contains {banned}", f.display());
        }
    }
}
