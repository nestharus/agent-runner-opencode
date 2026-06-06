//! Declared roles: orchestration, accessor, validator, formatter, mapper

use std::fs;
use std::path::Path;

const ADAPTER_SOURCES: &[&str] = &[
    "src/opencode.rs",
    "src/codex.rs",
    "src/launch.rs",
    "src/quota.rs",
    "src/session.rs",
];

#[test]
fn f8_adapter_declarations_present() {
    for source in ADAPTER_SOURCES {
        let text = source_text(source);
        assert_adapter_declaration(source, &text);
    }
}

fn source_text(source: &str) -> String {
    let path = source_path(source);
    fs::read_to_string(&path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
}

fn source_path(source: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(source)
}

fn assert_adapter_declaration(source: &str, text: &str) {
    assert_declared_roles_header(source, text);
    assert_adapter_declarations_block(source, text);
    assert_adapter_component(source, text);
    assert_adapter_role(source, text);
}

fn assert_declared_roles_header(source: &str, text: &str) {
    assert!(
        text.contains("//! Declared roles:"),
        "{source} must retain its F8 declared-role header"
    );
}

fn assert_adapter_declarations_block(source: &str, text: &str) {
    assert!(
        text.contains("//! adapter_declarations:"),
        "{source} must retain its F8 adapter_declarations block"
    );
}

fn assert_adapter_component(source: &str, text: &str) {
    assert!(
        text.contains(&adapter_component_line(source)),
        "{source} adapter_declarations block must declare its component"
    );
}

fn adapter_component_line(source: &str) -> String {
    format!("//!   - component: {source}")
}

fn assert_adapter_role(source: &str, text: &str) {
    assert!(
        text.contains("//!     role: adapter"),
        "{source} adapter_declarations block must declare adapter role"
    );
}
