use std::fs;
use std::path::Path;

#[test]
fn f8_adapter_declarations_present() {
    for source in [
        "src/opencode.rs",
        "src/codex.rs",
        "src/launch.rs",
        "src/quota.rs",
        "src/session.rs",
    ] {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(source);
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));

        assert!(
            text.contains("//! Declared roles:"),
            "{source} must retain its F8 declared-role header"
        );
        assert!(
            text.contains("//! adapter_declarations:"),
            "{source} must retain its F8 adapter_declarations block"
        );
        assert!(
            text.contains(&format!("//!   - component: {source}")),
            "{source} adapter_declarations block must declare its component"
        );
        assert!(
            text.contains("//!     role: adapter"),
            "{source} adapter_declarations block must declare adapter role"
        );
    }
}
