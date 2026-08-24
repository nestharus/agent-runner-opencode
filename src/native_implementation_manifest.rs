//! Declared roles: parser, validator, accessor
//! intrinsic_surface_declarations:
//!   - component: src/native_implementation_manifest.rs
//!     role: intrinsic-surface
//!     Domain: reviewed native implementation admission
//!     Owns:
//!       - the exact versioned manifest of the production OpenCode implementation
//!       - target-platform and byte-identity admission before any native effect
//!       - an explicitly non-release contract-fixture boundary

use serde::Deserialize;
use std::collections::BTreeSet;

pub(crate) const MANIFEST_CONTRACT: &str =
    "agent-runner-opencode.native-implementation-manifest/v1";
const MANIFEST_JSON: &str = include_str!("../contract/native-implementation-manifest-v1.json");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    contract: String,
    implementations: Vec<Implementation>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Implementation {
    id: String,
    component: String,
    target_os: String,
    target_arch: String,
    version: String,
    sha256: String,
    byte_length: u64,
    semantic_contract: String,
}

pub(crate) struct ApprovedImplementation {
    pub id: String,
    pub version: String,
    pub semantic_contract: String,
}

pub(crate) fn approved_implementation(
    component: &str,
    sha256: &str,
    byte_length: usize,
) -> Result<Option<ApprovedImplementation>, String> {
    let manifest: Manifest = serde_json::from_str(MANIFEST_JSON)
        .map_err(|error| format!("native implementation manifest is invalid: {error}"))?;
    if manifest.contract != MANIFEST_CONTRACT {
        return Err("native implementation manifest contract identity is invalid".to_string());
    }
    let mut ids = BTreeSet::new();
    let mut byte_identities = BTreeSet::new();
    for implementation in &manifest.implementations {
        if implementation.id.trim().is_empty()
            || implementation.component.trim().is_empty()
            || implementation.target_os.trim().is_empty()
            || implementation.target_arch.trim().is_empty()
            || implementation.version.trim().is_empty()
            || implementation.semantic_contract.trim().is_empty()
            || implementation.byte_length == 0
            || implementation.sha256.len() != 64
            || !implementation
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err("native implementation manifest has an incomplete entry".to_string());
        }
        if !ids.insert(implementation.id.as_str()) {
            return Err("native implementation manifest has a duplicate ID".to_string());
        }
        if !byte_identities.insert((
            implementation.component.as_str(),
            implementation.target_os.as_str(),
            implementation.target_arch.as_str(),
            implementation.sha256.as_str(),
            implementation.byte_length,
        )) {
            return Err("native implementation manifest has a duplicate byte identity".to_string());
        }
    }
    let mut matches = manifest
        .implementations
        .into_iter()
        .filter(|implementation| {
            implementation.component == component
                && implementation.target_os == std::env::consts::OS
                && implementation.target_arch == std::env::consts::ARCH
                && implementation.sha256 == sha256
                && implementation.byte_length == byte_length as u64
        });
    let approved = matches.next();
    if let Some(implementation) = approved {
        return Ok(Some(ApprovedImplementation {
            id: implementation.id,
            version: implementation.version,
            semantic_contract: implementation.semantic_contract,
        }));
    }

    #[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
    {
        let semantic_contract = match component {
            "opencode" => "agent-runner-opencode.opencode-native-state/v1",
            "curl" => "agent-runner-opencode.chatgpt-wham-http/v1",
            _ => return Ok(None),
        };
        Ok(Some(ApprovedImplementation {
            id: format!("contract-test-fixture:{component}:{sha256}"),
            version: "contract-test-fixture".to_string(),
            semantic_contract: semantic_contract.to_string(),
        }))
    }

    #[cfg(not(all(feature = "contract-test-fixtures", debug_assertions)))]
    {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::{approved_implementation, MANIFEST_CONTRACT, MANIFEST_JSON};

    #[test]
    fn production_manifest_is_versioned_and_has_unique_complete_entries() {
        let value: serde_json::Value =
            serde_json::from_str(MANIFEST_JSON).expect("parse native implementation manifest");
        assert_eq!(value["contract"], MANIFEST_CONTRACT);
        let implementations = value["implementations"]
            .as_array()
            .expect("implementation manifest entries");
        assert!(!implementations.is_empty());
        let mut identities = std::collections::BTreeSet::new();
        for implementation in implementations {
            for field in [
                "id",
                "component",
                "target_os",
                "target_arch",
                "version",
                "sha256",
                "semantic_contract",
            ] {
                assert!(implementation[field]
                    .as_str()
                    .is_some_and(|value| !value.trim().is_empty()));
            }
            assert!(implementation["byte_length"]
                .as_u64()
                .is_some_and(|size| size > 0));
            assert!(identities.insert((
                implementation["component"].as_str().unwrap().to_string(),
                implementation["target_os"].as_str().unwrap().to_string(),
                implementation["target_arch"].as_str().unwrap().to_string(),
                implementation["sha256"].as_str().unwrap().to_string(),
            )));
        }

        let _ = approved_implementation("not-a-component", "00", 1)
            .expect("valid manifest should remain readable");
        let approved = approved_implementation(
            "opencode",
            "fd4cfd76ca65a706d0138886dd23094dd07e35460080024b1467baaf32dcee2e",
            184_277_120,
        )
        .expect("valid manifest")
        .expect("production OpenCode identity is approved");
        assert_eq!(approved.id, "opencode-1.18.19-linux-x86_64-fd4cfd76");

        let approved = approved_implementation(
            "opencode",
            "c9485f62576606dbde6404647405df2401fada964b7f669f799dc125dbbeff99",
            184_498_304,
        )
        .expect("valid manifest")
        .expect("current production OpenCode identity is approved");
        assert_eq!(approved.id, "opencode-1.18.21-linux-x86_64-c9485f62");
        assert_eq!(approved.version, "1.18.21");

        let approved = approved_implementation(
            "opencode",
            "168f763fad45b30b8e508bb6fadf152c2888c0235dc4759fcb60a778c16ef768",
            184_584_320,
        )
        .expect("valid manifest")
        .expect("reviewed OpenCode update identity is approved");
        assert_eq!(approved.id, "opencode-1.18.22-linux-x86_64-168f763f");
        assert_eq!(approved.version, "1.18.22");
    }

    #[cfg(not(all(feature = "contract-test-fixtures", debug_assertions)))]
    #[test]
    fn production_build_rejects_an_unknown_executable_identity() {
        assert!(approved_implementation("opencode", "00", 1)
            .expect("valid manifest")
            .is_none());
        assert!(approved_implementation("curl", "00", 1)
            .expect("valid manifest")
            .is_none());
    }
}
