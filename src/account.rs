//! Declared roles: accessor
//! intrinsic_surface_declarations:
//!   - component: src/account.rs
//!     role: intrinsic-surface
//!     Domain: opencode account, credential, and quota-source attribution
//!     Owns:
//!       - static account profile declarations
//!       - wrapper, OpenCode auth path, and account tag pairing
//!       - canonical wrapper-shaped reference recognition and the bare opencode compatibility alias
//!       - selected quota/auth attribution identity and probe route

pub struct AccountProfile {
    pub opencode_wrapper: &'static str,
    pub opencode_index: u8,
    pub opencode_auth_path: &'static str,
    pub account_tag: &'static str,
    pub account_hash: &'static str,
}

pub const ACCOUNTS: [AccountProfile; 5] = [
    AccountProfile {
        opencode_wrapper: "opencode1",
        opencode_index: 1,
        opencode_auth_path: "~/.local/share/opencode/auth.json",
        account_tag: "opencode1",
        account_hash: "b7590111",
    },
    AccountProfile {
        opencode_wrapper: "opencode2",
        opencode_index: 2,
        opencode_auth_path: "~/.opencode2/opencode/auth.json",
        account_tag: "opencode2",
        account_hash: "6dadfdf6",
    },
    AccountProfile {
        opencode_wrapper: "opencode3",
        opencode_index: 3,
        opencode_auth_path: "~/.opencode3/opencode/auth.json",
        account_tag: "opencode3",
        account_hash: "00d3e164",
    },
    AccountProfile {
        opencode_wrapper: "opencode4",
        opencode_index: 4,
        opencode_auth_path: "~/.opencode4/opencode/auth.json",
        account_tag: "opencode4",
        account_hash: "d2b0bb16",
    },
    AccountProfile {
        opencode_wrapper: "opencode5",
        opencode_index: 5,
        opencode_auth_path: "~/.opencode5/opencode/auth.json",
        account_tag: "opencode5",
        account_hash: "7aee8329",
    },
];

pub fn profile_for_settings_id(settings_id: &str) -> Option<&'static AccountProfile> {
    ACCOUNTS
        .iter()
        .find(|account| settings_id_matches_account(settings_id, account))
}

/// Resolve a wrapper-shaped reference from policy argv, persisted settings, or
/// native-operation routing. Paths are recognized by basename. The unnumbered
/// `opencode` name is a compatibility alias for account one; numbered wrapper
/// names remain the canonical persisted identities.
pub fn profile_for_wrapper_reference(reference: &str) -> Option<&'static AccountProfile> {
    let basename = std::path::Path::new(reference)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(reference);
    ACCOUNTS
        .iter()
        .find(|account| account.opencode_wrapper == basename)
        .or_else(|| (basename == "opencode").then_some(&ACCOUNTS[0]))
}

impl AccountProfile {
    pub fn quota_auth_path(&self) -> &'static str {
        self.opencode_auth_path
    }

    pub fn quota_source_kind(&self) -> &'static str {
        "opencode_auth"
    }

    pub fn quota_probe_kind(&self) -> &'static str {
        "native_chatgpt_usage"
    }
}

fn settings_id_matches_account(settings_id: &str, account: &AccountProfile) -> bool {
    account.opencode_wrapper == settings_id || account_one_provider_alias(settings_id, account)
}

fn account_one_provider_alias(settings_id: &str, account: &AccountProfile) -> bool {
    settings_id == "opencode" && account.opencode_index == 1
}
