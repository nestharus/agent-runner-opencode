//! Declared roles: accessor
//! intrinsic_surface_declarations:
//!   - component: src/account.rs
//!     role: intrinsic-surface
//!     Domain: opencode wrapper to codex auth attribution
//!     Owns:
//!       - static account profile declarations
//!       - codex auth path and account tag pairing
//!       - quota/auth attribution identity

pub struct AccountProfile {
    pub opencode_wrapper: &'static str,
    pub opencode_index: u8,
    pub codex_auth_path: &'static str,
    pub codex_account_tag: &'static str,
    pub codex_account_hash: &'static str,
}

pub const ACCOUNTS: [AccountProfile; 5] = [
    AccountProfile {
        opencode_wrapper: "opencode1",
        opencode_index: 1,
        codex_auth_path: "~/.codex/auth.json",
        codex_account_tag: "codex1",
        codex_account_hash: "781db66f",
    },
    AccountProfile {
        opencode_wrapper: "opencode2",
        opencode_index: 2,
        codex_auth_path: "~/.codex5/auth.json",
        codex_account_tag: "codex5",
        codex_account_hash: "27f8ea6e",
    },
    AccountProfile {
        opencode_wrapper: "opencode3",
        opencode_index: 3,
        codex_auth_path: "~/.codex2/auth.json",
        codex_account_tag: "codex2",
        codex_account_hash: "60238f0b",
    },
    AccountProfile {
        opencode_wrapper: "opencode4",
        opencode_index: 4,
        codex_auth_path: "~/.codex3/auth.json",
        codex_account_tag: "codex3",
        codex_account_hash: "9d764739",
    },
    AccountProfile {
        opencode_wrapper: "opencode5",
        opencode_index: 5,
        codex_auth_path: "~/.codex4/auth.json",
        codex_account_tag: "codex4",
        codex_account_hash: "835bbc4d",
    },
];

pub fn profile_for_settings_id(settings_id: &str) -> Option<&'static AccountProfile> {
    ACCOUNTS
        .iter()
        .find(|account| account.opencode_wrapper == settings_id)
}
