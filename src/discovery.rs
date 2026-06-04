//! Declared roles: formatter, accessor

use crate::account::{AccountProfile, ACCOUNTS};
use crate::models::{ModelAlias, MODEL_ALIASES, PROVIDER_MODEL};
use serde_json::{json, Value};

pub fn models() -> Value {
    json!({
        "models": model_aliases(),
        "warnings": [],
    })
}

pub fn accounts() -> Value {
    json!({
        "accounts": ACCOUNTS.iter().map(account_json).collect::<Vec<_>>(),
        "warnings": [],
    })
}

fn model_aliases() -> Vec<Value> {
    MODEL_ALIASES.iter().map(model_alias_json).collect()
}

fn model_alias_json(model: &ModelAlias) -> Value {
    json!({
        "name": model.name,
        "provider_model": PROVIDER_MODEL,
        "provider_args": ["-m", PROVIDER_MODEL, "--variant", model.effort],
    })
}

fn account_json(account: &AccountProfile) -> Value {
    json!({
        "id": account.opencode_wrapper,
        "opencode_wrapper": account.opencode_wrapper,
        "opencode_index": account.opencode_index,
        "codex_auth_path": account.codex_auth_path,
        "codex_account_tag": account.codex_account_tag,
        "codex_account_hash": account.codex_account_hash,
        "quota_source": quota_source_json(account),
    })
}

fn quota_source_json(account: &AccountProfile) -> Value {
    json!({
        "kind": "codex_auth",
        "auth_path": account.codex_auth_path,
        "account_tag": account.codex_account_tag,
        "account_hash": account.codex_account_hash,
    })
}
