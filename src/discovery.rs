//! Declared roles: formatter, accessor, mapper

use crate::account::{AccountProfile, ACCOUNTS};
use crate::models::{ModelAlias, MODEL_ALIASES, MODEL_ELIGIBILITY_POLICY};
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
        "provider_model": model.provider_model,
        "provider_args": model.provider_args(),
        "account_eligibility": MODEL_ELIGIBILITY_POLICY,
        "eligible_accounts": model.eligible_account_ids(),
    })
}

fn account_json(account: &AccountProfile) -> Value {
    json!({
        "id": account.opencode_wrapper,
        "opencode_wrapper": account.opencode_wrapper,
        "opencode_index": account.opencode_index,
        "quota_auth_path": account.quota_auth_path(),
        "account_tag": account.account_tag,
        "account_hash": account.account_hash,
        "quota_source": quota_source_json(account),
    })
}

fn quota_source_json(account: &AccountProfile) -> Value {
    json!({
        "kind": account.quota_source_kind(),
        "auth_path": account.quota_auth_path(),
        "probe": account.quota_probe_kind(),
        "refresh_owner": account.opencode_wrapper,
        "account_tag": account.account_tag,
        "account_hash": account.account_hash,
    })
}
