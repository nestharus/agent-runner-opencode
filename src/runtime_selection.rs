//! Declared roles: resolver, mapper, accessor
//! intrinsic_surface_declarations:
//!   - component: src/runtime_selection.rs
//!     role: intrinsic-surface
//!     Domain: OpenCode runtime selection identity
//!     Owns:
//!       - declared-account versus persisted-settings-record origin
//!       - the resolved account and optional stored model route
//!       - truthful selection-reference evidence labels

use crate::account::{profile_for_account_reference, AccountProfile};
use crate::envelope::{HostContext, ProviderFailure};
use crate::models::ModelAlias;
use crate::settings;

pub struct RuntimeSelection {
    pub requested_reference: String,
    pub origin: RuntimeSelectionOrigin,
    pub account: &'static AccountProfile,
    pub model: Option<&'static ModelAlias>,
}

pub enum RuntimeSelectionOrigin {
    DeclaredAccount {
        account_reference: &'static str,
    },
    PersistedSettingsRecord {
        record_id: String,
        record_version: String,
    },
}

impl RuntimeSelection {
    pub fn origin_label(&self) -> String {
        match &self.origin {
            RuntimeSelectionOrigin::DeclaredAccount { account_reference } => {
                format!("declared account {account_reference}")
            }
            RuntimeSelectionOrigin::PersistedSettingsRecord {
                record_id,
                record_version,
            } => format!("settings record {record_id} at version {record_version}"),
        }
    }
}

pub fn resolve_runtime_selection(
    host: &HostContext,
    reference: &str,
    request_id: &str,
) -> Result<RuntimeSelection, ProviderFailure> {
    if let Some(account) = profile_for_account_reference(reference) {
        return Ok(RuntimeSelection {
            requested_reference: reference.to_string(),
            origin: RuntimeSelectionOrigin::DeclaredAccount {
                account_reference: account.opencode_wrapper,
            },
            account,
            model: None,
        });
    }
    let persisted = settings::resolve_persisted_runtime_record(host, reference, request_id)
        .map_err(|failure| map_missing_record(failure, request_id, reference))?;
    Ok(RuntimeSelection {
        requested_reference: reference.to_string(),
        origin: RuntimeSelectionOrigin::PersistedSettingsRecord {
            record_id: persisted.record_id,
            record_version: persisted.record_version,
        },
        account: persisted.account,
        model: persisted.model,
    })
}

fn map_missing_record(
    failure: ProviderFailure,
    request_id: &str,
    reference: &str,
) -> ProviderFailure {
    if failure.code != "settings_not_found" {
        return failure;
    }
    ProviderFailure::invalid_request(
        request_id,
        "unknown_settings_id",
        format!("unknown OpenCode runtime selection reference: {reference}"),
    )
}
