//! Declared roles: resolver, mapper, accessor
//! intrinsic_surface_declarations:
//!   - component: src/runtime_selection.rs
//!     role: intrinsic-surface
//!     Domain: OpenCode runtime selection identity
//!     Owns:
//!       - persisted settings-record identity and version
//!       - the resolved account and explicit requested-versus-exact model binding
//!       - truthful settings-record evidence labels

use crate::account::AccountProfile;
use crate::envelope::{HostContext, ProviderFailure};
use crate::models::ModelAlias;
use crate::settings;

pub struct RuntimeSelection {
    pub settings_id: String,
    pub settings_version: String,
    pub account: &'static AccountProfile,
    pub model_binding: RuntimeModelBinding,
}

#[derive(Clone, Copy)]
pub enum RuntimeModelBinding {
    AnyAdvertised,
    Exact(&'static ModelAlias),
}

impl RuntimeSelection {
    pub fn evidence_label(&self) -> String {
        format!(
            "settings record {} at version {}",
            self.settings_id, self.settings_version
        )
    }

    pub fn exact_model(&self) -> Option<&'static ModelAlias> {
        match self.model_binding {
            RuntimeModelBinding::AnyAdvertised => None,
            RuntimeModelBinding::Exact(model) => Some(model),
        }
    }
}

pub fn resolve_runtime_selection(
    host: &HostContext,
    settings_id: &str,
    request_id: &str,
) -> Result<RuntimeSelection, ProviderFailure> {
    let persisted = settings::resolve_persisted_runtime_record(host, settings_id, request_id)
        .map_err(|failure| map_missing_record(failure, request_id, settings_id))?;
    Ok(RuntimeSelection {
        settings_id: persisted.record_id,
        settings_version: persisted.record_version,
        account: persisted.account,
        model_binding: persisted
            .model
            .map(RuntimeModelBinding::Exact)
            .unwrap_or(RuntimeModelBinding::AnyAdvertised),
    })
}

fn map_missing_record(
    failure: ProviderFailure,
    request_id: &str,
    settings_id: &str,
) -> ProviderFailure {
    if failure.code != "settings_not_found" {
        return failure;
    }
    ProviderFailure::invalid_request(
        request_id,
        "unknown_settings_id",
        format!("unknown persisted OpenCode settings record: {settings_id}"),
    )
}
