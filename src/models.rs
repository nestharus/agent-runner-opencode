//! Declared roles: accessor, mapper, predicate

use crate::account::{AccountProfile, ACCOUNTS};

pub const SOL_PROVIDER_MODEL: &str = "openai/gpt-5.6-sol";
pub const LUNA_PROVIDER_MODEL: &str = "openai/gpt-5.6-luna";
pub const DEFAULT_MODEL_ALIAS: &str = "gpt-high";
pub const MODEL_ELIGIBILITY_POLICY: &str = "uniform_all_declared_accounts";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ModelAlias {
    pub name: &'static str,
    pub provider_model: &'static str,
    pub effort: &'static str,
}

pub const MODEL_ALIASES: &[ModelAlias] = &[
    ModelAlias {
        name: "gpt-low",
        provider_model: SOL_PROVIDER_MODEL,
        effort: "low",
    },
    ModelAlias {
        name: "gpt-medium",
        provider_model: SOL_PROVIDER_MODEL,
        effort: "medium",
    },
    ModelAlias {
        name: "gpt-high",
        provider_model: SOL_PROVIDER_MODEL,
        effort: "high",
    },
    ModelAlias {
        name: "gpt-xhigh",
        provider_model: SOL_PROVIDER_MODEL,
        effort: "xhigh",
    },
    ModelAlias {
        name: "gpt-max",
        provider_model: SOL_PROVIDER_MODEL,
        effort: "max",
    },
    ModelAlias {
        name: "gpt-luna-low",
        provider_model: LUNA_PROVIDER_MODEL,
        effort: "low",
    },
    ModelAlias {
        name: "gpt-luna-max",
        provider_model: LUNA_PROVIDER_MODEL,
        effort: "max",
    },
];

pub fn alias_names() -> Vec<&'static str> {
    MODEL_ALIASES.iter().map(|model| model.name).collect()
}

pub fn model_alias(name: &str) -> Option<&'static ModelAlias> {
    MODEL_ALIASES.iter().find(|model| model.name == name)
}

pub fn model_alias_matches(name: &str, provider_model: Option<&str>, effort: Option<&str>) -> bool {
    model_alias(name).is_some_and(|model| model.matches(provider_model, effort))
}

pub fn provider_args_match(model: &ModelAlias, args: &[String]) -> bool {
    args == model.provider_args()
}

pub fn default_model() -> &'static ModelAlias {
    model_alias(DEFAULT_MODEL_ALIAS).expect("default model alias must exist in model catalogue")
}

impl ModelAlias {
    pub fn supports_account(&self, account: &AccountProfile) -> bool {
        MODEL_ALIASES.iter().any(|declared| declared == self)
            && ACCOUNTS
                .iter()
                .any(|declared| declared.opencode_wrapper == account.opencode_wrapper)
    }

    pub fn eligible_account_ids(&self) -> Vec<&'static str> {
        ACCOUNTS
            .iter()
            .filter(|account| self.supports_account(account))
            .map(|account| account.opencode_wrapper)
            .collect()
    }

    pub fn matches(&self, provider_model: Option<&str>, effort: Option<&str>) -> bool {
        provider_model == Some(self.provider_model) && effort == Some(self.effort)
    }

    pub fn provider_args(&self) -> Vec<String> {
        vec![
            "-m".to_string(),
            self.provider_model.to_string(),
            "--variant".to_string(),
            self.effort.to_string(),
        ]
    }

    pub fn host_candidate_args(&self) -> Vec<String> {
        let mut args = vec![
            "run".to_string(),
            "--dangerously-skip-permissions".to_string(),
        ];
        args.extend(self.provider_args());
        args
    }

    pub fn policy_effective_args(&self) -> Vec<String> {
        let mut args = vec![
            "run".to_string(),
            "--format".to_string(),
            "json".to_string(),
            "--dangerously-skip-permissions".to_string(),
        ];
        args.extend(self.provider_args());
        args
    }
}
