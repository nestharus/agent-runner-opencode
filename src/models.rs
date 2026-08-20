//! Declared roles: accessor, mapper, predicate

pub const SOL_PROVIDER_MODEL: &str = "openai/gpt-5.6-sol";
pub const LUNA_PROVIDER_MODEL: &str = "openai/gpt-5.6-luna";
pub const DEFAULT_MODEL_ALIAS: &str = "gpt-high";

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
    model_alias(name).is_some_and(|model| {
        provider_model == Some(model.provider_model) && effort == Some(model.effort)
    })
}

pub fn provider_args_match(model: &ModelAlias, args: &[String]) -> bool {
    matches!(
        args,
        [model_flag, provider_model, variant_flag, effort]
            if model_flag == "-m"
                && provider_model == model.provider_model
                && variant_flag == "--variant"
                && effort == model.effort
    )
}

pub fn default_model() -> &'static ModelAlias {
    model_alias(DEFAULT_MODEL_ALIAS).expect("default model alias must exist in model catalogue")
}

pub fn default_model_effort() -> &'static str {
    default_model().effort
}
