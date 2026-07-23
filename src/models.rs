//! Declared roles: accessor, mapper

pub const PROVIDER_MODEL: &str = "openai/gpt-5.6-sol";
pub const DEFAULT_MODEL_ALIAS: &str = "gpt-high";

pub struct ModelAlias {
    pub name: &'static str,
    pub effort: &'static str,
}

pub const MODEL_ALIASES: &[ModelAlias] = &[
    ModelAlias {
        name: "gpt-low",
        effort: "low",
    },
    ModelAlias {
        name: "gpt-medium",
        effort: "medium",
    },
    ModelAlias {
        name: "gpt-high",
        effort: "high",
    },
    ModelAlias {
        name: "gpt-xhigh",
        effort: "xhigh",
    },
    ModelAlias {
        name: "gpt-max",
        effort: "max",
    },
];

pub fn alias_names() -> Vec<&'static str> {
    MODEL_ALIASES.iter().map(|model| model.name).collect()
}

pub fn effort_values() -> Vec<&'static str> {
    MODEL_ALIASES.iter().map(|model| model.effort).collect()
}

pub fn default_model_effort() -> &'static str {
    MODEL_ALIASES
        .iter()
        .find(|model| model.name == DEFAULT_MODEL_ALIAS)
        .expect("default model alias must exist in model catalogue")
        .effort
}
