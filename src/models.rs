//! Declared roles: accessor

pub const PROVIDER_MODEL: &str = "openai/gpt-5.5";
pub const DEFAULT_MODEL_ALIAS: &str = "gpt-high";

pub struct ModelAlias {
    pub name: &'static str,
    pub effort: &'static str,
}

pub const MODEL_ALIASES: &[ModelAlias] = &[
    ModelAlias {
        name: "gpt-none",
        effort: "none",
    },
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
