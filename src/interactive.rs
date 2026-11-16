//! Provider-owned fresh interactive OpenCode launcher.

use crate::account::profile_for_settings_id;
use crate::settings::{interactive_model_for_wrapper, InteractiveModelSelection};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const INTERACTIVE_SUBCOMMAND: &str = "interactive";
const XDG_STATE_HOME_ENV: &str = "XDG_STATE_HOME";

pub fn is_interactive_invocation(args: &[String]) -> bool {
    args.get(1).map(String::as_str) == Some(INTERACTIVE_SUBCOMMAND)
}

pub fn run(args: &[String]) -> Result<i32, String> {
    let wrapper = args
        .get(2)
        .ok_or_else(|| "interactive launch requires an OpenCode account wrapper".to_string())?;
    let account = profile_for_settings_id(wrapper)
        .ok_or_else(|| format!("unknown OpenCode account wrapper: {wrapper}"))?;
    let passthrough = args.get(3..).unwrap_or_default();
    let mut command = Command::new(account.opencode_wrapper);
    if passthrough.is_empty() {
        let selection = interactive_model_for_wrapper(&config_root()?, account.opencode_wrapper)?;
        let state_root = prepare_fresh_state(account.opencode_wrapper, &selection)?;
        command.env(XDG_STATE_HOME_ENV, state_root);
        command.args(["-m", selection.provider_model.as_str()]);
    } else {
        command.args(passthrough);
    }
    command
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let status = command
        .status()
        .map_err(|err| format!("failed to launch {}: {err}", account.opencode_wrapper))?;
    Ok(status.code().unwrap_or(1))
}

fn prepare_fresh_state(
    wrapper: &str,
    selection: &InteractiveModelSelection,
) -> Result<PathBuf, String> {
    let state_root = fresh_state_root(wrapper)?;
    let opencode_state = state_root.join("opencode");
    fs::create_dir_all(&opencode_state).map_err(|err| {
        format!(
            "failed to create provider-owned OpenCode state {}: {err}",
            opencode_state.display()
        )
    })?;
    write_fresh_model_state(&opencode_state, selection)?;
    Ok(state_root)
}

fn write_fresh_model_state(
    opencode_state: &Path,
    selection: &InteractiveModelSelection,
) -> Result<(), String> {
    let path = opencode_state.join("model.json");
    let temporary = opencode_state.join(format!(".model.json.{}.tmp", std::process::id()));
    let bytes = serde_json::to_vec(&fresh_model_state(selection))
        .map_err(|err| format!("failed to encode fresh OpenCode model state: {err}"))?;
    fs::write(&temporary, bytes).map_err(|err| {
        format!(
            "failed to write provider-owned OpenCode state {}: {err}",
            temporary.display()
        )
    })?;
    fs::rename(&temporary, &path).map_err(|err| {
        format!(
            "failed to install provider-owned OpenCode state {}: {err}",
            path.display()
        )
    })
}

fn fresh_model_state(selection: &InteractiveModelSelection) -> Value {
    let (provider_id, model_id) = selection
        .provider_model
        .split_once('/')
        .expect("validated provider model contains provider/model");
    json!({
        "recent": [{"providerID": provider_id, "modelID": model_id}],
        "favorite": [],
        "variant": {(selection.provider_model.clone()): selection.variant.clone()},
    })
}

fn fresh_state_root(wrapper: &str) -> Result<PathBuf, String> {
    let base = match std::env::var_os(XDG_STATE_HOME_ENV) {
        Some(root) => PathBuf::from(root),
        None => {
            let home = std::env::var_os("HOME")
                .ok_or_else(|| "HOME is unset; cannot locate provider state".to_string())?;
            Path::new(&home).join(".local").join("state")
        }
    };
    Ok(base.join("agent-runner-opencode").join(wrapper))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_model_state_sets_provider_default_without_global_state() {
        let selection = InteractiveModelSelection {
            alias: "gpt-xhigh".to_string(),
            provider_model: "openai/gpt-5.6-sol".to_string(),
            variant: "xhigh".to_string(),
        };
        let state = fresh_model_state(&selection);
        assert_eq!(state["recent"][0]["providerID"], "openai");
        assert_eq!(state["recent"][0]["modelID"], "gpt-5.6-sol");
        assert_eq!(state["variant"]["openai/gpt-5.6-sol"], "xhigh");
    }
}

fn config_root() -> Result<PathBuf, String> {
    if let Some(root) = std::env::var_os("XDG_CONFIG_HOME") {
        return Ok(Path::new(&root).join("oulipoly-agent-runner"));
    }
    let home = std::env::var_os("HOME")
        .ok_or_else(|| "HOME is unset; cannot locate provider settings".to_string())?;
    Ok(Path::new(&home)
        .join(".config")
        .join("oulipoly-agent-runner"))
}
