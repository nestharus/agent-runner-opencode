# agent-runner-opencode

Standalone **opencode/codex hybrid** provider CLI for the `oulipoly.provider/v1` external
provider contract.

One CLI, two underlying tools:

- **opencode** owns the model lifecycle — `launch` (`opencode run --pure -m openai/gpt-5.6-sol
  --variant <effort>`), `session` (read_turns/capture/export/replace/locate), `terminal`
  classification, and `policy` application.
- **codex** owns **only** `quota` — ChatGPT plan-window usage via `chatgpt-usage
  ~/.codexN/auth.json`, which opencode cannot report. This is why the provider needs both.

5 account-pinned profiles (the `~/.local/bin/opencodeN` launch wrappers; RFQ key purged,
native OAuth, `--pure`, infinite bash). Account map is shuffled between the tools:
opencode1=codex1, opencode2=codex5, opencode3=codex2, opencode4=codex3, opencode5=codex4.

This provider implements the one-shot invocation convention:

```text
agent-runner-opencode <subcommand>
```

Each subcommand reads one JSON request envelope on stdin. Non-launch commands write one JSON
response envelope on stdout. `launch` writes newline-delimited JSON events and finishes with
an `exit` event.

The installed provider can also be used as the account command for fresh TTY sessions:
`agent-runner-opencode interactive opencodeN`. With no passthrough arguments it reads that
account's provider settings and launches the configured default model. Explicit arguments are
passed through unchanged, keeping resume and model-specific launches independent of the fresh
default. Fresh TTY model/variant state is materialized under the provider's isolated
`XDG_STATE_HOME`; the user's global OpenCode model state is not modified.

The CLI never links the host-side `oulipoly-provider` crate; it implements the versioned JSON
Schema contract in `contract/v1/` directly. `provider_id = "opencode"`, settings schema
`opencode.settings/v1`.
