# Contract characterization fixtures

This directory owns checked-in native input fixtures used by the contract
clusters:

- `opencode_launch_events.jsonl` for `opencode run --format json` launch characterization.
- `opencode_export.json` for `opencode export <sessionID>` session characterization.
- `chatgpt_wham_usage.json` is the authoritative raw WHAM usage response for
  cluster C. The fake transport reads it directly, and the expected normalized
  windows are derived from the same fixture so protocol-shape changes have one
  maintenance owner.

Foundation tests do not require external-tool fixtures.
