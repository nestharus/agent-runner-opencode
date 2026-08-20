# agent-runner-opencode

Standalone OpenCode provider CLI for the `oulipoly.provider/v1` external
provider contract.

OpenCode owns every account-scoped boundary: model launch, sessions, native
session export/import for account rotation, authentication, and quota
attribution. Quota is read from the selected wrapper's native OpenCode auth
file and queried through the ChatGPT usage endpoint with `curl`; Codex
configuration and credentials are neither read nor modified. The optional
`chatgpt-usage` executable path exists only as a test override.

The provider recognizes five account-pinned wrappers, `opencode1` through
`opencode5`. The selected settings record, wrapper command, session commands,
auth path, quota probe, refresh command, and rotation target must resolve to
the same profile. One account-catalog resolver owns wrapper-shaped references
from command paths, settings values, and native routing. The bare `opencode`
name is a compatibility alias for account one; numbered wrappers are canonical
persisted identities. Setup plans and legacy-provider migration both resolve
their inputs through this same catalog and emit canonical numbered identities;
unknown OpenCode-shaped references are diagnostic errors and are never mapped
to account one. A contract field named `settings_id` identifies only an opaque,
persisted settings record; account or wrapper aliases are not accepted as a
second hidden token kind. A record carries its ID, version, account, and either
an exact stored route or the explicit `model.selection=requested` policy.
Policy evidence publishes that record identity and its effective account.
Rotation preserves `source_provider` and `target_provider` as opaque host
provider-instance identities. Provider-local `source_account` and
`target_account` parameters are resolved separately, and the decision receipt
referenced by the durable host plan binds both identity domains. Its optional
`settings_id` must be a persisted record for the target account. Stores written
by the prior schema are
compatibility-projected to the current OpenCode-owned account, quota, and model
shape while preserving record IDs and versions; the next mutation writes the
upgraded store schema. An unrecognizable record fails the entire store with
`settings_store_upgrade_required` instead of remaining listable but unusable.

## Model routes

One catalog in `src/models.rs` owns alias matching and every public or launch
projection. The current routes are:

| Runner alias | OpenCode model | Variant |
| --- | --- | --- |
| `gpt-low` | `openai/gpt-5.6-sol` | `low` |
| `gpt-medium` | `openai/gpt-5.6-sol` | `medium` |
| `gpt-high` | `openai/gpt-5.6-sol` | `high` |
| `gpt-xhigh` | `openai/gpt-5.6-sol` | `xhigh` |
| `gpt-max` | `openai/gpt-5.6-sol` | `max` |
| `gpt-luna-low` | `openai/gpt-5.6-luna` | `low` |
| `gpt-luna-max` | `openai/gpt-5.6-luna` | `max` |

Model eligibility is deliberately uniform across all five declared account
profiles. Discovery publishes that complete account set for every route, and
policy enforces the same relationship. A native account rejecting an advertised
route is therefore a dependency/runtime failure, not an unrepresented
account-specific policy branch.

Policy accepts only the exact `provider_args` advertised by
`discovery.models`, requires the configured wrapper to match the selected
settings profile, and reconstructs the managed launch prefix. Launch emits a
redacted route marker containing the account, alias, provider model, and
variant actually selected.

Policy evaluation has a provider-owned typed boundary. The policy core returns
an accepted or rejected `PolicyDecision` carrying a typed launch plan; launch
consumes that decision directly. Only the `policy.evaluate` command projects
the decision into the public JSON result, so supervision never reparses its own
external DTO or silently weakens launch-plan fields.

Quota probing likewise converges on a typed `QuotaObservation`. The native
adapter translates authenticated WHAM HTTP responses directly into that type,
while the optional `chatgpt-usage` test override parses its stdout only within
the override branch. Source-aware failures retain whether auth-file parsing,
WHAM transport/HTTP/protocol handling, or the explicit test override failed;
the quota command projects the observation or failure once into the public
result.

## Invocation and lifecycle

The one-shot invocation form is:

```text
agent-runner-opencode <subcommand>
```

Each invocation reads one JSON envelope from stdin. Non-launch commands write
one JSON response. `launch` writes NDJSON events ending in an `exit` event.
Child processes run in a provider-owned process group; every return path owns
termination and direct-child reaping. Drain queues and terminal-capture tails
are bounded. Pipe read failures, malformed native events, and capture
truncation are emitted as explicit evidence markers. Independent stdout and
stderr pipes are sequenced in provider receipt order; the provider makes no
claim about an unknowable pre-receipt cross-pipe emission order.

For resumed sessions, model switching is allowed per turn. Delivery and
completion are credited only when bounded native export observes the submitted
payload and a completed assistant message for the requested session, provider
model, and variant. A bare native `step_finish` is not completion authority.
The named resume-observation boundary owns this transcript traversal and
returns either evidence-backed completion or explicit uncertainty; launch only
orchestrates its probes alongside process supervision and projects the result
to contract markers.
If a lingering OpenCode process is terminated after response confirmation,
the exit event retains the real process signal while a separate marker records
the confirmed application response.

Generic canonical `session.replace` remains unsupported: OpenCode has no
stable canonical-transcript replacement API. Rotation's native full-session
export/import is a separate, representation-bounded capability. It requires a
fresh provider-issued assessment authorization, emits a decision receipt,
preserves an observed post-import session ID as recoverable state, and uses a
durable receipt so retries do not repeat the import.

## State, evidence, and authority

Settings are stored under `host.config_root/agent-runner-opencode` using an
interprocess lock and atomic file transactions. The same transaction records a
hash-chained mutation history with request/provider identity, predecessor and
result versions, value hashes, and tombstones. Migration artifacts are
content-addressed, atomically published, confined to provider-owned roots, and
retain hashes for the complete legacy input plus its provider/model records.
`src/settings_definition.rs` is the sole owner of both the published
`opencode.settings/v1` JSON schema and its executable domain validation;
`schema` projects that definition and `settings` owns record lifecycle.

When `host.data_root` is present, a redacted hash-chained activity ledger is
written under `provider-state/opencode/activity`. It joins requests across
policy, launch, session, quota, settings, migration, and rotation without
recording prompts, tokens, or environment values. Rotation authorizations,
idempotency records, and decision receipts live under the adjacent
`provider-state/opencode/rotation` tree.

Settings, migration, activity, and rotation all pass every provider-owned
filesystem target through the same lexical and canonical confinement guard
before creating a directory or file. Each subsystem retains its own lifecycle
rules after admission: settings and activity keep their interprocess locks,
and migration and rotation keep their content-addressed atomic publication.

Activity evidence is operational and explicitly best-effort. A directory,
lock, write, or chain-validation failure is emitted as a stderr warning but
does not deny or change the capability result. The recorder never appends past
a malformed chain; operators must repair or archive that ledger to restore
continuous evidence.

The v1 host envelope supplies a request ID and optional provider instance ID,
but no authenticated human/service principal or delegation. The provider
records that absence explicitly. Agent Runner remains responsible for
authenticating a principal, authorizing delegation, and binding those
identities to the request and provider instance before invocation; the
provider must not invent that authority.

## Contract provenance

The CLI implements the versioned JSON schemas in `contract/v1` directly and
does not link the host-side `oulipoly-provider` crate. The directory is an exact
commit-pinned snapshot of Agent Runner; see `contract/v1/UPSTREAM.md`. The old
`.s9b-step6a-contract.md` is retained only as historical design evidence.
