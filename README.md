# agent-runner-opencode

Standalone OpenCode provider CLI for the `oulipoly.provider/v1` external
provider contract.

OpenCode owns every account-scoped boundary: model launch, sessions, native
session export/import for account rotation, authentication, and quota
attribution. Quota is read from the selected wrapper's native OpenCode auth
file and queried through the ChatGPT usage endpoint with a durably bound
`curl`; Codex configuration and credentials are neither read nor modified.

The provider recognizes five account-pinned wrappers, `opencode1` through
`opencode5`. The selected settings record, wrapper command, session commands,
auth path, quota probe, refresh command, and rotation target must resolve to
the same profile. Numbered wrappers are the canonical persisted and executable
identities, and launch accepts only the exact canonical wrapper name selected
by the settings record. Path-shaped or basename-equivalent commands are not
account aliases. The bare `opencode` name remains an account-one compatibility
reference only at catalog-mediated setup and legacy inputs; it is not an
accepted launch command. Setup plans and legacy-provider migration emit
canonical numbered identities, while unknown or path-shaped OpenCode
references are diagnostic errors and are never inferred from a basename. A
contract field named `settings_id` identifies only an opaque,
persisted settings record; account or wrapper aliases are not accepted as a
second hidden token kind. A record carries its ID, version, account, and either
an exact stored route or the explicit `model.selection=requested` policy.
Policy evidence publishes that record identity and its effective account.
Rotation preserves `source_provider` and `target_provider` as opaque host
provider-instance identities. Provider-local `source_account` and
`target_account` parameters are resolved separately, and the decision receipt
referenced by the durable host plan binds both identity domains. Account aliases
are canonicalized while the binding is constructed, so eligibility,
authorization hashes, receipts, and native export/import all use the same
numbered account identity. Its optional
`settings_id` must be a persisted record for the target account. Stores written
by the prior schema are
compatibility-projected to the current OpenCode-owned account, quota, and model
shape while preserving record IDs and versions; the next mutation writes the
upgraded store schema. A predecessor-produced store above the current 4 MiB or
256-record steady-state limits remains readable and routable during the
transition. Creates and other growth are rejected, while a size-reducing update
or record-reducing delete is committed in predecessor recovery form; each later
process can continue that in-band reduction, and the first mutation that fits
the current envelope atomically writes the current schema. Oversized files that
claim the current schema are not admitted through this compatibility path. An
otherwise valid predecessor model tuple that omitted `model.name` is projected
from its exact `provider_model` and `variant`. A residual predecessor record
that cannot be routed remains listable with a `repair_required` migration
summary instead of rejecting the shared store; selecting that record fails with
its settings diagnostics, while its preserved ID and version allow an in-band
update or delete. Unrelated projected records remain fully usable.
Rotation assessment and materialization share one provider-state lock, so a
decision cannot race native materialization. A denied assessment durably removes
any earlier binding-matched authorization—including parent-directory
synchronization on an already-absent retry—before reporting denial.

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
external DTO or silently weakens launch-plan fields. Every accepted plan also
carries the resolved account wrapper, provider ID, model ID, and effort used by
resume observation; launch does not recover those identities from public model
JSON or generated argv. Agent Runner's `system_prompt_override` and its Claude-
and Codex-specific `tool_restrictions` have no proven faithful OpenCode mapping,
so policy retains their presence and rejects the launch as
`unsupported_system_prompt_override` or `unsupported_tool_restrictions`
instead of silently discarding owner-selected launch policy.

Quota probing likewise converges on a typed `QuotaObservation`. The native
adapter translates authenticated WHAM HTTP responses directly into that type.
Source-aware failures retain whether auth-file parsing or WHAM
transport/HTTP/protocol handling failed;
each source assigns typed authentication-refresh advice before control returns
to quota orchestration. `quota.probe` is observation-only: authentication
failures project actionable advice to call the separately durable
`quota.refresh_auth` operation instead of mutating credentials inside the probe
lifecycle. Refresh availability is decided from the typed probe, and the quota
command projects the observation or failure once into the public result.

The OpenCode auth crossing does not equate a zero-exit `auth list` with a
refresh. It returns a typed observation that distinguishes command success from
an observed before/after change in the selected credential source. The public
`refreshed` flag requires that credential change; post-operation quota
availability is reported independently.

`quota.refresh_auth` also takes durable custody of each request before admitting
the native auth command. The request binding includes the complete parameter
digest, provider/host identity, resolved settings-record ID and version,
account, credential-source path, and native runtime identity. A completed
observation is committed before the response is written, so an exact retry
after response loss replays the same
`refreshed`, availability, timestamp, and detail without invoking OpenCode
again. If provider loss occurs after the native-effect boundary but before that
observation is committed, the request is durably marked for reconciliation;
retries return an actionable conflict instead of guessing whether it is safe to
repeat the credential operation. Account-scoped locks serialize observable auth
refreshes while their provider invocation remains alive.
Request/account lock admission and the native auth child are bounded by the
earlier of `host.deadline_unix_ms` and a 20-second provider ceiling. A timed-out
child is terminated and reaped, the admitted operation becomes
`reconciliation_required`, and the account lane is released for an authorized
follow-up instead of remaining monopolized by the stalled process.
Exact refresh retries inspect this immutable request record before resolving
live settings. Deleting or updating the former settings record therefore cannot
hide either a committed result or an admitted-effect reconciliation handoff;
prepared work continues from its stored canonical account and auth-source path.

## Invocation and lifecycle

The one-shot invocation form is:

```text
agent-runner-opencode <subcommand>
```

Each invocation reads one JSON envelope from stdin. Non-launch commands write
one JSON response. `launch` writes NDJSON events ending in an `exit` event.
Launch children run in a provider-owned process group, while export and quota
helpers remain direct children. One custody boundary is installed immediately
after every manual spawn and owns termination and reaping on every fallible
return until a successful wait discharges it. Drain queues and terminal-capture
tails are bounded. Pipe read failures, malformed native events, and capture
truncation are emitted as explicit evidence markers. Independent stdout and
stderr pipes are sequenced in provider receipt order; the provider makes no
claim about an unknowable pre-receipt cross-pipe emission order.

Before a new-session child is spawned, launch stages its complete stdin and
durably binds the request ID to the accepted route and prompt digest. The route
event must also be handed off before spawn; a failed pre-spawn handoff durably
releases the binding. After spawn, projection is held until either the first
generated provider session ID or a no-session terminal is durable, so an event
write cannot discard the responsible successor. An exact retry returns an
observed session or terminal for reconciliation. A request left merely
prepared by an interrupted provider invocation runs native session discovery,
using the original passthrough environment plus the exact request-bound
declared environment. Every Unix launch starts behind a provider-owned exec
gate: its child process group is durably attached to the prepared request
before the gate can execute the native command. Provider loss or publication
failure closes an unreleased gate without admitting a native effect. Recovery
refuses readmission while a published actor is live; once that actor is
terminal (or a prepared record proves no actor was ever published), recovery
binds a matching session when present and readmits the request only after an
exhaustive same-context list proves no effect.
Non-Unix builds do not admit native launch because they cannot provide this
process-group custody contract.
Reusing the request ID with different launch inputs is rejected as a conflict.
Launch records also retain a settings-independent digest of the original host
app and complete request parameters. Exact retries inspect that immutable
identity and any durable session, terminal, or resume observation before
consulting mutable live settings. Current settings admission occurs only when
there is no prior operation or authoritative recovery proved that it had no
native effect.

For resumed sessions, model switching is allowed per turn. Delivery and
completion are credited only when bounded native export observes the submitted
payload and a completed assistant message for the requested session, provider
model, and variant. A bare native `step_finish` is not completion authority.
Before spawning a resumed turn, launch durably binds the request ID to its
session, route, payload digest, delivery nonce, observation timestamp, and
original export-command context. Post-spawn events remain withheld until a
terminal export result is durable. If event delivery then fails, an exact retry
uses that same context to inspect the bound session and refuses to resubmit a
turn whose user message is already present; readmission requires authoritative
evidence that the prior invocation had no effect. New-session and resumed-turn
records carry distinct operation kinds, so a request ID cannot cross those
lifecycle domains.
The named resume-observation boundary owns this transcript traversal and
returns either evidence-backed completion or explicit uncertainty; launch only
orchestrates its probes alongside process supervision and projects the result
to contract markers. If export proves the resumed user turn was submitted but
cannot prove a completed assistant response, launch emits an unresolved
completion marker and returns a non-clean unknown terminal result so the caller
must reconcile the named provider session before retrying the turn.
If a lingering OpenCode process is terminated after response confirmation,
the exit event retains the real process signal while a separate marker records
the confirmed application response.

`session.capture` accepts several compatibility-era evidence carriers at its
external boundary. It translates every non-empty carrier into one typed
candidate set with provenance and rejects the request if any simultaneously
supplied session identities disagree; priority never erases conflicting launch,
lifecycle, pinned-target, bare, or live-report evidence.

Generic canonical `session.replace` remains unsupported: OpenCode has no
stable canonical-transcript replacement API. Rotation's native full-session
export/import is a separate, representation-bounded capability. It requires a
fresh provider-issued assessment authorization, emits a decision receipt,
durably publishes the content-addressed source artifact, and then persists a
binding-keyed prepared operation before import. A successful
import durably advances that operation with the observed target session before
decision and materialization receipts are finalized. A retry resumes an
imported operation without repeating the effect. If execution stopped in the
irreducibly ambiguous prepared-to-imported window, automatic re-import remains
blocked: the provider first probes the expected target session, or the caller
supplies `recovery_target_session_id`; the exported target must match the
prepared source artifact before finalization. The recovery error retains the
prepared artifact path for a one-time manual import when no effect occurred.
Receipt replay and imported-operation finalization run before validating the
host working directory because neither path uses it. The directory is required
only before a new native import, so removing or renaming it cannot strand a
completed materialization.

## State, evidence, and authority

Settings are stored under `host.config_root/agent-runner-opencode` using an
interprocess lock and atomic file transactions. The same transaction records a
hash-chained mutation history with request/provider identity, predecessor and
result versions, value hashes, and tombstones. It also retains a request-bound
mutation receipt, so an exact retry after response loss returns the committed
create, update, delete, or migration result without repeating the mutation;
reuse of that request identity with a different binding is rejected. This
idempotency guarantee has a declared 24-hour retention window. The store is
bounded to 256 records, 1,024 retained history events, 4,096 live mutation
receipts, and 4 MiB encoded; history keeps a hash-linked contiguous tail and
expired receipts are removed before new admission. Capacity exhaustion rejects
only a new settings mutation as `settings_capacity_exhausted`; existing bounded
records remain readable and usable. The predecessor recovery exception is
non-growing and finite: only the exact schema-less predecessor serialization or
an intermediate schema-zero recovery transaction can exceed the encoded bound,
and each admitted recovery mutation must reduce record count or encoded size.
Settings lock admission is bounded by the earlier of the host deadline and five
seconds. Migration
artifacts are content-addressed, atomically published, confined to
provider-owned roots, and retain hashes for the complete legacy input plus its
provider/model records.
`src/settings_definition.rs` is the sole owner of both the published
`opencode.settings/v1` JSON schema and its executable domain validation;
`schema` projects that definition and `settings` owns record lifecycle.

When `host.data_root` is present, a redacted hash-chained activity ledger is
written under `provider-state/opencode/activity`. It joins requests across
policy, launch, session, quota, settings, migration, and rotation without
recording prompts, tokens, or environment values. Each capability translates
its own domain identities into typed targets: start evidence preserves every
attempted identity and its source, while completion evidence adds canonical,
resolved, or generated settings, account, model, provider, session, and
artifact identities. This prevents generic JSON-path fallback from collapsing
distinct source and target roles. Rotation authorizations, prepared/imported
operation records, materialization receipts, and decision receipts live under
the adjacent `provider-state/opencode/rotation` tree.
Activity recording never waits for its global lock: contention or evidence I/O
failure produces a warning and capability dispatch continues. Each event is at
most 64 KiB and the retained ledger is capped at 4,096 events and 8 MiB. When a
bound is reached, the recorder keeps the newest contiguous hash-chain tail; its
first sequence and predecessor digest expose the retention boundary.

Settings, migration, activity, and rotation all pass every provider-owned
filesystem target through the same lexical and canonical confinement guard
before creating a directory or file. Each subsystem retains its own lifecycle
rules after admission: settings and activity keep their interprocess locks,
and migration and rotation keep their content-addressed atomic publication.
One shared filesystem boundary durably publishes every settings, migration,
activity, launch-state, rotation-artifact, and rotation-state directory link
before a dependent file write or irreversible native import may proceed. Every
retry re-synchronizes the complete directory lineage, including links already
visible after an earlier parent-sync failure. A readable provider file can
satisfy a retry only after that boundary re-syncs its parent, so a prior
post-rename sync failure remains an error until durability completes.

Every native effect also passes through one durable runtime context per
numbered account under `host.data_root/provider-state/opencode/native-runtimes`.
The context privately records the canonical absolute wrapper, its content hash,
and the stable execution environment that can select OpenCode state or alter
wrapper behavior. Launch establishes or validates that binding before spawn;
session export/enumeration, resume observation, rotation export/import, auth
refresh, and quota-source observation after a binding exists reuse it instead
of resolving a fresh ambient command or auth path. Explicitly
transient runner-linkage and contract-test logging variables are forwarded for
the current invocation but do not change the state identity. A different
wrapper, stable environment, or changed wrapper implementation is rejected
before another native effect rather than silently addressing a second state
namespace with the same account/session labels.

Quota probes use a separate durable implementation context under
`host.data_root/provider-state/opencode/quota-observers`. The first probe for an
account resolves `curl` once, records its canonical path, content hash, and
minimal cleared environment, and every later probe reuses that exact observer.
The adapter owns one fixed `chatgpt_wham_curl/v1` request contract, disables
ambient curl configuration, supplies credentials through stdin configuration,
and accepts no environment-selected observer branch. Auth refresh binds both
the native OpenCode runtime identity and this quota-observer identity before it
admits a credential mutation.

`setup.detect` reports provider-wide `installed=true` only after exact-path
`--version` probes succeed for `opencode`, `curl`, and every one of the five
account wrappers, and every account's OpenCode auth file is present. Each probe
uses the earlier of the host deadline and a two-second ceiling. A missing,
non-executable, failing, or stalled dependency leaves the provider non-installed
and produces a tool- or profile-specific warning; regular-file presence alone
is never readiness. Native runtime and quota-observer admission independently
enforce executable-file status before binding an implementation identity and on
every later reuse.

### Native dependency identity upgrades

Wrapper and quota-observer files must not be replaced in place while their
identity is admitted. `setup.sync_plan` accepts `rebind_profiles` and emits the
exact per-profile binding files plus this bounded maintenance procedure:

1. Stop new admission for the selected profile. Give every in-flight provider
   request a deadline of at most 20 seconds and wait one such drain interval.
2. Reconcile every nonterminal launch, rotation, and quota-refresh record. If
   any effect remains ambiguous, abort the rollout and retain the old binding;
   the cutover interval does not begin until obligations are settled.
3. Stage the new wrapper and `curl` implementation without altering the files
   named by the old binding. Back up and remove only
   `native-runtimes/<profile>.json` and
   `quota-observers/<profile>.json` under the provider state root.
4. Restore admission and run one quota probe and one launch under the intended
   `PATH` and stable environment. They durably admit the new identities. If
   either admission fails, restore the two binding backups and the old staged
   dependencies.

After obligations are settled, the declared cutover bound is one
20-second admission interval; rollback is the same bounded two-file maintenance
operation. This drain/reconcile/reset boundary preserves the old identities for
recovery while giving wrapper and observer upgrades an explicit restoration
path.

Rotation assessment and materialization likewise bound acquisition of their
shared state lock. Native export and import performed while that lock is held
use the earlier of the host deadline and a 20-second provider ceiling; a stalled
import is terminated and reaped while its prepared record remains available for
identity-safe reconciliation. Unrelated rotation actors can therefore regain
the shared capability without terminating the provider manually.

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
