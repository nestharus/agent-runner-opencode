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
accepted launch command. Setup plans emit canonical numbered profile
identities, while legacy-provider migration preserves each recognized provider
table key as an exact settings-record ID. Unknown or path-shaped OpenCode
references are diagnostic errors and are never inferred from a basename. A
contract field named `settings_id` identifies only an opaque,
persisted settings record; account or wrapper aliases are not accepted as a
second hidden token kind. The installed-base transition makes the former token
an exact key instead of teaching runtime selection two meanings:
`settings.migrate` preserves every recognized legacy provider table key as the
ID of its current persisted record and reports that ID in the corresponding
activation action. Thus the existing `opencode`, `opencode2`, ... provider names
continue only after migration has materialized records with those exact keys.
New installations may instead configure model-provider names from the opaque
IDs returned by `settings.create`. A record carries its ID, version, account,
and either an exact stored route or the explicit
`model.selection=requested` policy.
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
transition up to a declared 16 MiB and 4,096-record predecessor envelope.
Reads stop at that byte bound; a larger predecessor store or one with more
records fails explicitly as `settings_store_capacity_unsupported` and must be
reduced with the predecessor binary before this provider is installed. Creates
and other growth are rejected, while a size-reducing update or record-reducing
delete is committed in predecessor recovery form; each later process can
continue that in-band reduction, and the first mutation that fits the current
envelope atomically writes the current schema. Files that claim the current
schema above 4 MiB are not admitted through this compatibility path. An
otherwise valid predecessor model tuple that omitted `model.name` is projected
from its exact `provider_model` and `variant`. A residual predecessor record
that cannot be routed remains listable with a `repair_required` migration
summary instead of rejecting the shared store; selecting that record fails with
its settings diagnostics, while its preserved ID and version allow an in-band
update or delete. Unrelated projected records remain fully usable.
`setup.detect` runs this same bounded, parsed-schema transition preflight
against the exact `host.config_root` store. It also requires a valid exact
record for every caller ID being activated. `params.settings_id` declares one
opaque caller ID, `params.settings_ids` declares the complete caller population,
and their absence selects the installed Agent Runner compatibility population
`opencode`, `opencode2`, ... `opencode5`. An absent or empty store is therefore
`activation_required`, never ready. Provider-wide `installed=true` requires
both store compatibility and caller activation to pass; install plans expose
the exact required and missing IDs as a blocking step, and sync plans emit an
error diagnostic until migration or explicit record configuration completes.
The declared caller population is bounded by the same 4,096-record predecessor
transition envelope as the settings store, independently of the five native
account profiles; multiple caller records may select one account.
This prevents cutover from removing every settings-selected route before its
required reducer has run or declaring an installation ready while established
provider-name callers still lack exact records.
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
JSON or generated argv. Native `--session`/`-s`, `--continue`/`-c`, and `--fork`
controls are reserved to typed `params.session` translation and rejected in the
user-level option suffix; the same text remains ordinary message content after
an explicit `--` boundary. Agent Runner's `system_prompt_override` and its Claude-
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
availability is reported independently. If the credential source changed but
the command then exits nonzero or exceeds its output bound, the provider
durably preserves that partial effect as `reconciliation_required` instead of
publishing `refreshed: false`. Post-spawn observation failures and an
unobservable credential state use the same fail-closed handoff; failures proven
to occur before child effect capability may settle as no refresh.

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
repeat the credential operation. The complete before/effect/after/commit
interval holds a stable lock beside the canonical credential path, plus the
current credential file's advisory lock. Provider invocations with different
data roots, account aliases, or symlinked path spellings therefore serialize
when they can observe or mutate the same credential source; hard-link aliases
converge on the file lock. Per-request durable custody remains scoped to the
declared data root.
Request/effect lock admission and the native auth child are bounded by the
earlier of `host.deadline_unix_ms` and a 20-second provider ceiling. A timed-out
child is terminated and reaped, the admitted operation becomes
`reconciliation_required`, and the credential-effect lane is released for an authorized
follow-up instead of remaining monopolized by the stalled process.
That authorized follow-up reuses the original request and adds
`params.context.reconciliation` with the `accept_current_credentials`
disposition and the lowercase SHA-256 of the current bound credential file.
The provider
verifies that exact source under the account lock, records the resolution on
the original operation, and commits a terminal result without invoking native
auth again. A stale source digest fails closed and leaves the obligation open.
Exact refresh retries inspect this immutable request record before resolving
live settings. Deleting or updating the former settings record therefore cannot
hide either a committed result or an admitted-effect reconciliation handoff;
prepared work continues from its stored canonical account and auth-source path.
Quota refresh custody reserves 64 records for active obligations independently
of a fixed 4,096-slot recent-replay ring. Cyclic slot replacement retires the
oldest available completion without parsing replay payloads; admission reads at
most the 64 active records, and completion probes at most 64 compact ring slots.
Shared replay pins protect exact callers between capacity admission and
request-lock acquisition.
Prepared, effect-admitted, and reconciliation-required records never age out or
lose their active reserve. Each record is capped at 256 KiB. The selected auth file is read
through a 1 MiB bound, access tokens and account IDs have explicit field bounds,
and WHAM `curl` capture is limited to 512 KiB stdout, 64 KiB stderr, and 20
seconds.

## Invocation and lifecycle

The one-shot invocation form is:

```text
agent-runner-opencode <subcommand>
```

Each invocation reads one JSON envelope from stdin. Input is capped at 4 MiB
before JSON parsing; the process reads at most one byte beyond that limit to
classify an oversize request. Request/provider IDs, host labels and paths, and
the host environment have field, entry-count, and 256 KiB aggregate environment
bounds. Non-launch commands write
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
before the gate can execute the native command. That attachment includes the
group leader's platform process-start incarnation, so a later process group
that reuses the same numeric ID cannot impersonate the admitted actor. Provider
loss or publication failure closes an unreleased gate without admitting a
native effect. Recovery refuses readmission while the published group and
leader incarnation are live; once that actor is terminal (or a prepared record
proves no actor was ever published), recovery
binds a matching session only when its user turn carries the provider-authored
delivery identity embedded in that request's actual child payload, and readmits
the request only after a
same-context list bounded to 257 native rows proves no effect. Recovery admits
at most 256 sessions, examines at most eight plausible candidates, captures at
most 2 MiB of list output, and shares one five-second/host-deadline budget across
listing and candidate exports.
Non-Unix builds do not admit native launch because they cannot provide this
process-group custody contract.
Reusing the request ID with different launch inputs is rejected as a conflict.
Launch records also retain a settings-independent digest of the original host
app and complete request parameters. Exact retries inspect that immutable
identity and any durable session, terminal, or resume observation before
consulting mutable live settings. Current settings admission occurs only when
there is no prior operation or authoritative recovery proved that it had no
native effect.
Launch custody reserves 64 records for active obligations independently of a
fixed 4,096-slot recent-replay ring; every record is no larger than 256 KiB.
Cyclic slot replacement retires the oldest available completion without parsing
replay payloads. Steady-state admission reads one fixed-shape compact active
index and classifies at most one active payload per request; completion probes at
most 64 compact ring slots. Prepared, submission-observed, and unresolved
records never age out because they may still own an effect. New work fails at
the active cap until those live obligations are reconciled, but routine
completed history cannot consume its admission reserve or make admission work
proportional to active payload volume or replay history. Shared replay pins
prevent cyclic eviction while an exact caller crosses from capacity admission
to its request lock.

For resumed sessions, model switching is allowed per turn. During the normal
launch, matching bounded `opencode run --format json` `step_start` and successful
`step_finish(reason=stop)` events establish submission and completion without
re-exporting and reparsing the growing transcript. A bare terminal process
status is not completion authority.
Before spawning a resumed turn, launch durably binds the request ID to its
session, route, payload digest, provider-authored delivery nonce, observation
timestamp, and original recovery-command context. The same nonce is embedded
in the actual child payload and is required for transcript recovery, so
identical sibling prompts cannot become request-local submission or completion
evidence. Legacy durable records without that identity remain unresolved.
Post-spawn run events remain withheld until
the corresponding observation is durable. Recovery of a prepared, legacy
terminal, submission-observed, or unresolved record may perform one bounded
750 ms export in that original context.
If it cannot prove safe readmission, the record becomes durable `unresolved`;
exact retries re-run that same bounded observer without resubmitting the turn.
A still-live prior process group blocks reconciliation and readmission.
A later completion moves the original record into terminal replay custody; an
authoritative no-effect observation can safely retire an `unresolved` record
for exact readmission. Submission evidence without completion remains owned and
is observed again on a later exact retry. New-session and resumed-turn records
carry distinct operation kinds, so a request ID cannot cross those lifecycle
domains. If run evidence proves submission but cannot prove a completed
assistant response, launch emits an unresolved
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
export/import is a separate, representation-bounded capability. Native export
stdout and the serialized source artifact are each capped at 16 MiB; native
import stdout and per-command diagnostics are capped at 64 KiB. Oversize
results fail explicitly before import while preserving bounded capture and
allocation. It requires a
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

`session.enumerate` materializes one bounded, request-bound private pagination
snapshot on the first page instead of relisting the complete native population
for every cursor. Exact retries of one initial request can converge on its
snapshot, while distinct requests over identical rows receive independent
cursor owners. A snapshot advances one page at a time: before a page is exposed,
the manifest durably records its exact request claim and the sole next cursor
offset. An exact page retry can replay that claim until its successor is
admitted, while a different request using the consumed or older cursor is
rejected instead of publishing a successor that another terminal handoff can
retire. Page size and admitted population are each capped at 256, list
capture at 2 MiB, each row at 64 KiB, and each snapshot at 4 MiB. At most 32
abandoned snapshots are retained for 15 minutes; continuation cursors read only
the requested rows. A terminal snapshot is retired only after the complete
response is successfully written and flushed; a failed write or flush preserves
the cursor for an exact retry. Before the terminal page is exposed, its manifest
durably claims that handoff for the continuation request; a different terminal
consumer, older cursor, or initial retry cannot reuse the snapshot while that
claim is live. Deferred cleanup carries the snapshot's unique durable instance
identity and terminal claim, reacquires the snapshot lock, and retires the path
only when the current manifest still matches both. A delayed cleanup from an
exact terminal retry is therefore a no-op if the deterministic path has since
been recreated for a new pagination instance.
A cleanup failure falls back to the existing 15-minute expiry.
An above-bound native population fails explicitly.

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
no admitted predecessor/recovery store may exceed 16 MiB or 4,096 records, and
each admitted recovery mutation must reduce record count or encoded size. The
fully serialized candidate—including upgraded values, history, and mutation
receipt—is checked against that transition envelope before atomic publication,
so a successful recovery mutation always leaves a store the next process can
read.
Settings lock admission is bounded by the earlier of the host deadline and five
seconds. Migration artifacts are content-addressed, atomically published,
confined to one of the two exact provider-owned roots, and retain hashes for the
complete legacy input plus its provider/model records. Each summary is capped
at 4 MiB; a fixed five-second/host-deadline capacity lock enforces at most 256
summaries with a 30-day retention window.
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
most 64 KiB. A small atomic head validates only the committed tail before each
append; two alternating append-only segments provide constant-space rotation
at 4,096 events or 8 MiB without rereading and rewriting the full history on
every invocation. The head's first sequence and predecessor digest expose the
retention boundary. Activity-target deduplication uses a hash set, so a settings
list does not perform quadratic scans as identities accumulate.

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
account wrappers, every account's OpenCode auth file is present, and every
declared caller `settings_id` resolves to a valid exact persisted record. Each probe
uses the earlier of the host deadline and a two-second ceiling. A missing,
non-executable, failing, or stalled dependency leaves the provider non-installed
and produces a tool- or profile-specific warning; regular-file presence alone
is never readiness. Native runtime and quota-observer admission independently
enforce executable-file status before binding an implementation identity and on
every later reuse.

### Native dependency identity upgrades

Wrapper and quota-observer files must not be replaced in place while their
identity is admitted. This provider owns the separately versioned
`opencode.native-identity-rebind/v1` maintenance protocol; its exact JSON Schema
is available through `schema`. `setup.sync_plan` accepts that protocol under
`params.native_identity_rebind` and emits operations carrying the same protocol,
schema ID, component-scoped operation ID, actor responsibilities, prior identity
evidence, and a typed completion-observation request. Evidence separately names
the component's semantic implementation `component_identity_sha256` and the
serialized persistence revision `state_record_sha256`; the full pair is bound to
the operation, while semantic identity change decides a commit. Each target names
exactly one `native_runtime` or `quota_observer` identity. A host that needs a coordinated
two-component rollout composes two explicit targets; neither independent entity
is implicitly replaced with the other. The authoritative
`oulipoly.provider/v1` setup schema remains an unchanged Agent Runner snapshot;
its intentionally open setup objects carry this explicitly named provider
extension rather than silently redefining the shared contract.

Wrapper and quota-observer identity state reads and writes are capped at 1 MiB, executable
identity hashing is capped at 64 MiB, and their per-account lock admission is
bounded by the earlier host deadline or five seconds. A lock timeout fails the
request with a named identity-lock result rather than waiting indefinitely.

1. The host stops ordinary admission for capabilities that consume the selected
   profile/component identity. It gives every affected in-flight provider request
   a deadline of at most 20 seconds and waits one such drain interval.
2. The operator reconciles every nonterminal obligation that consumes that
   component. If any effect remains ambiguous, abort the rollout and retain the
   old identity; the cutover interval does not begin until obligations are settled.
3. The host submits the plan's typed `seal` request while admission remains
   blocked. The provider rejects sealing if either host assertion is false or
   the selected provider identity record changed during drain; a successful seal
   binds its exact pre-cutover semantic identity, state-record digest, and
   component into the operation ID.
4. The operator stages only the selected replacement implementation without
   altering the implementation named by its old identity. Back up and remove
   only `native-runtimes/<profile>.json` for a `native_runtime` target or only
   `quota-observers/<profile>.json` for a `quota_observer` target.
5. Ordinary admission remains blocked. The host opens one operation-bound
   validation window for exactly one launch for a `native_runtime` target or one
   quota probe for a `quota_observer` target. That capability durably admits the
   selected identity without reopening unrelated traffic. If validation fails,
   the operator restores that component's backup and old staged dependency, then
   runs the same validation capability against the restored identity.
6. While ordinary admission is still blocked, the host sends the plan-bound
   `observe` request and attests that the selected validation capability completed.
   The provider validates the operation ID and both named current evidence layers. A valid
   commit or rollback returns `awaiting_host_release`, an observation-bound
   release request, and not a terminal success.
7. The host reopens ordinary admission and sends that exact `release` request.
   The provider checks the observation identity and unchanged current evidence pair,
   then returns `completed` or `rolled_back`. A false release assertion or a
   changed binding returns `rejected`.

Private state paths are included only as implementation evidence, not as the
protocol's meaning. A component commit reaches release only when its semantic
implementation identity changes and the validated state record carries it, while
rollback reaches release only when both prior semantic identity and persistence
revision are restored. The host exclusively owns the distinction between blocked ordinary
traffic and the component's cutover-validation exception, and the typed release
acknowledgment owns the transition back to ordinary admission.

After obligations are settled, the declared cutover bound is one
20-second admission interval; rollback is the same bounded two-file maintenance
operation. This drain/reconcile/reset boundary preserves the old identities for
recovery while giving wrapper and observer upgrades an explicit restoration
path.

Rotation assessment and materialization use 64 deterministic binding-lock
stripes, so the full native interval serializes only identical bindings (plus a
bounded collision domain), not every provider rotation. A provider-wide
capacity lock covers only bounded collection maintenance, durable admission
reservation, prepared-operation publication, and final receipt replacement; it
is released before runtime admission, export, import, and recovery. One
monotonic budget—the earlier of the host deadline and a 30-second provider
ceiling—still covers all lock admission and native work. Each phase checks the
remaining absolute budget, native children are terminated and reaped at expiry,
artifacts are read back through the same 16 MiB bound, and provider-owned
rotation state records are capped at 1 MiB. Every deadline path releases its
binding lane while retaining any prepared/imported record needed for
identity-safe reconciliation, and independent binding stripes retain useful
overlap.
Authorizations are capped at 64 records, while pre-effect reservations,
unresolved operations, and materialization receipts share one 64-record
lifecycle cap so every admitted operation has capacity to become its replay
receipt. Abandoned reservations are removed immediately on ordinary failure and
reclaimed after two minutes when their binding stripe proves no owner remains.
Authorizations expire after ten minutes;
completed materializations replay for 24 hours, after which unreferenced source
and decision artifacts are retired. A durable receipt replaces its completed
operation record, while prepared/imported operations remain until safely
finalized. Artifact and decision collections are capped at 128 records, so
crash-orphaned publications cannot grow without bound.

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
