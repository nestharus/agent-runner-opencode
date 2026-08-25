# OpenCode native state adapter contract v1

Contract identity: `agent-runner-opencode.opencode-native-state/v1`

This provider treats OpenCode as one effectful native-state participant behind a
direct executable boundary. The numbered `opencode1` through `opencode5` values
remain logical account and policy identities; they are never executed. On
initial admission, rebind, and predecessor upgrade the provider resolves the
`opencode` executable, canonicalizes and streams a bounded hash of its bytes,
and admits it only when
the target platform, byte length, digest, version, and semantic contract match
`contract/native-implementation-manifest-v1.json`. It binds that manifest
identity, an admitted size/inode/change metadata stamp, the durable environment
selectors, and fixed `--pure` argument, then persists them per numbered
account. Reuse validates executable status, canonical path, the constant-time
metadata stamp, and the immutable build manifest under the account lock without
rereading the whole executable. The stamp is persistence/incarnation evidence,
not part of the semantic component identity. OpenCode is therefore unable to
select a different implementation through a mutable numbered wrapper after
admission. When OpenCode performs a same-canonical-path auto-update, the
provider holds the account lock, hashes the replacement, runs a bounded
`--version` probe, and admits only a strictly forward numeric version from the
existing admitted lineage. It atomically persists the replacement's exact
hash, version-derived lineage ID, and metadata stamp before using it. This
continuity path is unavailable for initial admission, path changes, downgrades,
fixed-argument changes, contract changes, or state-selector changes; those
remain fail-closed and require the native-identity rebind protocol. A static
manifest entry may pre-admit an expected update but is not required for a
forward same-path auto-update.
Durable schema-v10 launch recovery records the same direct executable hash,
manifest ID/version, constant-time admitted metadata stamp, fixed argument,
contract identity, and state environment and validates them before list/export
observation. An operation admitted before an auto-update therefore retains its
old exact recovery obligation instead of silently executing the successor.
Predecessor launch records
with manifest evidence but no stamp use a bounded content check during recovery;
records without complete manifest evidence fail closed instead of executing an
unbound wrapper. Setup uses the same boundary: it
admits the direct executable against the source-included manifest before any
exact-path `--version` probe and never resolves or executes the numbered logical
account identities. Per-account setup readiness is the conjunction of that one
direct-runtime result and the account's declared auth-file evidence.

Schema-v1 wrapper, schema-v2 direct native-runtime, schema-v3 manifest-bound,
schema-v4 full-environment, and schema-v5 PATH-bound bindings are bounded
transition inputs only. The provider validates their recorded implementation
evidence and requires the currently selected direct implementation to be
manifest-approved. While holding the account runtime lock, it persists the
schema-v6 manifest-bound identity and metadata stamp before any new native
effect. Schema-v4 activation removes accidentally persisted per-invocation
values, and schema-v5 activation removes `PATH`, while holding the account lock.
It never uses the schema-v1 wrapper as the acting implementation after the
upgrade-capable provider is installed.

The bound environment identifies the executable and native state namespace.
`HOME`, `XDG_DATA_HOME`, the provider Bash-timeout policy, and the logical
account marker are its only persisted selectors. If
an isolated numbered account has no explicit `XDG_DATA_HOME`, the provider binds
`$HOME/.opencodeN` for account N. Every other inherited or request-declared
environment variable is forwarded to the child for that invocation without
becoming durable identity state. The provider adds the logical account identity
as `OULIPOLY_OPENCODE_ACCOUNT` and invokes the canonical absolute executable.
`PATH` resolves the admitted executable for the current launch and is forwarded
unchanged for tools that the OpenCode turn executes, but it is not persisted or
identity-bound. `--pure` excludes external plugins from the acting boundary.

A current-schema account record whose selected state root is exactly the
canonical root declared for a different configured numbered account is a
recoverable cross-account binding. Under the account lock, the provider may
replace only that selector with the current account's exact declared canonical
root while preserving provider state. This exception does not admit a custom,
unknown, ambiguous, or undeclared selector change; those remain fail-closed and
require the native-identity rebind protocol.

Agent Runner launches favor completion of long-running agent work over
OpenCode's shorter implicit per-Bash cutoff. The provider therefore owns a
finite fallback of `2000000000` milliseconds (about 23.1 days) for
`OPENCODE_EXPERIMENTAL_BASH_DEFAULT_TIMEOUT_MS` when the host/request supplies
no value. The value is below the signed 32-bit millisecond ceiling and is not
an unbounded timeout. A host/request-authored value takes precedence and becomes
part of the durable runtime identity. When `host.deadline_unix_ms` is present,
that deadline remains the outer provider-operation limit independently of this
per-Bash fallback. Without a host deadline, the provider deliberately accepts
the longer resource-occupancy window in order to avoid prematurely terminating
valid agent work; the finite fallback remains the last per-Bash ceiling.

The provider relies on these command observations and validates them at its
edge:

- `run --format json` may create or advance one session in the bound namespace.
  JSON events are transport observations, while durable provider custody and
  later list/export reconciliation own ambiguous response or process failure.
  OpenCode exposes no out-of-band request-local identity that both survives
  provider loss and distinguishes identical sibling turns. The provider
  therefore deliberately appends one reserved, provider-authored
  `[OULIPOLY-DELIVERY <64-lowercase-hex-digest>]` item to every actual child
  payload. This chooses crash-safe at-most-once recovery over byte-exact
  caller-payload fidelity: the model may observe the item, it is not claimed to
  be semantically neutral, and native/session exports preserve it inside the
  native `role=user` transport message without claiming human authorship.
  There is no opt-out because an unmarked launch cannot provide the same
  request-local recovery proof; exact-complete-payload workloads are outside
  this adapter's launch-fidelity contract.
- `session list --format json` is the bounded set of sessions visible in the
  bound namespace at that invocation. The native edge translates every row
  once into a typed provider observation with one canonical non-empty session
  identity plus an explicit missing/absolute/invalid directory classification
  and optional created/updated time, title, and turn count. Compatibility
  aliases and decimal-string timestamps are resolved there; conflicting aliases,
  invalid field types, or a row without identity invalidate the whole
  observation. Enumeration and launch recovery consume that same type. An
  invalid directory remains a warning in enumeration but is unknown—not a
  mismatch—for recovery, so malformed evidence can never become
  consumer-specific absence. Ambiguous or multiple recovery matches fail
  closed.
- `export <session>` is the authoritative serialized state of that exact
  visible session at the observation point. The provider validates all embedded
  session identities before using it for turns, completion, or recovery. Each
  message's top-level and nested provider/model/variant forms are alternative
  wire representations of one aggregate identity. The native adapter selects a
  sole present form, accepts simultaneous forms only when their complete
  aggregates agree exactly, and rejects conflicts or complementary partial
  forms; downstream launch, resume, and session domains receive only the typed
  canonical aggregate.
- `import <artifact>` may create one session from the supplied native artifact.
  Rotation persists its operation and exact process-group incarnation before
  import, proves the whole group terminal or recycled after direct-leader exit,
  and validates the resulting session by exact export before terminalization;
  an ambiguous import window is never blindly repeated.
- `auth list` may refresh the bound namespace's credential file. The provider
  serializes the canonical credential identity, persists the exact process-group
  incarnation, observes that exact file before the command, proves the whole
  group terminal or recycled, and only then takes the authoritative successor
  credential snapshot. It requires that ordering before committing or allowing
  credential reconciliation and records reconciliation-required rather than
  attributing an ambiguous change.

OpenCode owns its internal database and file synchronization. The provider does
not infer a committed effect from process exit alone: it combines bounded,
identity-validated native observations with its own durable request, rotation,
session, or quota state machines. Concurrent native operations may coexist only
through those provider-owned admission and reconciliation rules. This contract
does not authorize an unbound executable, wrapper-selected implementation,
external plugin, alternative state namespace, or unvalidated output to mutate
the provider's causal account.
