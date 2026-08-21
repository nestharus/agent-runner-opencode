# OpenCode native state adapter contract v1

Contract identity: `agent-runner-opencode.opencode-native-state/v1`

This provider treats OpenCode as one effectful native-state participant behind a
direct executable boundary. The numbered `opencode1` through `opencode5` values
remain logical account and policy identities; they are never executed. For every
native operation the provider instead resolves the `opencode` executable once,
canonicalizes and hashes its bytes, binds the exact cleared stable environment
and fixed `--pure` argument, persists that identity per numbered account, and
revalidates the executable bytes before reuse. OpenCode is therefore unable to
select a different implementation through a mutable numbered wrapper after
admission. A changed executable, fixed argument, contract identity, or stable
state-selection environment requires the native-identity rebind protocol.
Durable launch recovery records the same direct executable hash, fixed argument,
contract identity, and state environment and validates them before list/export
observation. Predecessor launch records without that evidence fail closed
instead of executing an unbound wrapper.

Schema-v1 native-runtime bindings are a bounded transition input only. The
provider first validates the exact predecessor wrapper bytes, then, while
holding the account runtime lock, resolves and persists the schema-v2 direct
binding before any new native effect. It never uses that predecessor wrapper as
the acting implementation after the upgrade-capable provider is installed.

The bound environment identifies the native state namespace. `HOME` and any
explicit stable XDG/OpenCode configuration values are part of the identity. If
an isolated numbered account has no explicit `XDG_DATA_HOME`, the provider binds
`$HOME/.opencodeN` for account N. The provider clears all other ambient values,
adds the logical account identity as `OULIPOLY_OPENCODE_ACCOUNT`, and invokes the
canonical absolute executable; `PATH` remains identity-bound for tools that an
OpenCode turn may itself execute, but does not select the OpenCode executable
after admission. `--pure` excludes external plugins from the acting boundary.

The provider relies on these command observations and validates them at its
edge:

- `run --format json` may create or advance one session in the bound namespace.
  JSON events are transport observations, while durable provider custody and
  later list/export reconciliation own ambiguous response or process failure.
- `session list --format json` is the bounded set of sessions visible in the
  bound namespace at that invocation. It is used for enumeration and as a
  recovery observation; ambiguous or multiple recovery matches fail closed.
- `export <session>` is the authoritative serialized state of that exact
  visible session at the observation point. The provider validates all embedded
  session identities before using it for turns, completion, or recovery.
- `import <artifact>` may create one session from the supplied native artifact.
  Rotation persists its operation before import and validates the resulting
  session by exact export before terminalization; an ambiguous import window is
  never blindly repeated.
- `auth list` may refresh the bound namespace's credential file. The provider
  serializes the canonical credential identity, observes that exact file before
  and after the command, and records reconciliation-required rather than
  attributing an ambiguous change.

OpenCode owns its internal database and file synchronization. The provider does
not infer a committed effect from process exit alone: it combines bounded,
identity-validated native observations with its own durable request, rotation,
session, or quota state machines. Concurrent native operations may coexist only
through those provider-owned admission and reconciliation rules. This contract
does not authorize an unbound executable, wrapper-selected implementation,
external plugin, alternative state namespace, or unvalidated output to mutate
the provider's causal account.
