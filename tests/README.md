# Contract test ownership

Each integration-test target owns a product capability boundary. Its same-named
helper directory contains fixtures, requests, stream samples, and assertions for
that boundary; helper file names describe roles inside the boundary rather than
independent test suites.

| Cargo test target | Root and helper owner | Capability boundary |
| --- | --- | --- |
| `contract_launch_policy_terminal` | `contract_launch_policy_terminal.rs`, `launch_policy_terminal/` | Model policy, launch and resume recovery, native-runtime admission, request custody, and terminal delivery |
| `contract_session_projection` | `contract_session_projection.rs`, `session_projection/` | Bounded SQLite turn paging, session export, metadata-only capture, enumeration, and cursor custody |
| `contract_quota_auth` | `contract_quota_auth.rs`, `quota_auth/` | Quota observation, observer identity, credential refresh, and refresh reconciliation |
| `contract_control_plane_lifecycle` | `contract_control_plane_lifecycle.rs`, `control_plane_lifecycle/` | Settings and migration, activity evidence, setup and native rebind, rotation, and provider lifecycle |

Put a new contract in the target that owns the production invariant it proves.
Cross-capability helpers belong in `support/`; do not create numbered or
size-based clusters.
