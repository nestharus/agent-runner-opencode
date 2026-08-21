# ChatGPT WHAM in-process HTTP contract v1

Contract identity: `agent-runner-opencode.chatgpt-wham-http/v1`

Production quota observation has no executable transport participant. The
provider constructs one blocking HTTPS GET to the fixed ChatGPT WHAM usage
endpoint with its source-included `quota_adapter` and the Cargo.lock-pinned
`reqwest`/rustls/webpki implementation. It disables environment proxy discovery,
rejects redirects, applies a 20-second request deadline, and supplies the access
token and account identity only as request headers.

The provider reads the response through a 512 KiB upper bound, accepts only UTF-8,
retains the exact HTTP status, parses one JSON response, and validates finite
percentage/reset fields before producing quota windows. The in-process client has
no subprocess environment, argv, inherited file descriptor, or independently
selected executable through which it could mutate OpenCode credentials, native
session databases, or provider-owned records.

Each account persists a schema-v3 observer identity derived from this contract,
the source-included adapter digest, and the complete locked dependency graph.
Schema-v1 and schema-v2 external-curl records are authenticated as transition
evidence and atomically replaced under the account observer lock before the first
production probe; their external programs are never executed by this version.
A provider source or dependency-lock change therefore requires the typed
component rebind protocol before that account can resume quota admission.

The `contract-test-fixtures` Cargo feature is a non-release transport fixture. It
is effective only with debug assertions and substitutes a byte-identified fake
curl so integration contracts can deterministically exercise HTTP, output-bound,
timeout, and credential handling. Default and release builds compile only the
in-process transport and never admit or execute that fixture path.
