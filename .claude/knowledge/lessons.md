# Lessons

<!-- distilled-through: 2026-11-18 -->

## Public API and bindings
- L-0001 (4×, #250 #184 #372): Preserve public Rust compatibility: non-exhaustive structs need external builders, public enums need fallback arms, and breaking 1.x type changes require a major release.
- L-0002 (2×, #246 #248): At FFI boundaries, define empty versus absent explicitly. Validate coupled config values together before mutating state, and test null/empty inputs.
- L-0003 (2×, #247 #361): napi Rust identifiers determine published TypeScript names; choose cross-binding spelling before release and sync generated index.d.ts when Rust docs or defaults change.
- L-0004 (1×, #262): Use napi u32 for JS-number duration/size options, widen inside Rust, and document its range; u64/usize expose BigInt and differ from Python/C.
- L-0005 (2×, #373 #379): Audit C headers, PyO3 and napi together when kernel contracts or validation change. Update each wrapper or explicitly document intentional differences.
- L-0006 (1×, #269): Document and test zero/negative limit semantics against a warehouse; kernel row limits and JDBC maxRows do not necessarily share unlimited sentinels.
- L-0007 (1×, #298): Use raw parameters to preserve qualified SEA sql_type values that TypedValue cannot express. Reject raw/typed and named/positional mixtures before dispatch.
- L-0008 (1×, #299 #306): Give inherent and trait methods distinct names when receiver mutability differs; Rust may resolve the &self trait method before an inherent &mut self method.
- L-0009 (1×, #314): Reusable statement options need explicit clear semantics; statement_conf survives set_sql even though bound parameters reset.
- L-0010 (1×, #389): Represent unspecified versus explicit opt-out separately, such as Option<bool>, so feature flags cannot silently re-enable an explicit false.
- L-0011 (1×, #389): Confirm connector-service flag names are provisioned for every kernel driver-family endpoint before relying on them; absent flags otherwise make a precedence tier inert.
- L-0012 (3×, #380 #432): Derived readers/cancellers and async getters must track owner close, drop and re-submit. Test owner invalidation with the parent session alive; server CLOSED can look like an empty success.

## Authentication and secrets
- L-0013 (1×, #241): Hand-write redacted Debug for every type that transitively contains tokens, secrets or JWTs; test that distinctive secret values never appear.
- L-0014 (2×, #247 #241 #257): Treat U2M client id, scopes and redirect ports as one registered-app bundle. Use exact registered ports; arbitrary loopback ports are allowed only when that client registration supports them.
- L-0015 (5×, #261 #283 #287): Shared cache keys must include identity, config and all response-varying inputs. Use isolated test caches; host-only caches leak first-session credentials and state.
- L-0016 (1×, #249): Sign JWT client assertions inside the refresh closure, not on every auth_header call; retain the cheap fresh-token fast path.
- L-0017 (1×, #249): Keep decrypted private-key material in zeroizing containers, including intermediate plaintext PEM strings, so normal drop does not leave secrets in freed heap.
- L-0018 (1×, #249): Keep rsa/RUSTSEC-2023-0071 out of shipped dependencies; a global advisory ignore intended for dev dependencies would also hide an accidental runtime dependency.
- L-0019 (1×, #275 #249): Expose the JWT/private-key M2M token-endpoint override in every binding and auth arm; Databricks-native OIDC discovery does not advertise private_key_jwt.
- L-0020 (1×, #263): Azure management tokens use the Entra v1 resource grant to retain the audience trailing slash; fetch them and send Azure SP headers only when azure_workspace_resource_id is set.
- L-0021 (2×, #289 #342): Require HTTPS for mTLS. Generate test CA/client identities at runtime and assert parser-specific failures to prove credential forwarding without committed keys.
- L-0022 (3×, #399 #430): Preserve transient auth failures and retry-exhaustion classification. The decision to retry now, including idempotency, is distinct from the exported error permanence/retryable flag.
- L-0023 (2×, #376 #423): Propagate shared auth acquisition failures before best-effort calls, and acquire credentials before starting a bounded HTTP-attempt clock.
- L-0024 (2×, #361 #454): Never echo result contents or credentialed URLs in errors/logs. Strip URL query/userinfo and parser value text; assert distinctive secret substrings are absent from both outputs.

## HTTP and protocol
- L-0025 (1×, #436): CloudFetch opt-out sets can_cloud_download=false at session creation, deduplicating caller casing variants; execution still uses INLINE_OR_EXTERNAL_LINKS. Test unsupported-server failures instead of restoring old disposition-only behavior.
- L-0026 (2×, #303 #436): Preserve case-sensitive dotted spark.* keys verbatim. When synthesizing a case-insensitive session key, remove all caller casing variants before inserting it.
- L-0027 (1×, #255 #266): Use the server universe proto as wire-model authority; sibling drivers can drift. Verify each field type and exact JSON encoding against it.
- L-0028 (1×, #261): Reuse the shared reqwest client and connection pool; constructing Client::new per request wastes TLS/pool state and can panic on TLS initialization.
- L-0029 (1×, #249): HTTP execute helpers return Err for non-2xx. Capture response bodies in their error path; code after execute(...).await? cannot inspect failed responses.
- L-0030 (2×, #293 #430): Prefer typed TLS/DNS errors or stable OS codes. If text classification is unavoidable, test glibc, musl, macOS and Windows samples plus negative cases across TLS backends.
- L-0031 (1×, #310): Construct errors through categorized helpers so retryability is correct; document and test deliberate overrides such as caller-deadline errors.
- L-0032 (1×, #309): Preserve a server-registered User-Agent prefix; SEA gates response disposition on known_driver_prefixes, so a replacement prefix can disable parallel downloads.
- L-0033 (2×, #323): A request timeout overrides the client timeout rather than taking its minimum. Cap it explicitly by remaining budget and reapply all per-request settings when rebuilding retries.
- L-0034 (1×, #288): HttpConfig::request_timeout is a read-idle timeout that resets on reads, separate from connect timeout; do not describe it as a whole-response deadline.
- L-0035 (1×, #316): rustls-tls uses bundled webpki roots; select rustls-tls-native-roots for OS-installed private CAs and test native-tls/rustls behavior explicitly.
- L-0036 (1×, #331): Only session-owned operations may mutate negotiated session state; detached statement-id operations need an explicit ownership marker.
- L-0037 (2×, #385 #426): Validate header-bound options at set_option and reject driver-owned custom headers. reqwest header() appends, so remove conflicts before adding defaults; test the outbound request.
- L-0038 (1×, #376): Use Instant::checked_add for server-controlled durations with a defined fallback; overflow must not panic on fail-open paths.
- L-0039 (1×, #376): Do not let an auxiliary fail-open call amplify connection-open latency; use short negative-cache backoff or move it off the hot path.
- L-0040 (2×, #401): Parse large responses from bytes and borrow attachments instead of UTF-8/string/Vec copies. Test escaped JSON too, since it exercises the owned deserializer path.
- L-0041 (1×, #380): SEA DELETE /api/2.0/sql/sessions/{id} requires warehouse_id in the query; omitting it returns 400 and leaks the session until idle expiry.
- L-0042 (1×, #436): Enforce explicit CloudFetch opt-out on response paths too; reject external links consistently on first and continuation pages when the server violates the disabled capability.

## Async lifecycle and FFI
- L-0043 (4×, #261 #299 #300): Provide best-effort Drop cleanup as well as explicit close. Recover poisoned cleanup locks and retain identifiers after failed close RPCs so idempotent cleanup can retry.
- L-0044 (2×, #277): At the C ABI, copy !Send pointer inputs before spawning futures on owned runtime workers; block foreign caller threads on a channel, never Runtime::block_on.
- L-0045 (1×, #277): Pin a host-loaded module while detached runtime threads can execute its code, and check/log module-pin failure rather than silently allowing unsafe unload.
- L-0046 (2×, #297 #413): Store an appropriate runtime Handle instead of using ambient tokio::spawn from bindings/Drop. Process-shared objects need a process-lifetime runtime, not a session runtime.
- L-0047 (3×, #326 #329 #413): Build every kernel runtime with util::runtime stack sizing: preserve a larger host RUST_MIN_STACK while applying the 2 MiB floor. Cache successful shared-runtime creation, not failure.
- L-0048 (2×, #312 #374): Do not form whole-allocation &mut references for concurrent FFI handles. Use shared references/interior mutability or project raw pointers to specific fields before borrowing.
- L-0049 (1×, #312): Dropping an awaited future bounds caller waiting, not a spawned task or its network request. Wire cancellation into the underlying work or document deferred reclamation.
- L-0050 (2×, #322): Publish provider-level terminal errors and wake all waiters even before per-chunk state exists; deliver already-ready in-order chunks before surfacing the terminal failure.
- L-0051 (1×, #322): Register and enable Notify::notified before checking state/terminal conditions; registering after checks can lose notify_waiters notifications.
- L-0052 (1×, #323): Carry typed Error values through channels/state machines, preserving code, SQLSTATE and source; test classification at the caller-visible read boundary.
- L-0053 (5×, #323 #339 #374): Bound every await/backoff with remaining deadline, audit all helper callers, and distinguish Default/Unlimited/client deadlines. Latch terminal state so later polls cannot replace success with timeout.
- L-0054 (2×, #339 #374): Route both inner and outer deadline expiries through the same nonblocking cleanup path; gate cancel/close dispatch once so repeated expired polls cannot flood RPCs.
- L-0055 (2×, #356): Wrap the entire extern C body in catch_ffi, including prologue/epilogue. Initialize out-params to defined sentinels immediately after null checks and before fallible work.
- L-0056 (2×, #356 #384): Centralize SEA extended-status propagation. FFI tests must assert both status code and caller-thread last-error fields, so a typeless or dropped error cannot pass.
- L-0057 (1×, #372): Exercise nonblocking readers through the real provider: Pending/Ready/End, empty chunks, terminal errors, status mapping and retrievable caller-thread typed errors.
- L-0058 (3×, #388): Release the GIL before blocking runtime shutdown, use nonblocking Drop shutdown, suppress background Python logging before interpreter finalization, and bound deadlock tests by timeout.
- L-0059 (2×, #462): Binding close must release child session Arcs before a fallible await and avoid waiting on an in-flight fetch lock. Prefer a spawned lock waiter over flag/try_lock handoffs that can both miss.
- L-0060 (1×, #466): After clearing a CAS-claimed in-progress flag, recheck the trigger with adequate ordering; rearm only paths that can make progress, never non-advancing/error exits.
- L-0061 (1×, #466): Coalesce requests without holding locks across network calls: key attempts, share same-key errors/results, recheck after different-key waits, and detach work from individual waiter cancellation.
- L-0062 (1×, #431): A non-consuming peek buffers errors as well as data for the next read. Test one-shot-error-then-EOF; optional enrichment must not silently turn server success into execution failure.
- L-0063 (1×, #451): Cached terminal ExecuteResponse payloads must be complete and take-once. Clear on error, close, Drop, owner release, timeout and fresh status polls; document how terminal observations retain inline results.

## Arrow results and metadata
- L-0064 (2×, #301 #373): Empty catalog/pattern semantics vary by metadata operation; test short-circuit ordering and avoid turning an empty request into catalog=None (all catalogs). GetFunctions accepts meaningful empty catalogs.
- L-0065 (3×, #291 #353 #393): Client-side catalog/schema/table identifier filters use eq_ignore_ascii_case; table_types filters are case-sensitive. Preserve Databricks Thrift type-info flags, including CASE_SENSITIVE: STRING=true, CHAR/VARCHAR=false.
- L-0066 (1×, #355): Translate JDBC patterns separately from SQL literal escaping; translate wildcards first and escape exactly once where the result enters SQL.
- L-0067 (3×, #355 #364): Normalize native metadata values per operation/schema, not bare column names or position alone. Native GetColumns ordinals need +1; SHOW fallback is already one-based.
- L-0068 (2×, #369 #381): In metadata fallback reshapes, preserve optional server columns when present and tolerate missing/uncastable ones as null; test native, fallback and empty-result schemas.
- L-0069 (2×, #369 #379): Resolve current catalog/schema live after USE operations. Do not backfill current schema independently when an explicit different catalog was supplied.
- L-0070 (1×, #365): The SEA manifest defines logical types; arrow_schema describes physical attachments. Use consistent logical-schema fallback on broken attachment schemas for empty and streaming results.
- L-0071 (1×, #360): Numeric-to-text conversions need reference parity tests for fractional, huge and tiny values; integer cases miss precision, trailing-zero and scientific-notation drift.
- L-0072 (3×, #391 #390 #396): Match server Arrow conventions literally: ARRAY child name element and Thrift Spark:DataType:* metadata keys. Assert literal wire strings, not only shared constants.
- L-0073 (3×, #361 #410): Nested Arrow conversion must honor ancestor validity/offsets before parsing. Verify physical nulls before tightening nullability, and test default transforms against null-bearing and real server data.

## Telemetry and logging
- L-0074 (2×, #255 #266 #256): Mirror each telemetry field meaning and JSON type: proto int64 fields are strings while wrapper numbers stay numeric; stack_trace must not contain unrelated status codes.
- L-0075 (1×, #272): Derive telemetry flags from the same parser as the operational code, such as CompressionCodec::from_manifest; duplicate classification logic drifts.
- L-0076 (3×, #272 #274): Give terminal telemetry one owner: never finalize on resumable errors, record real error paths before Drop, and cover abandoned async handles without losing stream/CloudFetch metrics.
- L-0077 (2×, #272 #274): Use None for unmeasured counters/latencies, not placeholder zero. Initial-chunk latency belongs to chunk index 0, not the first concurrent completion.
- L-0078 (1×, #272): Measure execution duration at terminal observation, not delayed Drop, which measures caller handle lifetime.
- L-0079 (1×, #278): A custom tracing Layer must walk event_scope to include span fields; event.record alone differs from fmt::layer and loses query/statement context.
- L-0080 (1×, #278): Document every first-call-wins initialization success/no-op case, including OFF, so callers do not mistake successful skips for an installed sink.
- L-0081 (1×, #287): Validate a sub-feature tuning knob only when that sub-feature is enabled; an unused invalid value must not disable an entire parent feature.

## Verification and CI
- L-0082 (2×, #184 #314): README Rust blocks are untested unless included as crate docs; verify external imports/builders explicitly. Public rustdoc must not link to private anchors.
- L-0083 (4×, #259 #279 #322): Isolate fallback/error sources and assert exact codes, SQLSTATE and distinctive nonzero values; generic Err or zero/default-only checks miss the wrong path.
- L-0084 (2×, #261 #353): Drive protocol changes through real transport/exporter/provider routing with wiremock, asserting paths, payloads and failure behavior; helper-only or capturing-fake tests bypass wiring.
- L-0085 (5×, #263 #291 #331): Wiremock cannot establish server parity or capability acceptance. Capture real fixtures or add ignored tests/v0_*_e2e.rs warehouse coverage, including unsupported-server behavior.
- L-0086 (2×, #293 #312): Sequence concurrency tests so operations truly overlap and bound both client/server waits. A fixed mock accept-count wait can otherwise hang on a missing retry.
- L-0087 (2×, #326 #454 #462): Test singleton/env/global-subscriber behavior in child processes or separate integration-test binaries; cargo unit tests share one process and global state.
- L-0088 (4×, #288 #316 #425): Root cargo commands omit standalone adbc/pyo3/napi workspaces. Target each manifest and lockfile, align CI/local feature flags, and ensure filtered jobs actually execute feature-gated tests.
- L-0089 (1×, #287): Keep PyO3 extension-module off for Rust cargo tests; enable it for maturin wheel builds where the interpreter supplies libpython symbols.
- L-0090 (1×, #288): Compare floating-point values with guards/if, not literal match patterns that trigger the illegal_floating_point_literal_pattern lint under -D warnings.
- L-0091 (1×, #333): Avoid PowerShell automatic-variable names such as $host, $profile and $error for locals; use distinct task-specific names.
- L-0092 (2×, #333 #429): Keep required checks always present. Compute path changes from BASE...HEAD with full history and include standalone workspaces plus their root path dependencies.
- L-0093 (1×, #429): repository_dispatch 204 only acknowledges delivery. Sender-owned checks need cancellation/failure completion and stale-check expiry; list check runs with filter: all.
- L-0094 (2×, #411 #412 #473): Regenerate dependency pins on the normal toolchain with a healthy registry. Inspect every lockfile for unrelated downgrades/checksum drift and validate locked builds.

## Review follow-up [phase=review,followup]
- L-0095 (1×, #257): Before claiming a review finding is already fixed or a symbol exists, reread HEAD and cite its current lines; remembered or partial source reads waste review rounds.
