# Repository Guidelines

## Project Structure & Module Organization

- `common/src/` hosts shared utilities (logging, metrics, settings); keep cross-cutting concerns here.
  - `settings.rs` — TOML config + env var loading; all tunables live here, not in CLI args.
  - `metrics.rs` — 22 Prometheus counters/gauges/histograms; increment at decision points, not in libraries.
- `protocol/src/` defines the wire protocol; `frame.rs` for codec, `messages.rs` for request/response types, `predicate.rs` for the predicate AST.
- `storage/src/` is the core crate:
  - `cache_trait.rs` — unified KV interface (`Cache` trait).
  - `page_cache.rs` — CLOCK-Pro + TinyLFU + Bloom filters, disk layout.
  - `metadata_cache.rs` — Parquet footer caching with TTL-based staleness.
  - `gcs.rs` — custom GCS client (monoio TLS, HTTP/1.1 keep-alive pool, retry with exponential backoff).
  - `cache.rs` — orchestrates metadata + page caches, range coalescing.
  - `retry.rs` — `RetryPolicy` + `GcsError` with transient/permanent classification.
- `cluster/src/` manages distributed state:
  - `service.rs` — `MembershipService` trait (extensible for Raft).
  - `static_membership.rs` — fixed peer list backend.
  - `etcd.rs` — etcd backend (lease keepalive, watch, tokio bg thread).
  - `ring.rs` — consistent hash ring with virtual nodes.
  - `membership.rs` — wraps service + ring, polls events.
- `server/src/` is the binary entrypoint:
  - `data.rs` — binary data plane (connection handling, flow control, prefetch, thundering herd).
  - `http.rs` — HTTP/1.1 data plane (zero-copy sendfile).
  - `control.rs` — health/readiness/metrics endpoints.
- `bench/src/` contains the CLI benchmark tool; `bench/benches/` contains criterion micro-benchmarks.

## Build, Test, and Development Commands

- `cargo check` — fast type-checking across all crates.
- `cargo test --all` — run all unit tests across the workspace.
- `cargo bench` — run criterion micro-benchmarks (page_cache, hash_ring, frame_codec).
- `cargo clippy --all-targets` — lint all crates; fix warnings before committing.
- `cargo fmt --all` — format all Rust code.
- `cargo run --bin ruxio-server` — run the server (reads `ruxio_config.toml` or `RUXIO_CONFIG` env var).
- `RUXIO_GCS__BUCKET=my-bucket cargo run --bin ruxio-server` — run with env overrides.

## Configuration

All configuration is in TOML files + env vars. See `common/src/settings.rs` for the full schema.

- **Config file**: `ruxio_config.toml` (or path in `RUXIO_CONFIG` env var, or `--config` CLI arg).
- **Env overrides**: `RUXIO_` prefix, `__` separator for nesting (e.g., `RUXIO_CACHE__PAGE_SIZE_BYTES`).
- **No CLI args** for tunables — only `--config` and `--bench-populate` are CLI flags.
- When adding new tunables, add them to `Settings` struct in `settings.rs`, not as CLI args.
- Config option names must be clear and self-documenting — no cryptic abbreviations.
- Implement `Default` on config structs; validate fields eagerly in constructors, not lazily at use sites.

## Coding Style & Naming Conventions

**Optimize for the reader.** Code is read far more often than it is written. Every naming choice, comment, and abstraction should serve the person reading the code next — not the person writing it now. When readability conflicts with a style rule, readability wins.

**Be consistent with surrounding code.** When editing an existing file, match its local conventions — even if you'd write it differently from scratch. Consistency within a file matters more than personal preference.

Evaluate every change across three axes: **correctness & maintainability**, **safety** (thread safety, bounded resources, error handling), and **user-friendliness** (clear config names, documentation, error messages).

### Formatting & Organization
- Format with `cargo fmt --all`; keep modules and functions `snake_case`, types `PascalCase`.
- Run `cargo clippy --all-targets` before committing; treat warnings as errors.
- Place `use` imports at the top of the file, not scattered within function bodies. **No glob imports** (`use module::*`) except in test preludes or re-exporting a crate's public API.
- Place `#[cfg(test)] mod tests` as a single block at the bottom of each file — no production code after it.
- Struct definition before `impl` blocks. Within a struct, order fields: `pub` first, then `pub(crate)`, then private.
- Arrange methods in logical reading order: public API first, then private helpers in the order they're called. A reader should be able to follow the code top-to-bottom.
- **Avoid deep nesting** — restructure with early returns, `let...else`, `?`, or extracted helper functions. If code exceeds ~4 indentation levels, refactor.
- Extract substantial new logic into dedicated submodules rather than growing a single file indefinitely.
- **Never mix reformatting with logic changes.** If a file needs formatting cleanup, do it in a separate commit or PR so reviewers can focus on behavior.

### Naming
- **No abbreviations** — use full words (`position_count` not `pos_cnt`, `buffer_size` not `buf_sz`), except for universally established domain terms (e.g., `rpc`, `gcs`, `ttl`, `io`, `id`).
- Name variables after what the value *is* (e.g., `peer_id` not `mask`, `page_offset` not `val`) — precise names act as inline documentation.
- Prefix booleans with `is_` or `has_`; default booleans to `false`. Use `disable_*` instead of `enable_*` when the feature defaults to on.
- **Avoid generic module names** like `utils.rs`, `helpers.rs` — name modules after the concept they represent (e.g., `retry.rs`, `ring.rs`). Ask: "what do these functions have in common beyond being useful?"
- Name functions to reflect their actual scope — `handle_page_eviction` not `handle_eviction` if only page eviction is handled.
- **Avoid generic `get_` prefixes** — use verbs that convey what actually happens: `find_` (lookup, may fail), `fetch_` (remote/expensive), `compute_` (derived), `load_` (from disk/storage). Reserve `get_` only for trivial field accessors.
- **`_` prefix is only for truly unused bindings.** If you read the variable, drop the underscore. `_foo` means "I know this is unused, and that's intentional."
- Use **type aliases for domain concepts** at API boundaries — `type PageId = u64;` or a newtype wrapper, not raw `u64` everywhere. This turns the type system into documentation.
- Use **named constants** for magic numbers and strings — no bare `4096`, `64`, `"default"` in logic. Give them a name that explains the intent: `const PAGE_SIZE_BYTES: usize = 4 * 1024 * 1024;`.
- Use digit separators for large numeric literals: `10_000` not `10000`, `1_048_576` not `1048576`.
- Prefer `const` over `static` for compile-time values. Use `static` only when you need a fixed memory address or interior mutability (`static` with `AtomicU64`, etc.).

### Variables & Mutability
- Initialize variables at their declaration; avoid uninitialized bindings assigned later.
- Declare variables in the smallest possible scope, closest to where they are used.
- Minimize `mut` — prefer transformations that return new values. When mutation is necessary, keep the mutable window as short as possible.
- Prefer value types → `Option<T>` → `Box<T>` in that order for ownership.
- Inline function calls when used only once; don't create a named binding just to pass it to the next line.
- **Never take ownership when a borrow suffices.** Accept `&str` not `&String`, `&[T]` not `&Vec<T>`, `&Path` not `&PathBuf`. This is idiomatic Rust and avoids forcing callers to allocate.
- When multiple function parameters share the same type, add comments for clarity at call sites: `send(/* host= */ addr, /* port= */ 8080)`.
- Prefer function references over trivial closures: `.map(ToString::to_string)` not `.map(|x| x.to_string())`.
- **Avoid macros when functions or generics suffice.** Macros obscure control flow and are harder to debug. Use them only for reducing genuine boilerplate that cannot be expressed with generics, traits, or functions.

### Iterator & Control Flow Discipline
- Use `let Some(x) = iter.next() else { return Err(...) }` — never call `.next()` twice (once to check, once to use).
- When advancing multiple iterators in lockstep, advance *all* of them before any `continue` or conditional branch. An early `continue` that skips a `.next()` causes silent misalignment.
- Prefer iterator chains (`.filter().map().collect()`) over manual `for` loops with `push` — unless the loop body has complex control flow or multiple side effects. **Exception: avoid iterator chains in hot loops or performance-sensitive sections** where the abstraction cost matters; use explicit `for` loops for clarity and control.
- Use `for (i, item) in collection.iter().enumerate()` not manual index tracking.
- **No wildcard `_ =>` in `match` on owned enums.** Use exhaustive patterns so the compiler catches missing variants when new ones are added. Use `_ =>` only for borrowed/external enums you don't control.

### API Design
- Keep public APIs minimal — prefer `pub(crate)` with selective `pub use` re-exports over broad `pub` visibility.
- Use builder pattern with `with_` prefixes for optional parameters (e.g., `Config::new(required).with_timeout(dur)`).
- Use `Into<T>` and `AsRef<T>` trait bounds on public APIs for flexible inputs.
- Use enums instead of magic numbers for variant types, format versions, and discriminators — leverage exhaustive `match`.
- Use strongly-typed structs instead of `HashMap<String, String>` for configuration — convert to strings only at serialization boundaries.
- Replace mutually exclusive boolean flags with a single enum/mode parameter.
- Keep traits minimal — only core abstraction methods. Move helpers to standalone functions and config to struct fields.
- **Eagerly validate in constructors.** Assert preconditions in `new()` and builders — not at usage sites deep in the call stack. Fail fast with a clear error at the point of construction.
- Add `#[must_use]` on functions where ignoring the return value is almost certainly a bug (e.g., functions that return a new value without side effects).
- Delete obsolete internal methods in the same PR that introduces their replacements. For public APIs, deprecate with `#[deprecated]` first.
- **Never expose internals for testing.** Design testable interfaces; do not add `pub` or `#[cfg(test)]` accessors to reach into private state.
- Use stable, versioned serialization formats for persistent data (cache metadata, index files). Never assume format stability across versions without explicit versioning.
- **Gate risky new features behind config flags** until proven stable in production. Don't force-enable new behavior in the same PR that introduces it.

### Logging
- Use `tracing` over `log` for new code; existing `log`/`fern` usage in common is acceptable.
  - `debug!` — routine/high-frequency operations (cache hits, connection recycling).
  - `info!` — infrequent, operator-visible state changes (server started, member joined ring).
  - `warn!` — unexpected conditions that are handled (retry triggered, connection reset).
  - `error!` — unrecoverable failures requiring operator attention.
- Remove `println!` and `dbg!` before merging — these are never acceptable outside of local debugging.

### Other
- Use `Vec::with_capacity()` when the size is known or estimable — prefer over-estimating to multiple reallocations.
- Wrap expensive-to-clone fields in `Arc<T>` to avoid deep copies.
- **Be mindful of memory usage.** Prefer `RoaringBitmap` over `HashSet<u32>` for large sparse sets, `SmallVec` for small-N collections, `Cow<str>` for mostly-borrowed strings. Avoid collecting entire streams into `Vec` when you can process iteratively.
- Remove dead code entirely instead of adding `#[allow(dead_code)]`.
- **Every `unsafe` block must have a `// SAFETY:` comment** explaining why the invariants are upheld. No exceptions.
- Keep the `Cache` trait in `storage/src/cache_trait.rs` as the unified KV interface.
- Membership backends implement `MembershipService` trait in `cluster/src/service.rs`.

## Comments & Documentation

- Comments should explain non-obvious "why" reasoning, not restate what the code does. Write comments that capture information that *could not* be represented as code.
- Document every public struct, enum, trait, and non-trivial public function with `///` doc comments. Explain semantic meaning and valid values, not just the type signature.
- **Document struct fields individually** — especially fields with non-obvious semantics, valid ranges, or units. A reader shouldn't have to trace usage to understand what a field means.
- Start doc comments with active verbs: `/// Returns the page at the given offset` not `/// This function returns the page...`.
- Write comments as full sentences: capital letter, period at end.
- Document `Option<T>` fields with the meaning of both `Some` and `None`.
- Document magic constants, thresholds, and non-obvious numeric values — explain what the value represents and why it was chosen.
- Use `// TODO:` and `// FIXME:` for forward-looking changes to distinguish current behavior from planned improvements.
- In large functions, group related code blocks with short high-level comments that explain the phase or intent.

## Error Handling

- **No `.unwrap()` in production code** — use `match`, `if let`, `let...else`, or `?`. Reserve `.unwrap()` for tests only. If truly unavoidable, use `.expect("reason")`.
- **No silent `let _ =` on `Result`** — log at `debug` or `warn` level, or increment an error metric.
- Use `snafu` for error types in all non-test code (libraries and binaries). Use `anyhow` only in tests. `thiserror` is not used in this project.
- **Every error variant must carry `#[snafu(implicit)] location: Location`** for automatic file/line tracking. Use `#[snafu(visibility(pub))]` on error enums to generate public context selectors. Use `BoxedError` (`Box<dyn Error + Send + Sync>`) for flexible source wrapping. See `storage/src/error.rs` for the canonical example.
- **Use snafu context selectors** instead of constructing errors directly: `.context(ConnectionSnafu { detail: "..." })?` for Result chains, `PageAssemblySnafu { detail: "..." }.fail()` for returning errors, `GcsSnafu.into_error(source)` for wrapping sources manually.
- **Match error variants to root causes**: invalid input errors for caller data issues, corruption errors for integrity problems, not-found for missing resources, I/O errors for system failures. Do not use a generic catch-all. Use specific variants so failure frequencies can be tracked via Prometheus metrics — a single `StorageError::Io` is far less useful than `StorageError::ConnectionTimeout` vs `StorageError::DiskFull`.
- Include full context in error messages: variable names, actual values, sizes, types, indices. `"Page offset {} exceeds file size {}"` not `"Invalid offset"`.
- Validate inputs at API boundaries and reject invalid values with descriptive errors — never silently clamp, adjust, or default.
- Validate mutually exclusive options in builders/configs — return a clear error if both are set.
- Use `checked_add`/`checked_mul` for counters and offsets; return errors on overflow instead of wrapping.
- Prefer `debug_assert!` for internal invariants; reserve `assert!` for conditions that prevent data corruption. Always include a descriptive message.
- Use `.ok_or_else()` for required config parameters instead of `unwrap_or()`.
- Log warnings for best-effort/cleanup failures rather than silently swallowing or propagating.
- **Don't log a warning then immediately return the same error.** The error propagation carries the information — a redundant `warn!` before `return Err(...)` just adds noise. Log warnings only for conditions that are *handled* and won't propagate.
- Don't silently guard against impossible conditions — use `debug_assert!`, return an explicit error, or remove the check entirely.
- **Don't silently work around infrastructure or tooling bugs.** Report the root cause and fix it properly. Silent workarounds accumulate and obscure real problems.

## Safety & Performance

- **No unbounded data structures** in performance-sensitive paths — always enforce capacity limits or use bounded containers.
- **No expensive calls on the hot path** — move allocations, logging, and validation outside inner loops.
- Hoist loop-invariant conditions outside hot loops — branch once, then run the tight loop.
- Pre-allocate buffers with known sizes using `Vec::with_capacity()` or `vec![0u8; len]`.
- **monoio thread-per-core**: no `Send`/`Sync` required for most types. Each core owns its own cache partition. `Rc<RefCell<>>` for intra-thread, `Arc<AtomicBool/AtomicU32>` for cross-thread flags.
- **No tokio on hot path**: all data plane I/O goes through monoio's io_uring backend. The only tokio usage is in the etcd background thread (`cluster/src/etcd.rs`), communicating via `std::sync::mpsc`.

## Architecture Notes

- **Page cache internals**: CLOCK-Pro (hot/cold/test lists) for eviction, Count-Min Sketch (TinyLFU) for admission control, per-file Bloom filters for fast negative lookups. These are internal to `PageCache` — callers use the simple `Cache` trait.
- **Predicate evaluation**: operates on Parquet row group statistics (min/max). Conservative — unknown types or missing stats assume a match (never incorrectly prunes data).
- **GCS client**: HTTP/1.1 with keep-alive connection pool (per-thread `RefCell<VecDeque>`). Retry with exponential backoff for transient errors (429, 5xx). Per-request timeout via `monoio::time::timeout`.
- **Production hardening**: connection limits, idle timeout, flow control (64MB max in-flight), page corruption detection, cache restore on restart, readiness probe, graceful shutdown, bounded prefetch tasks.

## Dependencies

- Prefer implementing functionality with the standard library or existing workspace dependencies before adding new external crates.
- Keep `Cargo.lock` changes intentional; revert unrelated dependency bumps. Pin broken deps with a comment linking the upstream issue.
- Ensure new dependencies do not introduce known high or critical severity vulnerabilities.

## Testing Guidelines

- **All bugfixes and features must have corresponding tests. Do not merge code without tests.**
- **Write a failing test before fixing a bug** — verify the test captures the failure, then implement the fix. If a test cannot be written first, explain why in the PR.
- Add Rust unit tests alongside implementations via `#[cfg(test)]`; prefer focused scenarios.
- Use `rstest` for tests that differ only in inputs — use `#[case::{name}(...)]` for readable case names.
- Extend existing test modules instead of adding overlapping new ones; group related tests next to the functionality they cover.
- **Tests must be deterministic** — no random seeds without fixed values, no `std::thread::sleep` for synchronization; use explicit conditions, channels, or polling with bounded retries.
- **No mocking libraries.** Write manual test doubles (stub structs that implement the trait) to keep tests simple and encourage testable designs. If a type is hard to test without mocks, that's a signal to refactor the interface.
- Use `assert_eq!`/`assert_ne!` over `assert!(a == b)` — the former prints actual values on failure.
- Assert on both error type and message content — don't just check `is_err()`.
- **If a test fails, assume the code is wrong.** Only update the test expectation after verifying the code is correct. Never reflexively adjust tests to make them pass.
- **`#[ignore]` tests must link a tracking issue** — no bare `#[ignore]` without a `// TODO(#123): reason` explaining when it will be re-enabled.
- Test cache algorithms (eviction, admission, Bloom filters) with deterministic inputs.
- Hash ring tests should verify distribution uniformity and minimal disruption on node changes.
- Frame codec tests should cover round-trip encoding, partial reads, and error cases.
- Membership tests should cover trait implementations (static, etcd) independently.

## Commit & Pull Request Guidelines

### Commit Messages
- **Format**: `type(scope): Description` — imperative mood, capital letter, no trailing period, under 72 characters.
- **Types**: `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `build`, `chore`.
- **Scopes**: `cache`, `gcs`, `protocol`, `cluster`, `server`, `bench`, `config`, or other affected module.
- **Body**: separate from title with blank line. Explain *what* and *why*, not *how*. Assume someone may need to revert your change during an emergency — give them enough context.
- Reference issues when relevant; include benchmark deltas for performance changes.

### Pull Requests
- Keep PRs focused — no drive-by refactors, reformatting, or cosmetic changes. One logical change per PR.
- Every commit in a PR should compile and pass tests independently.
- **Discuss large designs before implementing.** If a change touches 3+ modules or introduces a new abstraction, write up the approach first.
- Include test commands run and results. For performance changes, include before/after benchmark numbers.
- Explain the *why* in the PR description, not just the *what*.
- **All new features, config options, and behavioral changes need user-facing documentation** — not just code-level `///` docs.

### Code Review
- **Reviews are collaborative** — the goal is to help someone land their changes, not to hunt for mistakes. Provide explicit, actionable feedback.
- Focus reviews on correctness, safety, and performance — not style preferences already enforced by `clippy` and `fmt`.
- Verify error handling, thread safety, and bounded resource usage.
- Check that naming, error messages, and test coverage meet the standards in this document.
- Be concise. Focus on P0/P1 issues: bugs, safety violations, performance regressions. Don't reiterate what's already well done.
