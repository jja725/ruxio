# Repository Guidelines

## Project Structure

- `common/` — shared utilities: logging, metrics (`metrics.rs`), config (`settings.rs`).
- `protocol/` — wire protocol: frame codec (`frame.rs`), request/response types (`messages.rs`), predicate AST (`predicate.rs`).
- `storage/` — core crate: `Cache` trait (`cache_trait.rs`), page cache (`page_cache.rs`), metadata cache (`metadata_cache.rs`), GCS client (`gcs.rs`), retry policy (`retry.rs`), error types (`error.rs`).
- `cluster/` — distributed state: `MembershipService` trait (`service.rs`), consistent hash ring (`ring.rs`), etcd backend (`etcd.rs`), static membership (`static_membership.rs`).
- `server/` — binary: data plane (`data.rs`), HTTP plane (`http.rs`), control plane (`control.rs`).
- `bench/` — CLI benchmark tool + criterion micro-benchmarks.

## Build & Test

```
cargo check                     # type-check all crates
cargo test --all                # run all unit tests
cargo bench                     # criterion micro-benchmarks
cargo clippy --all-targets      # lint (fix all warnings before committing)
cargo fmt --all                 # format
cargo run --bin ruxio-server    # run server (reads ruxio_config.toml)
```

## Configuration

All tunables live in `common/src/settings.rs` as TOML + env vars. No CLI args for tunables — only `--config` and `--bench-populate`.

- **Config file**: `ruxio_config.toml` (or `RUXIO_CONFIG` env var, or `--config` CLI arg).
- **Env overrides**: `RUXIO_` prefix, `__` separator (e.g., `RUXIO_CACHE__PAGE_SIZE_BYTES`).

## Architecture

- **monoio thread-per-core**: no `Send`/`Sync` for most types. Each core owns its cache partition. `Rc<RefCell<>>` intra-thread, `Arc<AtomicBool/AtomicU32>` cross-thread.
- **No tokio on hot path**: all data plane I/O through monoio's io_uring. Tokio only in etcd background thread (`cluster/src/etcd.rs`), communicating via `std::sync::mpsc`.
- **Page cache**: CLOCK-Pro + TinyLFU + Bloom filters. Internal to `PageCache` — callers use the `Cache` trait.
- **Predicate evaluation**: conservative on Parquet row group stats (min/max). Unknown types or missing stats assume a match.
- **GCS client**: HTTP/1.1 keep-alive pool, exponential backoff retry for transient errors (429, 5xx).
- **Production hardening**: connection limits, idle timeout, flow control (64MB in-flight), corruption detection, cache restore, readiness probe, graceful shutdown.

## Testing (Project-Specific)

- Use `rstest` for parameterized tests with `#[case::{name}(...)]`.
- Test cache algorithms (eviction, admission, Bloom filters) with deterministic inputs.
- Hash ring tests: verify distribution uniformity and minimal disruption on node changes.
- Frame codec tests: round-trip encoding, partial reads, error cases.

---

## General Rust Conventions

The following conventions apply broadly to Rust projects.

### Naming
- **No abbreviations** except established domain terms (`rpc`, `ttl`, `io`, `id`). `position_count` not `pos_cnt`.
- **Avoid generic `get_` prefixes** — use `find_` (lookup, may fail), `fetch_` (remote/expensive), `compute_` (derived), `load_` (from disk). Reserve `get_` for trivial field accessors.
- **Avoid generic module names** like `utils.rs`, `helpers.rs` — name after the concept.
- Prefix booleans with `is_`/`has_`; default to `false`. Use `disable_*` instead of `enable_*` when the feature defaults to on.
- Use **named constants** for magic numbers. No bare `4096` or `"default"` in logic.
- Use **type aliases** for domain concepts at API boundaries: `type PageId = u64;`.

### Organization
- Arrange methods in reading order: public API first, then private helpers in call order.
- **Never mix reformatting with logic changes** — separate commits/PRs.
- When multiple function parameters share the same type, add comments at call sites: `send(/* host= */ addr, /* port= */ 8080)`.

### Control Flow
- When advancing multiple iterators in lockstep, advance *all* before any `continue` or conditional branch — an early `continue` that skips a `.next()` causes silent misalignment.
- **Avoid iterator chains in hot loops** — use explicit `for` loops for clarity and control in performance-sensitive sections.
- **No wildcard `_ =>` on owned enums.** Use exhaustive patterns so the compiler catches missing variants. Use `_ =>` only for external enums you don't control.

### API Design
- **Eagerly validate in constructors.** Fail fast at construction, not deep in the call stack.
- **Never expose internals for testing.** Design testable interfaces; don't add `pub` or `#[cfg(test)]` accessors for private state.
- Use stable, versioned serialization formats for persistent data.
- **Gate risky new features behind config flags** until proven stable in production.
- Replace mutually exclusive boolean flags with a single enum/mode parameter.

### Error Handling
- Use `snafu` for all non-test error types. Use `anyhow` only in tests.
- **Every error variant must carry `#[snafu(implicit)] location: Location`** for automatic file/line tracking (Rust's equivalent of Java stack traces). Use `#[snafu(visibility(pub))]` to generate public context selectors.
- **Use snafu context selectors** instead of direct construction: `.context(ConnectionSnafu { detail: "..." })?` for Result chains, `PageAssemblySnafu { detail }.fail()` for returning errors, `GcsSnafu.into_error(source)` for wrapping sources.
- **Match error variants to root causes** with specific variants for monitoring — not a generic catch-all.
- Include full context in error messages: values, sizes, types. `"Page offset {} exceeds file size {}"` not `"Invalid offset"`.
- **No silent `let _ =` on `Result`** — log at `debug`/`warn` or increment a metric.
- Use `checked_add`/`checked_mul` for counters and offsets; return errors on overflow.
- Prefer `debug_assert!` for internal invariants; reserve `assert!` for data corruption prevention.
- Log warnings for best-effort/cleanup failures rather than silently swallowing.
- **Don't log a warning then immediately return the same error.** Log only for conditions that are *handled* and won't propagate.
- **Don't silently work around infrastructure bugs.** Report root causes and fix properly.

### Memory & Safety
- Prefer memory-efficient data structures for large collections (`RoaringBitmap` over `HashSet<u32>`, `SmallVec` for small-N, `Cow<str>` for mostly-borrowed). Avoid collecting streams into `Vec` when you can process iteratively.
- **Every `unsafe` block must have a `// SAFETY:` comment.** No exceptions.
- **No unbounded data structures** in performance-sensitive paths.

### Testing
- **All bugfixes and features must have tests. Do not merge without tests.**
- **Write a failing test before fixing a bug.**
- **Tests must be deterministic** — no random seeds without fixed values, no `std::thread::sleep` for synchronization.
- **No mocking libraries.** Write manual test doubles. If a type is hard to test without mocks, refactor the interface.
- Assert on both error type and message content — don't just check `is_err()`.
- **If a test fails, assume the code is wrong.** Only update expectations after verifying correctness.
- **`#[ignore]` tests must link a tracking issue.**

### Commits & PRs
- **Commit format**: `type(scope): Description` — imperative, under 72 chars. Body explains *what* and *why*.
- **Types**: `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `build`, `chore`.
- One logical change per PR. Every commit must compile and pass tests.
- **Discuss large designs before implementing** — write up the approach first.
- Include before/after benchmark numbers for performance changes.
- **All new features and config options need user-facing documentation.**

### Code Review
- **Reviews are collaborative** — help someone land their changes, don't hunt for mistakes.
- Focus on correctness, safety, and performance — not style enforced by `clippy`/`fmt`.
- Be concise. Focus on bugs, safety violations, performance regressions.
