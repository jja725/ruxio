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

## Coding Style & Naming Conventions

- Format with `cargo fmt --all`; keep modules and functions `snake_case`, types `PascalCase`.
- Run `cargo clippy --all-targets` to catch lint regressions.
- Use `thiserror` for error types in libraries, `anyhow` for binaries and tests.
- Prefer `tracing` over `log` for new code; existing `log`/`fern` usage in common is acceptable.
- Keep the `Cache` trait in `storage/src/cache_trait.rs` as the unified KV interface.
- **No `.unwrap()` in production code** — use proper error handling with `match`, `if let`, or `?`. Reserve `unwrap` for tests only.
- **No silent `let _ =` on `Result`** — log at `debug` or `warn` level, or increment an error metric.
- Membership backends implement `MembershipService` trait in `cluster/src/service.rs`.

## Architecture Notes

- **monoio thread-per-core**: no `Send`/`Sync` required for most types. Each core owns its own cache partition. `Rc<RefCell<>>` for intra-thread, `Arc<AtomicBool/AtomicU32>` for cross-thread flags.
- **No tokio on hot path**: all data plane I/O goes through monoio's io_uring backend. The only tokio usage is in the etcd background thread (`cluster/src/etcd.rs`), communicating via `std::sync::mpsc`.
- **Page cache internals**: CLOCK-Pro (hot/cold/test lists) for eviction, Count-Min Sketch (TinyLFU) for admission control, per-file Bloom filters for fast negative lookups. These are internal to `PageCache` — callers use the simple `Cache` trait.
- **Predicate evaluation**: operates on Parquet row group statistics (min/max). Conservative — unknown types or missing stats assume a match (never incorrectly prunes data).
- **GCS client**: HTTP/1.1 with keep-alive connection pool (per-thread `RefCell<VecDeque>`). Retry with exponential backoff for transient errors (429, 5xx). Per-request timeout via `monoio::time::timeout`.
- **Production hardening**: connection limits, idle timeout, flow control (64MB max in-flight), page corruption detection, cache restore on restart, readiness probe, graceful shutdown, bounded prefetch tasks.

## Testing Guidelines

- Add Rust unit tests alongside implementations via `#[cfg(test)]`; prefer focused scenarios.
- Test cache algorithms (eviction, admission, Bloom filters) with deterministic inputs.
- Hash ring tests should verify distribution uniformity and minimal disruption on node changes.
- Frame codec tests should cover round-trip encoding, partial reads, and error cases.
- Membership tests should cover trait implementations (static, etcd) independently.

## Commit & Pull Request Guidelines

- Follow existing history style: imperative, concise subjects under 72 characters.
- Group related changes in a single commit; separate unrelated changes.
- Include test commands run and benchmark deltas when applicable.
- Reference issues when relevant; include brief context in the commit body.
