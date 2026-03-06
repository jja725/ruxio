# Repository Guidelines

## Project Structure & Module Organization

- `common/src/` hosts shared utilities (logging, metrics, settings); keep cross-cutting concerns here.
- `protocol/src/` defines the wire protocol; `frame.rs` for codec, `messages.rs` for request/response types, `predicate.rs` for the predicate AST.
- `storage/src/` is the core crate; `cache_trait.rs` defines the KV interface, `page_cache.rs` implements CLOCK-Pro + TinyLFU + Bloom filters, `metadata_cache.rs` handles Parquet footer caching, `gcs.rs` is the custom GCS client, and `cache.rs` orchestrates everything.
- `cluster/src/` manages distributed state; `ring.rs` for consistent hashing, `membership.rs` for node discovery.
- `server/src/` is the binary entrypoint; `data.rs` handles the binary data plane, `control.rs` handles the HTTP/2 control plane.
- `bench/src/` contains the CLI benchmark tool; `bench/benches/` contains criterion micro-benchmarks.

## Build, Test, and Development Commands

- `cargo check` — fast type-checking across all crates.
- `cargo test --all` — run all unit tests across the workspace.
- `cargo bench` — run criterion micro-benchmarks (page_cache, hash_ring, frame_codec).
- `cargo clippy --all-targets` — lint all crates; fix warnings before committing.
- `cargo fmt --all` — format all Rust code.
- `cargo run --bin ruxio-server -- --bucket <bucket> --cache-dir /tmp/ruxio_cache` — run the server.
- `cargo run --bin ruxio-bench -- scan --server localhost:8081 --uri <uri>` — run benchmarks against a live server.

## Coding Style & Naming Conventions

- Format with `cargo fmt --all`; keep modules and functions `snake_case`, types `PascalCase`.
- Run `cargo clippy --all-targets` to catch lint regressions.
- Use `thiserror` for error types in libraries, `anyhow` for binaries and tests.
- Prefer `tracing` over `log` for new code; existing `log`/`fern` usage in common is acceptable.
- Keep the `Cache` trait in `storage/src/cache_trait.rs` as the unified KV interface — both `MetadataCache` and `PageCache` implement it.

## Architecture Notes

- **monoio thread-per-core**: no `Send`/`Sync` required for most types. Each core owns its own cache partition.
- **No tokio**: all I/O goes through monoio's io_uring backend. The custom GCS client, TCP listeners, and disk I/O all use monoio.
- **Page cache internals**: CLOCK-Pro (hot/cold/test lists) for eviction, Count-Min Sketch (TinyLFU) for admission control, per-file Bloom filters for fast negative lookups. These are internal to `PageCache` — callers use the simple `Cache` trait.
- **Predicate evaluation**: operates on Parquet row group statistics (min/max). Conservative — unknown types or missing stats assume a match (never incorrectly prunes data).

## Testing Guidelines

- Add Rust unit tests alongside implementations via `#[cfg(test)]`; prefer focused scenarios.
- Test cache algorithms (eviction, admission, Bloom filters) with deterministic inputs.
- Hash ring tests should verify distribution uniformity and minimal disruption on node changes.
- Frame codec tests should cover round-trip encoding, partial reads, and error cases.

## Commit & Pull Request Guidelines

- Follow existing history style: imperative, concise subjects under 72 characters.
- Group related changes in a single commit; separate unrelated changes.
- Include test commands run and benchmark deltas when applicable.
- Reference issues when relevant; include brief context in the commit body.
