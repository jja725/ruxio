# AI Agent Guidelines for Ruxio

This document provides guidelines for AI agents working on the ruxio codebase.

## Quick Reference

| Task | Command |
|------|---------|
| Build | `cargo check` |
| Test | `cargo test --all` |
| Lint | `cargo clippy --all-targets` |
| Format | `cargo fmt --all` |
| Bench | `cargo bench` |
| Run server | `cargo run --bin ruxio-server -- --bucket <bucket>` |

## Crate Map

| Crate | Path | Purpose |
|-------|------|---------|
| ruxio-common | `common/` | Logging, Prometheus metrics, TOML+env settings |
| ruxio-protocol | `protocol/` | Frame codec, predicate AST, request/response messages |
| ruxio-storage | `storage/` | GCS client, metadata cache, page cache (CLOCK-Pro + TinyLFU), cache manager |
| ruxio-cluster | `cluster/` | Consistent hash ring, cluster membership (static/etcd) |
| ruxio-server | `server/` | Binary: monoio data plane + HTTP/2 control plane |
| ruxio-bench | `bench/` | CLI benchmarks + criterion micro-benchmarks |

## Key Design Constraints

1. **No tokio** — the project uses monoio (io_uring) exclusively. Do not add tokio dependencies or use `Send`/`Sync` bounds unless absolutely necessary.
2. **Cache trait** — both `MetadataCache` and `PageCache` implement `storage/src/cache_trait.rs::Cache`. New cache types should implement this trait.
3. **Thread-per-core** — each monoio thread owns its data. No cross-thread sharing, no `Arc<Mutex<>>` for cache state.
4. **Conservative predicate evaluation** — when evaluating predicates against Parquet stats, always assume a match if uncertain. Never incorrectly prune data.
5. **Raw Parquet pages** — cached data is stored as raw Parquet bytes, no format transcoding. Pages are 4MB aligned.

## Common Tasks

### Adding a new message type
1. Add variant to `MessageType` enum in `protocol/src/frame.rs`
2. Add request/response struct in `protocol/src/messages.rs`
3. Handle in `server/src/data.rs::process_frame()`

### Adding a new cache backend
1. Implement `Cache` trait from `storage/src/cache_trait.rs`
2. Wire into `CacheManager` in `storage/src/cache.rs`

### Adding a new cloud storage provider
1. Create a new module in `storage/src/` (e.g., `s3.rs`)
2. Implement the same async methods as `GcsClient`: `get_range()`, `head()`, `list()`
3. Use monoio `TcpStream` + TLS, not tokio-based HTTP clients

### Adding a cluster feature
1. Extend `ClusterMembership` in `cluster/src/membership.rs`
2. Add control plane endpoint in `server/src/control.rs`

## Testing Patterns

- **Page cache**: test with deterministic keys, verify CLOCK-Pro promotion/demotion and TinyLFU rejection
- **Hash ring**: verify even distribution across N nodes with >1000 keys, verify minimal disruption on node add/remove
- **Frame codec**: test encode→decode round-trip, partial buffer feeds, multi-frame streams, unknown message types
- **Metadata cache**: test LRU eviction order, TTL staleness

## Files to Read First

1. `storage/src/cache_trait.rs` — the unified KV interface
2. `storage/src/cache.rs` — the main orchestrator (read_range + scan flows)
3. `protocol/src/frame.rs` — wire protocol
4. `cluster/src/ring.rs` — consistent hashing
5. `server/src/data.rs` — request handling
