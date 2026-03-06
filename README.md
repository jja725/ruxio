# Ruxio: Distributed Parquet Cache

A high-performance distributed cache for Parquet files on cloud storage (GCS), built on io_uring via monoio.

## Key Features

- **Page-level caching (4MB aligned)** — fine-grained caching of Parquet data pages on local NVMe/SSD, not whole files
- **Deserialized metadata cache** — Parquet footer and column indices cached in parsed form for sub-ms metadata lookups (vs 200+ms from GCS)
- **Predicate pushdown at cache layer** — engines send a single Scan RPC with a predicate; ruxio evaluates it against cached row group statistics and streams only matching pages (N+1 round-trips → 1)
- **Direct range-read API** — engines that handle their own predicates can request exact byte ranges
- **io_uring everywhere** — monoio thread-per-core runtime for both disk and network I/O, no tokio
- **Consistent hashing** — stateless routing with virtual nodes; auto-scaling on node join/leave
- **CLOCK-Pro + TinyLFU + Bloom filters** — scan-pollution-resistant eviction, frequency-based admission control, fast cache membership checks

## Architecture

```
  Client ──HTTP/2──> Control Plane (cluster ops, health, metadata)
  Client ──Binary──> Data Plane   (range reads, predicate scans, page streaming)

  [ruxio-node]  <--consistent hash ring (on file path)-->  [ruxio-node]
       |                                                        |
  NVMe/SSD (4MB page cache, io_uring)                      NVMe/SSD
       |                                                        |
  GCS (custom async client on monoio TCP + TLS)             GCS
```

### Data Flow

**Scan with predicate (1 RPC)**:
```
Engine → ruxio: SCAN file.parquet WHERE ts > 2024-01-01 AND region = 'US'
  1. Consistent hash → route to owning node
  2. Metadata cache → get Parquet footer + column stats (sub-ms)
  3. Evaluate predicate against row group min/max → find matching row groups
  4. For each matching row group:
     - Page cache hit → read from NVMe via io_uring
     - Page cache miss → fetch from GCS, cache (if TinyLFU admits), serve
  5. Stream raw Parquet page bytes back to engine
```

**Direct range read**:
```
Engine → ruxio: READ file.parquet OFFSET=4096 LENGTH=4MB
  → Page cache hit or GCS fetch → stream bytes
```

## Project Structure

```
ruxio/
├── common/        ruxio-common    — logging, Prometheus metrics, settings
├── protocol/      ruxio-protocol  — wire protocol frame codec, predicate AST, messages
├── storage/       ruxio-storage   — GCS client, metadata cache, page cache (CLOCK-Pro + TinyLFU), cache manager
├── cluster/       ruxio-cluster   — consistent hash ring, cluster membership
├── server/        ruxio-server    — monoio binary: data plane + control plane listeners
└── bench/         ruxio-bench     — CLI benchmark tool + criterion micro-benchmarks
```

### Crate Dependency Graph

```
ruxio-common  (no internal deps)
  ├── ruxio-protocol
  ├── ruxio-storage
  ├── ruxio-cluster
  ├── ruxio-server   (depends on all above)
  └── ruxio-bench    (depends on all above)
```

## Getting Started

### Prerequisites

- Rust stable toolchain (1.75+)
- Linux kernel 5.6+ (for io_uring; monoio falls back to epoll on older kernels)
- macOS supported for development (epoll fallback)

### Build

```bash
cargo build
```

### Test

```bash
cargo test --all
```

### Run the server

```bash
cargo run --bin ruxio-server -- \
  --bucket my-gcs-bucket \
  --cache-dir /mnt/nvme/ruxio \
  --max-cache-bytes 107374182400 \
  --data-port 8081 \
  --control-port 8080
```

### Benchmarks

```bash
# Criterion micro-benchmarks
cargo bench

# CLI benchmark tool (requires running server)
cargo run --bin ruxio-bench -- scan \
  --server localhost:8081 \
  --uri gs://bucket/file.parquet \
  --iterations 1000
```

## Wire Protocol

### Data Plane (custom binary, TCP)

Frame format:
```
[4B length (big-endian)] [1B message type] [4B request ID] [payload]
```

| Type | Code | Direction | Description |
|------|------|-----------|-------------|
| ReadRange | 0x01 | Request | Read a byte range |
| BatchRead | 0x02 | Request | Read multiple ranges in one round-trip |
| DataChunk | 0x03 | Response | Raw Parquet page bytes |
| Error | 0x04 | Response | Error with code + message |
| Redirect | 0x05 | Response | Redirect to correct node (hash miss) |
| Done | 0x06 | Response | All data sent for this request |
| GetMetadata | 0x07 | Request | Get cached Parquet footer |
| Metadata | 0x08 | Response | Footer metadata |
| Scan | 0x09 | Request | Predicate pushdown scan |
| BatchScan | 0x0A | Request | Batch predicate scan |

### Control Plane (HTTP/2)

REST-like JSON API for cluster operations:
- `GET /health` — health check
- `GET /cluster/state` — ring state and node list
- `POST /cluster/join` — node join notification
- `POST /cache/evict` — evict cached data

## License

Apache-2.0
