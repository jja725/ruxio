# Ruxio: Distributed Parquet Cache

A high-performance distributed cache for Parquet files on cloud storage (GCS), built on io_uring via monoio. Inspired by Alluxio's Dora architecture.

## Key Features

- **Page-level caching (4MB aligned)** — fine-grained caching of Parquet data pages on local NVMe/SSD, not whole files
- **Deserialized metadata cache** — Parquet footer and column indices cached in parsed form for sub-ms metadata lookups (vs 200+ms from GCS)
- **Predicate pushdown at cache layer** — engines send a single Scan RPC with a predicate; ruxio evaluates it against cached row group statistics and streams only matching pages (N+1 round-trips → 1)
- **Direct range-read API** — engines that handle their own predicates can request exact byte ranges
- **io_uring everywhere** — monoio thread-per-core runtime for both disk and network I/O, no tokio
- **Consistent hashing** — stateless routing with virtual nodes; auto-scaling on node join/leave
- **CLOCK-Pro + TinyLFU + Bloom filters** — scan-pollution-resistant eviction, frequency-based admission control, fast cache membership checks
- **Dynamic cluster membership** — extensible `MembershipService` trait with etcd and static backends (Raft planned)
- **Production-grade reliability** — GCS retry with exponential backoff, connection pooling, graceful shutdown, page corruption detection, cache restore on restart

## Architecture

```
  Client ──Binary──> Data Plane   (range reads, predicate scans, page streaming)
  Client ──HTTP/1.1> Data Plane   (range reads — zero-copy sendfile)

  [ruxio-node]  <--consistent hash ring (on file path)-->  [ruxio-node]
       |              etcd / static membership                   |
  NVMe/SSD (4MB page cache, io_uring)                      NVMe/SSD
       |                                                        |
  GCS (custom async client on monoio TCP + TLS)             GCS
```

### Data Flow

**Scan with predicate (1 RPC)**:
```
Engine -> ruxio: SCAN file.parquet WHERE ts > 2024-01-01 AND region = 'US'
  1. Consistent hash -> route to owning node (or Redirect response)
  2. Metadata cache -> get Parquet footer + column stats (sub-ms)
  3. Evaluate predicate against row group min/max -> find matching row groups
  4. For each matching row group:
     - Page cache hit -> read from NVMe via io_uring (zero-copy sendfile)
     - Page cache miss -> fetch from GCS (with retry), cache (if TinyLFU admits), serve
  5. Stream raw Parquet page bytes back to engine
```

**Direct range read**:
```
Engine -> ruxio: READ file.parquet OFFSET=4096 LENGTH=4MB
  -> Page cache hit or GCS fetch -> stream bytes
```

## Project Structure

```
ruxio/
├── common/        ruxio-common    — logging, Prometheus metrics (22 counters), config (TOML + env)
├── protocol/      ruxio-protocol  — wire protocol frame codec, predicate AST, messages
├── storage/       ruxio-storage   — GCS client (retry + pool), page cache (CLOCK-Pro + TinyLFU), cache manager
├── cluster/       ruxio-cluster   — consistent hash ring, membership service (trait + etcd + static)
├── server/        ruxio-server    — monoio binary: data plane + health/metrics endpoints
└── bench/         ruxio-bench     — CLI benchmark tool + criterion micro-benchmarks
```

### Crate Dependency Graph

```
ruxio-common  (no internal deps)
  ├── ruxio-protocol
  ├── ruxio-storage  (GCS client, page cache, retry)
  ├── ruxio-cluster  (hash ring, membership trait, etcd backend)
  ├── ruxio-server   (depends on all above)
  └── ruxio-bench    (depends on all above)
```

## Getting Started

### Prerequisites

- Rust stable toolchain (1.75+)
- Linux kernel 5.6+ (for io_uring; monoio falls back to epoll on older kernels)
- macOS supported for development (epoll fallback)
- etcd 3.x (optional, for dynamic cluster membership)

### Build

```bash
cargo build --release
```

### Test

```bash
cargo test --all
```

### Lint

```bash
cargo clippy --all-targets
cargo fmt --all -- --check
```

## Configuration

Ruxio uses a TOML config file with environment variable overrides. No CLI args needed for most deployments.

### Config file

Create `ruxio_config.toml` (or set `RUXIO_CONFIG` env var to a custom path):

```toml
# ruxio_config.toml

log_level = "info"
data_port = 51234
control_port = 51235        # health + metrics
http_port = 51236            # HTTP/1.1 data plane
threads = 16

# Discovery: "static" or "etcd"
service_discovery_type = "static"
static_service_list = "node1:51234,node2:51234"

# etcd (when service_discovery_type = "etcd")
# etcd_uris = "http://etcd1:2379,http://etcd2:2379"
# etcd_prefix = "/ruxio/nodes/"

[cache]
page_size_bytes = 4194304           # 4MB
max_cache_bytes = 107374182400      # 100GB
max_metadata_entries = 100000
metadata_ttl_secs = 300
root_path = "/mnt/nvme/ruxio"
eviction_policy = "lru"             # or "clockpro"
restore_on_startup = true           # rebuild index from disk on restart

[gcs]
bucket = "my-gcs-bucket"
max_idle_connections = 4            # HTTP/1.1 keep-alive pool per thread
idle_timeout_secs = 60
max_retries = 5
retry_base_delay_ms = 100
retry_max_delay_ms = 5000
request_timeout_secs = 30

[server]
idle_timeout_secs = 300             # close idle connections after 5min
max_inflight_bytes = 67108864       # 64MB flow control per connection
max_connections_per_thread = 10000

[cluster]
vnodes_per_node = 150
etcd_lease_ttl_secs = 10
prefetch_pages = 4                  # sequential read-ahead depth
max_prefetch_tasks = 16             # bound outstanding prefetch tasks
inflight_timeout_secs = 30          # thundering herd wait timeout
```

### Environment variable overrides

All settings can be overridden via `RUXIO_` prefixed env vars. Nested keys use `__` separator:

```bash
export RUXIO_DATA_PORT=51234
export RUXIO_CACHE__PAGE_SIZE_BYTES=8388608
export RUXIO_GCS__BUCKET=my-bucket
export RUXIO_SERVER__MAX_CONNECTIONS_PER_THREAD=20000
```

### Run the server

```bash
# With config file
cargo run --release --bin ruxio-server

# With custom config path
cargo run --release --bin ruxio-server -- --config /etc/ruxio/production

# With env overrides
RUXIO_GCS__BUCKET=my-bucket cargo run --release --bin ruxio-server
```

The server starts three listeners:
- **Binary data plane** on `data_port` (default 51234)
- **HTTP/1.1 data plane** on `http_port` (default 51236) — zero-copy sendfile
- **Health/metrics endpoint** on `control_port` (default 51235)

### Health and observability

```bash
# Liveness check
curl http://localhost:51235/health

# Readiness check (returns 503 during startup/shutdown)
curl http://localhost:51235/ready

# Prometheus metrics (22 production metrics)
curl http://localhost:51235/metrics
```

### Benchmarks

```bash
# Criterion micro-benchmarks
cargo bench

# E2E loopback benchmark (server + client in one process)
cargo run --release --bin ruxio-e2e -- 4 16 10
```

## Production Metrics

Ruxio exposes 22 Prometheus metrics at `/metrics`:

| Category | Metrics |
|----------|---------|
| **Requests** | `incoming_requests`, `connected_clients`, `active_operations`, `response_time_seconds` |
| **Cache** | `cache_hits_total`, `cache_misses_total`, `cache_read_latency_seconds` |
| **Throughput** | `bytes_read_cache_total`, `bytes_read_gcs_total`, `bytes_served_total` |
| **Capacity** | `page_cache_bytes`, `page_cache_capacity_bytes`, `page_cache_pages` |
| **Eviction** | `pages_evicted_total`, `bytes_evicted_total` |
| **Errors** | `cache_put_errors_total`, `cache_get_errors_total`, `cache_delete_errors_total` |
| **Metadata** | `metadata_cache_hits_total`, `metadata_cache_misses_total` |
| **GCS** | `gcs_fetches_total`, `gcs_fetch_latency_seconds`, `gcs_retries_total`, `gcs_timeouts_total` |
| **Cluster** | `cluster_members` |
| **Optimization** | `prefetch_pages_total`, `inflight_coalesced_total` |

## Cluster Membership

Ruxio supports pluggable membership backends via the `MembershipService` trait:

### Static (default)
Fixed peer list in config. Suitable for small clusters with infrequent changes.

### etcd
Lease-based liveness with automatic failure detection. Workers register with a TTL lease; if a worker dies, its lease expires and all other nodes update their hash ring within seconds.

```toml
service_discovery_type = "etcd"
etcd_uris = "http://etcd1:2379,http://etcd2:2379"
etcd_prefix = "/ruxio/nodes/"

[cluster]
etcd_lease_ttl_secs = 10
```

### Future: Raft
The `MembershipService` trait is designed for extensibility. A Raft-based backend can be added by implementing `join`, `leave`, `get_live_members`, and `subscribe`.

## Wire Protocol

### Binary Data Plane (TCP)

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
| Heartbeat | 0x0B | Keepalive | Connection keepalive ping |
| Cancel | 0x0C | Request | Cancel an in-flight request |

### HTTP/1.1 Data Plane (zero-copy sendfile)

Uses `sendfile(2)` to transfer cached page data directly from the OS page cache to the socket.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/read?uri=...&offset=N&length=N` | GET | Read byte range (sendfile on cache hit) |

## Benchmark Results

### Throughput (AMD EPYC 7B13, 32Gbps NIC, 4MB page reads, sendfile zero-copy)

| Connections | Throughput | NIC Utilization |
|-------------|-----------|-----------------|
| 32 | 3.6 GB/s (28.8 Gbps) | 90% |
| 64 | 3.68 GB/s (29.4 Gbps) | 92% |

### Micro-benchmarks

| Component | Operation | Latency | Throughput |
|-----------|-----------|---------|------------|
| Page cache | put | 61 ns | 16.3M ops/s |
| Page cache | get (hit) | 246 ns | 4.1M ops/s |
| Page cache | get (miss) | 373 ns | 2.7M ops/s |
| Bloom filter | lookup | 10 ns | 103M ops/s |
| Hash ring | lookup (10 nodes) | 73 ns | 13.6M ops/s |
| Hash ring | lookup (100 nodes) | 87 ns | 11.5M ops/s |
| Frame codec | encode | 25 ns | 40.5M ops/s |
| Frame codec | decode | 18 ns | 55.9M ops/s |

## Production Readiness

Ruxio includes the following production hardening features, modeled after Alluxio's battle-tested Dora architecture:

- **GCS retry with exponential backoff** — transient errors (429, 5xx) retried automatically with configurable policy
- **HTTP/1.1 connection pooling** — TLS connections reused across requests, idle timeout cleanup
- **Graceful shutdown** — SIGTERM/SIGINT handler, drain in-flight requests, deregister from cluster
- **Connection limits** — configurable max connections per thread, rejects beyond limit
- **Idle timeout** — close connections with no activity (default 5min)
- **Flow control** — 64MB max bytes-in-flight per connection, prevents slow client blocking
- **Page corruption detection** — validates page size on read, auto-removes corrupt entries
- **Cache restore on restart** — scans disk on startup, rebuilds page index without cold-start penalty
- **Prefetch bounds** — max 16 outstanding prefetch tasks per thread, prevents task explosion
- **Readiness probe** — `/ready` returns 503 during startup/shutdown (Kubernetes-compatible)
- **Heartbeat** — keepalive messages prevent premature idle timeout on long-lived connections

## License

Apache-2.0
