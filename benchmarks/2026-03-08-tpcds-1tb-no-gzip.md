# Benchmark: TPC-DS 1TB Snapshot — No Gzip, 16 vCPU

**Date**: 2026-03-08
**Job**: job ID 88
**Status**: Running (partial results at time of recording)

## Infrastructure

| Resource | Value |
|----------|-------|
| EC2 Instance | 16 vCPU (c5.4xlarge or equivalent) |
| vCPUs | 16 |
| Source DB | PostgreSQL RDS db.r6i.2xlarge (8 vCPU, 64 GB RAM) |
| Sink | StarRocks (self-hosted EC2) |
| Region | us-west-2 |
| DBMazz Version | Post-gzip removal |

## Job Configuration

| Setting | Value |
|---------|-------|
| Chunk Size | 50,000 rows |
| Snapshot Workers | 20 (concurrent) |
| Flush Size | 10,000 msgs |
| Flush Interval | 5,000 ms |
| Tables | 25 (full TPC-DS schema) |
| Gzip Compression | **Disabled** (removed in commit `dc9e186`) |

## Key Change vs Previous Test

Gzip compression (`flate2::Compression::fast()`) was removed from `StreamLoadClient`. On the previous 8 vCPU test, gzip was identified as the CPU bottleneck on wide tables. This test validates performance without gzip on a larger instance.

## Dataset

Same TPC-DS 1TB dataset as previous test (~6.35B rows, 25 tables, 113,129 chunks total).

## Performance Results

### Phase 1: Narrow Tables (catalog_returns — 27 columns, 2,880 chunks)

| Metric | Value |
|--------|-------|
| Throughput | **~93-95K rows/sec** |
| CPU Usage | 87-97% sustained |
| Duration | ~25 min |
| Chunks completed | 2,880 |
| Behavior | CPU-bound, all 20 workers active |

### Phase 2: Wide Tables (catalog_sales — 34 columns, 28,801 chunks)

| Metric | Value |
|--------|-------|
| Throughput | **~14.8K rows/sec** |
| CPU Usage | 2-3% (!) |
| Chunks completed | ~4,185 (at time of recording) |
| Chunk duration | ~80 seconds each |
| Behavior | I/O-bound on PostgreSQL |

### Phase Transition

CPU dropped from ~97% to near-zero at ~15:06 UTC — exactly when processing moved from `catalog_returns` to `catalog_sales`. A brief idle gap (~2 min) was observed between tables due to the **producer persistence gap** (Known Issue #1: producer persists all chunks for a table before sending to the channel).

## Bottleneck Analysis

### Why Wide Tables Are 6x Slower

**Root cause: PostgreSQL RDS I/O contention**

Evidence at time of observation:
- **23 PostgreSQL connections** active (20 workers + overhead)
- **0 StarRocks connections** at that moment
- **Only 2 HTTP connections** to StarRocks (most workers waiting on PG SELECT)
- CPU at 2-3% on EC2 — workers are idle, waiting for PG responses

Each chunk lifecycle on `catalog_sales`:
1. `SELECT * FROM catalog_sales WHERE pk BETWEEN x AND y` → **~75-78s** (bottleneck)
2. JSON serialization → <1s
3. HTTP Stream Load to StarRocks → ~2-3s

20 concurrent SELECT queries against an 8 vCPU RDS instance with 34-column rows creates massive I/O contention. The data is too large for `shared_buffers`, so every chunk hits disk.

### Narrow vs Wide Table Comparison

| Metric | catalog_returns (27 cols) | catalog_sales (34 cols) |
|--------|--------------------------|------------------------|
| Rows/sec | ~93-95K | ~14.8K |
| CPU % | 87-97% | 2-3% |
| Bottleneck | CPU (serialization) | PostgreSQL I/O |
| Chunks | 2,880 | 28,801 |
| Total rows | 144M | 1.44B |

## Key Findings

1. **Removing gzip did NOT improve narrow-table throughput**: 93K rows/sec vs 110K on previous test (8 vCPU). Actually slightly lower — likely because the previous test benefited from gzip reducing Stream Load transfer time on narrow tables.

2. **Gzip removal validated for wide tables**: Without gzip CPU overhead, the true bottleneck is visible — PostgreSQL read I/O, not EC2 compute.

3. **16 vCPU underutilized**: The extra cores (vs 8 vCPU) provide no benefit when the bottleneck is RDS. CPU sits at 2-3% during wide table processing.

4. **Worker count matters for RDS contention**: 20 workers × concurrent SELECTs overwhelm an 8 vCPU RDS. Reducing workers may actually **increase** per-chunk throughput by reducing contention.

5. **StarRocks is NOT the bottleneck**: Near-zero connections to StarRocks at any time. Stream Load handles 50K-row batches in ~2-3 seconds.

## Competitive Context

| Solution | Throughput | Notes |
|----------|-----------|-------|
| Debezium | ~7-10K rows/sec | WAL-only, no parallel snapshot |
| Airbyte | ~10K rows/sec | JDBC-based, single-threaded |
| Fivetran | ~50-100K rows/sec | Per-connector, proprietary |
| **EZ-CDC (narrow)** | **~93K rows/sec** | 16 vCPU, 20 workers |
| PeerDB | ~300 MB/sec | 32 threads, COPY-based, strongest competitor |

EZ-CDC is 9-13x faster than open-source alternatives on narrow tables. Wide tables need optimization (see recommendations).

---

## Test B: 12 Workers, 200K Chunk Size (Job 89)

Based on Test A findings, ran a second test reducing workers and increasing chunk size.

### Configuration

| Setting | Value |
|---------|-------|
| Chunk Size | 200,000 rows |
| Snapshot Workers | 12 (concurrent) |
| Total Chunks | 28,283 (vs 113,129 with 50K chunks) |

### Timing

| Phase | Duration | Notes |
|-------|----------|-------|
| Chunking (all 25 tables) | ~87s | vs ~2m53s with 50K chunks (3.7x faster) |
| catalog_returns (720 chunks) | ~22 min | 19:45:05 → 20:07:26 |
| Producer persistence gap | ~3 min | 20:07:26 → 20:10:26 (Known Issue #1) |
| catalog_sales (7,201 chunks) | ongoing | started 20:10:26 |

### catalog_returns (27 cols) — 12 workers / 200K chunks

| Metric | Value |
|--------|-------|
| Throughput | **~97-107K rows/sec** |
| CPU Usage | 60-62% |
| CPU warnings | Yes (evaluator: "CPU usage too high during snapshot: 62.34%") |
| Chunks completed | 720 / 720 |

### catalog_sales (34 cols) — 12 workers / 200K chunks

| Metric | Value |
|--------|-------|
| Throughput (cache warmup, chunks 0-10) | ~100K rows/sec |
| Throughput (stabilized, chunks 23-49) | **~21.8K rows/sec** |
| CPU Usage | 7-8% |
| Notable gaps | ~2 min stalls between some chunks (PG buffer eviction) |

### Comparison: Test A vs Test B

| Metric | Test A (20w/50K) | Test B (12w/200K) | Change |
|--------|------------------|-------------------|--------|
| catalog_returns rows/sec | ~93-95K | ~97-107K | +10-13% |
| catalog_returns CPU | 87-97% | 60-62% | -30% |
| catalog_sales rows/sec | ~14.8K | ~21.8K | **+47%** |
| catalog_sales CPU | 2-3% | 7-8% | +5% |
| Efficiency (rows/sec/vCPU) | 5.8K | 6.7K | +15% |
| Chunking time | 2m53s | 87s | 2x faster |

### Key Findings from Test B

1. **Worker reduction validated**: 12 workers outperform 20 on wide tables by 47%. Less RDS contention = faster per-query I/O.

2. **CPU efficiency improved dramatically**: Same throughput on narrow tables with 60% CPU vs 97%. Leaves headroom for other processes.

3. **Wide tables still I/O-bound**: Even with fewer workers, catalog_sales throughput (21.8K rows/sec) is still 4-5x slower than narrow tables. RDS db.r6i.2xlarge (8 vCPU, 64GB) is the hard ceiling.

4. **PG buffer eviction visible**: ~2 min gaps between some catalog_sales chunks suggest data exceeds shared_buffers, forcing disk reads.

5. **Producer persistence gap persists**: ~3 min idle between tables (Known Issue #1). Not affected by worker/chunk config.

## Future Optimizations to Explore

1. **PostgreSQL COPY instead of SELECT**: Binary COPY is significantly faster for bulk reads. PeerDB uses this approach.
2. **Adaptive worker scaling**: Auto-reduce workers when CPU is low (I/O bound signal).
3. **RDS instance sizing**: Test with db.r6i.4xlarge (16 vCPU) to match EC2. Should improve wide table throughput significantly.
4. **Re-enable gzip selectively**: Only for narrow tables where CPU headroom exists (60% in Test B leaves plenty of room).
5. **Fix producer persistence gap**: Stream chunks to workers as they're computed instead of persisting all first.

## Previous Test Results

| Test | Date | Workers | Chunk Size | Instance | Gzip | Result |
|------|------|---------|------------|----------|------|--------|
| Baseline | 2026-03-06 | 5 | 50,000 | 8 vCPU | Yes | ~40K rows/sec |
| Optimized (gzip+cache+escape) | 2026-03-07 | 25 | 50,000 | 8 vCPU | Yes | ~110K rows/sec |
| Test A (no gzip) | 2026-03-08 | 20 | 50,000 | 16 vCPU | No | ~93K narrow / ~14.8K wide |
| **Test B (fewer workers)** | **2026-03-08** | **12** | **200,000** | **16 vCPU** | **No** | **~107K narrow / ~21.8K wide** |
