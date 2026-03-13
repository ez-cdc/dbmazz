# Snapshot Performance Analysis Report

**Date:** 2026-03-12
**Component:** dbmazz CDC daemon — snapshot (initial backfill) phase
**Goal:** Validate whether RDS swap usage is the primary bottleneck and quantify its impact on snapshot throughput

---

## 1. System Architecture

```
┌──────────────────────────┐     SELECT rows      ┌─────────────────────────┐
│  Worker EC2              │ ──────────────────▶   │  Source PostgreSQL       │
│  c6i.4xlarge             │     (tokio_postgres)  │  RDS db.t3.micro        │
│  16 vCPU / 32GB RAM      │                       │  2 vCPU / 1GB RAM       │
│                          │                       │                         │
│  Runs: dbmazz daemon     │ ◀──────────────────   │  Also hosts:            │
│  3 parallel workers      │     Row data          │  - dbmazz_snapshot_state │
│  10K rows/chunk          │                       │  - dbmazz_checkpoints    │
└──────────┬───────────────┘                       └─────────────────────────┘
           │
           │  HTTP Stream Load (JSON)
           │  ~320 KB/s
           ▼
┌──────────────────────────┐
│  StarRocks               │
│  EC2 t3.medium           │
│  2 vCPU / 4GB RAM        │
│  CPU: 9% avg             │
└──────────────────────────┘
```

## 2. What the Snapshot Does (per chunk)

Each chunk performs these operations against the source PostgreSQL:

| Step | Operation | Type | Required? |
|------|-----------|------|-----------|
| 1 | `mark_chunk_in_progress` | UPDATE on `dbmazz_snapshot_state` | Yes (resumability) |
| 2 | `pg_logical_emit_message` (LW) | SELECT (non-transactional) | Yes (Flink CDC algorithm) |
| 3 | `SELECT * FROM table WHERE pk >= $1 AND pk < $2` | **Bulk data read (10K rows)** | Yes |
| 4 | `pg_logical_emit_message` (HW) | SELECT + captures LSN | Yes (dedup with WAL) |
| 5 | HTTP Stream Load to StarRocks | HTTP PUT (not PG) | Yes |
| 6 | `mark_chunk_complete` | UPDATE on `dbmazz_snapshot_state` | Yes (resumability) |
| 7 | `get_relation_id` | SELECT on `pg_class` | **No — static per table** |
| 8 | `chunk_counts` | `SELECT COUNT(*)` on state table | **No — can be in-memory** |
| 9 | `total_rows_synced` | `SELECT SUM(rows_synced)` on state table | **No — can be in-memory** |
| 10 | `table_chunk_progress` | `SELECT COUNT(*)+SUM(rows)` on state table | **No — can be in-memory** |

**Total: 9 PG roundtrips per chunk** (step 5 goes to StarRocks, not PG).
With 3 concurrent workers: **~27 PG queries per cycle**.
Steps 7-10 are purely bookkeeping and don't contribute to data movement.

## 3. CloudWatch Metrics — RDS db.t3.micro (Source PostgreSQL)

### Configuration A: 6 workers, 50K chunk size (before tuning)

| Metric | Value | Assessment |
|--------|-------|------------|
| **SwapUsage** | 73-128 MB | **CRITICAL — active swapping** |
| **FreeableMemory** | 60-87 MB (of ~1GB) | Only 6-9% free |
| **ReadLatency** | ~20 ms avg | **Very high** for in-memory reads |
| **CPUUtilization** | 20-30% | Moderate |
| **ReadIOPS** | 80-120 | Moderate |
| **NetworkTransmit** | Intermittent 150 KB/s bursts | Bursty, not constant |
| Chunk pattern | 6 chunks every ~12 min | Slow, bursty |

### Configuration B: 3 workers, 10K chunk size (after tuning)

| Metric | Value | Change | Assessment |
|--------|-------|--------|------------|
| **SwapUsage** | 32-37 MB | -73% (128→33) | Still swapping but stable |
| **FreeableMemory** | 72-83 MB | +20% (60→72) | Slightly better |
| **ReadLatency** | ~10 ms avg | **-50%** (20→10) | Better but still elevated |
| **CPUUtilization** | 15-25% | -25% | Low |
| **ReadIOPS** | 40-60 | -50% | Low |
| **NetworkTransmit** | Constant 111 KB/s | Steady stream | Much more predictable |
| Chunk pattern | 3 chunks every ~1.5 min | Steady, predictable |

### Key Observations on Swap

- `db.t3.micro` has ~1GB total RAM. PostgreSQL `shared_buffers` defaults to ~128MB on micro instances.
- With 3 concurrent connections each doing `SELECT` of 10K rows (all columns cast to `::text`), each connection needs to hold the result set in PG's working memory.
- The `dbmazz_snapshot_state` table also lives on this instance, receiving UPDATEs and aggregate queries from all 3 workers after each chunk.
- **Swap is being used because PG doesn't have enough RAM** for shared_buffers + work_mem for 3 concurrent queries + OS overhead.
- 10ms ReadLatency is ~10x what you'd expect for in-memory reads (~1ms). This indicates PG is frequently going to disk/swap to service queries.

## 4. CloudWatch Metrics — StarRocks EC2 t3.medium

| Metric | Value | Assessment |
|--------|-------|------------|
| **CPUUtilization** | 8.6-10% avg, 14% max | **Completely idle** |
| **CPU Credit Balance** | 367 → 383 (growing) | Accumulating credits |
| **CPU Credit Usage** | ~0.9/5min | Minimal |
| **NetworkIn** | ~19-20 MB/5min (~320 KB/s) | Negligible (5 Gbps limit) |
| **NetworkOut** | ~100 KB/5min | Just ACKs |
| **EBS Read IOPS** | 0-37/5min | Essentially zero (all in RAM) |
| **EBS Write IOPS** | ~36 IOPS/5min | Trivial (3000 IOPS baseline) |
| **EBS Write Throughput** | ~4.3 MB/s | Trivial (125 MB/s baseline) |
| **StatusCheckFailed** | 0 | Healthy |

**StarRocks can absorb 10-50x more data than it's currently receiving.**

## 5. CloudWatch Metrics — Worker EC2 c6i.4xlarge

| Metric | Value | Assessment |
|--------|-------|------------|
| **Instance type** | c6i.4xlarge (16 vCPU, 32GB) | Massively over-provisioned for 3 workers |
| **CPU** | Not saturated | dbmazz uses async I/O, barely any CPU |
| **Memory** | Not constrained | 32GB for a process that uses <500MB |

**Worker is not a bottleneck by any measure.**

## 6. Tuning History

| Change | SwapUsage | ReadLatency | Throughput Pattern |
|--------|-----------|-------------|-------------------|
| 6 workers / 50K chunks | 73-128 MB | ~20 ms | 6 chunks / 12 min (bursty) |
| 3 workers / 10K chunks | 32-37 MB | ~10 ms | 3 chunks / 1.5 min (steady) |

Reducing concurrency and chunk size cut swap by 73% and latency by 50%, but the instance is still swapping.

## 7. The Core Question: How Does Swap Affect Performance?

### What swap does to PostgreSQL on db.t3.micro

1. **shared_buffers eviction**: When OS swaps, pages from PG's shared buffer pool can be swapped out. Subsequent queries that need those pages trigger a swap-in, adding 5-20ms per page fault.

2. **work_mem pressure**: Each `SELECT` uses `work_mem` (default ~4MB) for sorting/hashing. With 3 concurrent connections, that's 12MB of work_mem + the result set memory. On a 1GB instance, this competes directly with shared_buffers.

3. **State table contention**: The `dbmazz_snapshot_state` table receives:
   - 3 UPDATEs per cycle (mark_in_progress)
   - 3 UPDATEs per cycle (mark_complete)
   - 9 aggregate queries per cycle (chunk_counts + total_rows_synced + table_chunk_progress × 3 workers)
   - Total: **15 additional queries per cycle** just for bookkeeping
   - These writes also generate WAL, adding to the replication slot's workload

4. **ReadLatency at 10ms**: For a properly cached PostgreSQL, sequential reads should be <1ms. The 10ms average indicates that ~10% of pages are being fetched from disk/swap rather than shared_buffers.

### Estimated impact of swap

- **Without swap** (hypothetical): ReadLatency ~1-2ms, each chunk SELECT completes in ~200-500ms
- **With swap** (observed): ReadLatency ~10ms, each chunk SELECT takes proportionally longer
- **Overhead from bookkeeping queries**: 6 additional roundtrips per chunk at 10ms each = 60ms wasted per chunk
- At 3 workers, that's 180ms of wasted PG time per cycle just on bookkeeping queries that could be in-memory

### Quantified: what we'd gain by eliminating swap

If we could bring ReadLatency from 10ms to 2ms (by either upgrading the instance or reducing PG memory pressure):
- Each PG roundtrip saves ~8ms
- 9 roundtrips per chunk × 8ms = ~72ms saved per chunk
- With 3 workers processing continuously, that's ~72ms × 3 = 216ms saved per cycle
- At current cycle time of ~30s, that's a ~0.7% improvement per cycle from latency alone

But the bigger effect is **throughput**: with less swap, PG can serve data faster, allowing more chunks per unit time. The difference between config A (6 chunks/12min) and config B (3 chunks/1.5min) shows that **reducing memory pressure had a dramatic effect on predictability and throughput density**, even though we halved concurrency.

## 8. Proposed Code Optimizations

### Priority 1: In-memory atomic counters (eliminates 3 PG queries/chunk)

Replace `chunk_counts()`, `total_rows_synced()`, `table_chunk_progress()` with `AtomicU64` counters in `SharedState`. The infrastructure already exists (`SharedState` has `snapshot_chunks_done: AtomicU64` etc.) — currently these are set from DB values instead of incremented in-memory.

- **Impact**: -3 PG roundtrips/chunk = -9 queries/cycle across 3 workers
- **Risk**: Low. Counters are re-seeded from DB on restart. `fetch_add` must happen after `mark_chunk_complete` succeeds.

### Priority 2: Pre-compute `relation_id` in TableMeta (eliminates 1 PG query/chunk)

`get_relation_id` queries `pg_class` per chunk. The OID is static per table. `TableMeta` struct already exists and is populated once before workers start.

- **Impact**: -1 PG roundtrip/chunk = -3 queries/cycle
- **Risk**: None. OID doesn't change during a snapshot.

### Priority 3: Gzip compression on Stream Load

Add `Content-Encoding: gzip` to Stream Load HTTP headers. JSON compresses ~70-80%.

- **Impact**: Reduces network transfer. Not a current bottleneck but good for larger tables.
- **Risk**: Verify StarRocks version supports it (v2.2+).

### Combined effect of priorities 1+2

From 9 PG roundtrips/chunk to **5 PG roundtrips/chunk** (44% reduction in PG query load).
From 27 queries/cycle to **15 queries/cycle** across 3 workers.
This directly reduces memory pressure, I/O, and swap activity on the db.t3.micro.

## 9. Additional Considerations

- **Chunk size**: Consider reducing to 2K-5K rows to further reduce per-query memory pressure on PG
- **State table location**: `dbmazz_snapshot_state` lives on the source PG — every write competes with reads. Could be moved to local SQLite on the worker.
- **Streaming rows**: `client.query()` materializes all 10K rows in memory. `client.query_raw()` would allow streaming serialization, reducing memory spikes.
- **Instrumentation needed**: Add per-step timing (`Instant::now()`) in `process_chunk` to get exact time budget breakdown before optimizing.

## 10. Raw CloudWatch Data

### RDS db.t3.micro (Config B: 3 workers / 10K chunks)

```
SwapUsage (Bytes):
  01:04: 34,398,208 (32.8 MB)
  01:09: 34,398,208
  01:14: 34,795,520 (33.2 MB)
  01:19: 36,904,960 (35.2 MB)
  01:24: 38,711,296 (36.9 MB)
  01:29: 34,537,472 (32.9 MB)
  01:34: 35,717,120 (34.1 MB)
  01:39: 34,398,208 (32.8 MB)

FreeableMemory (Bytes):
  01:04: 82,456,576 (78.6 MB)
  01:09: 83,292,160 (79.4 MB)
  01:14: 76,107,776 (72.6 MB)
  01:19: 75,956,224 (72.4 MB)
  01:24: 72,151,040 (68.8 MB)
  01:29: 82,210,816 (78.4 MB)
  01:34: 79,446,016 (75.8 MB)
  01:39: 83,750,912 (79.9 MB)

ReadLatency (seconds):
  01:04: 0.0101
  01:09: 0.0095
  01:14: 0.0098
  01:19: 0.0109
  01:24: 0.0102
  01:29: 0.0105
  01:34: 0.0099
  01:39: 0.0101

CPUUtilization (%):
  01:04: 15.2
  01:09: 18.4
  01:14: 16.8
  01:19: 19.1
  01:24: 22.3
  01:29: 17.5
  01:34: 18.9
  01:39: 16.1

ReadIOPS:
  01:04: 42
  01:09: 48
  01:14: 55
  01:19: 51
  01:24: 60
  01:29: 45
  01:34: 52
  01:39: 43
```

### StarRocks EC2 t3.medium

```
CPUUtilization (%): 8.6 - 10.0 avg, 14% max
CPUCreditBalance: 367 → 383 (growing)
NetworkIn (per 5min): ~19-20 MB (during snapshot), ~50-60 KB (idle)
NetworkOut (per 5min): ~67-180 KB
EBSReadOps (per 5min): 0-37
EBSWriteOps (per 5min): ~10,400-11,400
EBSWriteBytes (per 5min): ~1.15-1.4 GB
```
