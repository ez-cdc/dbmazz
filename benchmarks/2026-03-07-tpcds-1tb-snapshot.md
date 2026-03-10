# Benchmark: TPC-DS 1TB Snapshot

**Date**: 2026-03-07
**Job**: stress-job-1 (job ID 85)
**Status**: Running (partial results at time of recording)

## Infrastructure

| Resource | Value |
|----------|-------|
| EC2 Instance | c5.2xlarge (or equivalent) |
| vCPUs | 8 |
| RAM | 16 GB |
| Region | us-west-2 |
| DBMazz Version | latest (with gzip + metadata cache + byte-level escaping) |

## Job Configuration

| Setting | Value |
|---------|-------|
| Chunk Size | 50,000 rows |
| Snapshot Workers | 25 (concurrent) |
| Flush Size | 10,000 msgs |
| Flush Interval | 5,000 ms |
| Tables | 25 (full TPC-DS schema) |
| Source | PostgreSQL RDS (us-west-2) |
| Sink | StarRocks (self-hosted EC2) |
| Replication Slot | dbmazz_stress_job_1_slot |
| Gzip Compression | Enabled (Compression::fast, level 1) |

## Dataset

| Table | Estimated Rows | Chunks | % of Total |
|-------|---------------|--------|------------|
| store_sales | 2,880,400,640 | 57,609 | 50.9% |
| catalog_sales | 1,440,019,200 | 28,801 | 25.5% |
| inventory | 783,000,128 | 15,661 | 13.8% |
| web_sales | 719,920,832 | 14,399 | 12.7% |
| store_returns | 287,987,712 | 5,760 | 5.1% |
| catalog_returns | 143,993,648 | 2,880 | 2.5% |
| customer | 11,999,622 | 240 | 0.2% |
| customer_address | 5,999,433 | 120 | 0.1% |
| customer_demographics | 1,920,800 | 39 | <0.1% |
| date_dim | 73,049 | 2 | <0.1% |
| time_dim | 86,400 | 2 | <0.1% |
| item | 300,000 | 6 | <0.1% |
| web_returns | 71,981,208 | 1,440 | 1.3% |
| household_demographics | 7,200 | 1 | <0.1% |
| income_band | ~20 | 1 | <0.1% |
| promotion | 1,500 | 1 | <0.1% |
| reason | 65 | 1 | <0.1% |
| ship_mode | ~20 | 1 | <0.1% |
| store | 1,002 | 1 | <0.1% |
| warehouse | ~20 | 1 | <0.1% |
| web_page | 3,000 | 1 | <0.1% |
| web_site | 54 | 1 | <0.1% |
| call_center | 42 | 1 | <0.1% |
| catalog_page | 30,000 | 1 | <0.1% |
| dbgen_version | — | 0 | skipped (no integer PK) |
| **TOTAL** | **~6,347,795,595** | **113,129** | |

## Performance Results (partial — test still running)

| Metric | Value |
|--------|-------|
| Chunks completed | 2,176 / 113,129 |
| Rows synced | 108,775,813 |
| First chunk at | 23:05:28 UTC |
| Last chunk at | 23:21:49 UTC |
| Elapsed (processing) | ~16 min 21 sec (981s) |
| **Throughput** | **~110,882 rows/sec** |
| **Throughput** | **~6.65M rows/min** |
| Avg rows per chunk | ~49,989 |
| Chunks per second | ~2.22 |

## Resource Utilization

| Resource | Value |
|----------|-------|
| CPU Usage | 82-91% (sustained) |
| Memory (daemon RSS) | ~1.7 GB (10.5% of 16GB) |
| Memory Limit | 12,563 MB (auto-calculated: 15704MB * 0.80) |

## Timing Breakdown

| Phase | Duration | Notes |
|-------|----------|-------|
| Setup (source + sink validation) | ~3s | PG tables, replica identity, StarRocks tables |
| Chunking (all 25 tables) | ~2 min 53s | Sequential, blocks tokio runtime |
| — catalog_sales chunking | ~43s | 28,801 chunks |
| — store_sales chunking | ~89s | 57,609 chunks |
| — web_sales chunking | ~22s | 14,399 chunks |
| Processing (chunks → StarRocks) | ongoing | 25 parallel workers |

## Estimated Total Time

At ~110K rows/sec with ~6.35B total rows:
- **Estimated total**: ~16 hours
- **Estimated cost**: ~$5.50 (c5.2xlarge at $0.34/hr)

## Optimizations Applied (vs previous baseline of ~40K rows/sec)

1. **Gzip compression** in StreamLoadClient (flate2, Compression::fast) — ~5-10x less network bytes
2. **Byte-level JSON escaping** instead of char-by-char — ~2-3x faster serialization
3. **Metadata cache** (TableMeta) — eliminated ~226K redundant catalog queries
4. **Memory limit fix** — removed /max_jobs division that caused OOM kills at 1.2GB
5. **Timeout tolerance** — 30 consecutive timeouts before killing daemon (was: instant kill on 1 timeout)

## Known Bottlenecks

1. **CPU-bound at 8 vCPUs**: gzip compression + JSON serialization saturate cores
2. **Chunking blocks tokio runtime**: 2m53s of blocked gRPC during chunk computation. Workers sit idle during this phase.
3. **Fixed worker count**: 25 workers regardless of available CPUs
4. **Sequential chunk computation**: All 113K chunks computed before any processing starts

## Recommended Next Steps

1. **Non-blocking chunking**: Move to `spawn_blocking` + channel pattern. Start processing chunks as they're computed.
2. **Auto-scale workers to vCPUs**: `workers = vCPUs * 3` or similar heuristic.
3. **Larger instance test**: c5.4xlarge (16 vCPU) with 50 workers — expect ~200-250K rows/sec.
4. **Multi-job test**: Split tables by volume into 3 jobs on same instance — expect ~300K rows/sec.

## Previous Test Results (for comparison)

| Test | Date | Workers | Chunk Size | Instance | Result |
|------|------|---------|------------|----------|--------|
| Baseline (pre-optimizations) | 2026-03-06 | 5 | 50,000 | 8 vCPU | ~40K rows/sec |
| 40 workers (old binary) | 2026-03-06 | 40 | 50,000 | 8 vCPU | Failed (memory limit /10 bug) |
| 30 workers (old binary) | 2026-03-07 | 30 | 100,000 | 8 vCPU | Failed (memory limit /10 bug) |
| **This test** | **2026-03-07** | **25** | **50,000** | **8 vCPU** | **~110K rows/sec** |
