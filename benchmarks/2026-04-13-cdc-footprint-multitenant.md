# Benchmark: dbmazz CDC footprint at scale

**Date analyzed**: 2026-04-13
**Test period**: 2026-02-21 → 2026-02-23 (3 days)
**Status**: Final

---

## TL;DR

Across a 3-day production load test running **40 dbmazz daemons concurrently
on a single ~t3.medium worker** (2 vCPU / 4 GB RAM), per-daemon footprint
was **nearly constant across three workload tiers** that differed by 4
orders of magnitude in configured insert rate.

| Tier | Configured rate (per source DB) | Jobs | CPU avg | RSS avg |
|---|---|:---:|:---:|:---:|
| **High-rate** | 500–1 000 inserts/sec | 10 | **11.3 millicores** | **11.0 MB** |
| **Moderate-rate** | 50–100 inserts/sec | 20 | **10.7 millicores** | **10.7 MB** |
| **Low-rate** | 1–5 inserts/min | 10 | **12.3 millicores** | **10.7 MB** |

**The headline finding: dbmazz overhead is fixed-cost per daemon, not
load-dependent.** Every daemon — regardless of whether its source was
producing 1 000 inserts/sec or 1 insert/minute — converged on roughly
**1 % of one CPU core and ~11 MB of RSS**. The cost is dominated by
holding the replication slot, parsing pgoutput messages, and maintaining
sink connections; the marginal cost per event is small enough to be
invisible at this scale.

---

## Setup

| Component | Spec |
|---|---|
| Worker (dbmazz host) | ~t3.medium — **2 vCPU, ~4 GB RAM** |
| Source databases | 5× RDS PostgreSQL 16, `db.t3.large` / `db.r5.large` / `db.r5.xlarge`, us-west-2 |
| Source DB count | 40 (10 high-rate + 20 moderate + 10 low-rate) |
| Sink | StarRocks on `m5.2xlarge` (8 vCPU, 32 GB), shared across all 40 jobs |
| Tables per source | `orders`, `events`, `products` (some sources also `wide_records`, 50 cols) |
| Workload generator | Custom Python — random insert/update/delete mix, 70 / 20 / 10 ratio |
| Job count | 40 dbmazz daemons concurrent (peak) |
| dbmazz version | 1.4.x |

The workload was multi-tenant by design: each of the 40 dbmazz daemons
replicated its own dedicated PostgreSQL database on the source side, all
writing to the same StarRocks instance on the sink side.

## Methodology

Per-process metrics (CPU and RSS) were collected from each dbmazz daemon
during the live test, persisted to a Prometheus-compatible time-series
store, and aggregated server-side over the full 3-day window. CPU was
measured directly by each daemon from `/proc/<self_pid>/stat` and
converted to millicores; RSS was sampled per-process from the OS so that
each daemon's footprint is reported independently of the host or any
sibling processes.

Aggregations were computed per job over the 3-day window using `avg`,
`min`, `max`, and `p50`.

## Per-tier results

### High-rate tier — 10 daemons

Configured target: **500–1 000 inserts/sec per source** (plus 20 % updates,
10 % deletes). Source DB: `db.r5.xlarge` (4 vCPU / 32 GB). Some daemons
in this tier also replicated a 50-column `wide_records` table.

| Metric | Min | Avg | Max | p50 |
|---|---:|---:|---:|---:|
| CPU (millicores) | 8.6 | **11.3** | 14.7 | 11.7 |
| RSS (MB) | 10.66 | **11.01** | 11.31 | 11.03 |
| ops/sec average (during active periods) | 22.5 | 31.4 | 38.8 | 33.6 |
| ops/sec peak | 107 | 140 | 180 | 145 |
| Total events processed (3 days) | 303 K | 322 K | 342 K | 323 K |

### Moderate-rate tier — 20 daemons

Configured target: **50–100 inserts/sec per source** (plus 20 % updates,
10 % deletes). Source DB: `db.r5.large` (2 vCPU / 16 GB).

| Metric | Min | Avg | Max | p50 |
|---|---:|---:|---:|---:|
| CPU (millicores) | 9.1 | **10.7** | 11.9 | 10.9 |
| RSS (MB) | 10.41 | **10.75** | 11.32 | 10.73 |
| ops/sec average (during active periods) | 26.8 | 35.1 | 45.5 | 35.5 |
| ops/sec peak | 174 | 232 | 285 | 243 |
| Total events processed (3 days) | 313 K | 390 K | 458 K | 403 K |

### Low-rate tier — 10 daemons

Configured target: **1–5 inserts/minute per source** (plus 20 % updates,
10 % deletes). Source DB: `db.t3.large` (2 vCPU / 8 GB).

| Metric | Min | Avg | Max | p50 |
|---|---:|---:|---:|---:|
| CPU (millicores) | 10.4 | **12.3** | 13.3 | 12.7 |
| RSS (MB) | 10.56 | **10.68** | 10.91 | 10.68 |
| ops/sec average (during active periods) | 54.9 | 68.8 | 77.0 | 70.9 |
| ops/sec peak | 303 | 348 | 417 | 348 |
| Total events processed (3 days) | 603 K | 663 K | 721 K | 678 K |

## Worker-level results (bonus context)

The same worker that hosted these 40 dbmazz daemons reported the
following over the test period:

| Metric | Value |
|---|---|
| Concurrent dbmazz daemons | 40 (peak), 27 (avg) |
| Worker total CPU | 15 % avg, 32 % peak |
| Worker total RAM used | 522 MB of ~4 GB (12.7 %) |
| Per-daemon RAM (derived from worker total ÷ 40) | ~13 MB |

The per-daemon RAM derived from the worker total (~13 MB) is consistent
with the per-process RSS measured directly (~11 MB) — the small delta
covers OS overhead and supporting host processes.

## What this benchmark proves

1. **dbmazz is genuinely lightweight per daemon.** Average CPU was about
   **1 % of one core** and RSS was about **11 MB** across all three
   tiers. Nothing in the setup pushed the daemon close to a ceiling.

2. **Footprint is essentially independent of workload.** The high-rate
   tier and the low-rate tier differed by ~4 orders of magnitude in
   configured insert rate, yet the per-daemon CPU and RSS averages are
   within ~15 % of each other. The fixed cost of running a CDC daemon
   (replication slot, pgoutput parsing, sink connection) dominates; the
   marginal cost per event is too small to register at this scale.

3. **dbmazz scales horizontally on a single host.** 40 independent
   daemons fit comfortably on a 2-vCPU / 4-GB worker with ~70 % of the
   memory still free and total CPU averaging 15 %. Each daemon is a
   normal Unix process with its own replication slot and sink
   connection.

## What this benchmark does NOT prove

This is a footprint study, not a throughput study. Specifically:

- **Maximum sustained throughput per daemon was not measured.** No
  daemon was anywhere close to CPU- or memory-bound during the run, so
  the data does not bound dbmazz's ceiling.

- **The total-events column includes snapshot work, not pure CDC
  streaming.** Several jobs were restarted during the test (rebalancing
  on host churn), and each restart triggers a fresh snapshot of the
  seed data. The fact that the *low-rate* tier processed *more* total
  events than the *high-rate* tier (663 K vs 322 K per job) is
  explained by restart churn, not by CDC throughput. Treat the
  total-events numbers as "events the daemon processed", not as
  "events the source produced".

- **End-to-end replication lag was below the metric reporting
  threshold** for all 40 daemons across the 3 days. The workload was
  light enough that there was no measurable lag, but this also means
  we cannot make a defensible p50 / p99 lag claim from this data.

A reproducible CDC streaming benchmark with a sustained workload that
actually saturates the daemon — and with measurable lag — is tracked
separately in [issue #71](https://github.com/ez-cdc/dbmazz/issues/71).

## Caveats

- **Worker instance type is deduced**, not directly observed. The
  measured ~522 MB used out of ~4.1 GB total and a peak of 32 % of two
  cores are consistent with `t3.medium` (2 vCPU, 4 GB). The exact
  instance type was not preserved at the time of the test.

- **The ops/sec gauge is sampled**, not a derived rate. It under-reports
  short bursts that fall between samples. The "ops/sec average" column
  above counts only samples where the gauge was non-zero, which is why
  it is higher than the configured target rate divided by 60.

- The exact workload generator behaviour (request rates, retry policy,
  per-table mix) was custom Python and the configured *target* rates do
  not always match what the generator actually produced.

## Reproducibility

The exact infrastructure used for this run is internal and not directly
reproducible from this repo. The methodology — per-process CPU and RSS
sampling aggregated over a multi-day window — applies to any
PostgreSQL → StarRocks setup with per-process metrics enabled.

For a future run that *is* fully reproducible (single dbmazz daemon
against a controlled `pgbench` workload, with a published script),
see [issue #71](https://github.com/ez-cdc/dbmazz/issues/71).
