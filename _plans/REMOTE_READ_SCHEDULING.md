# Remote read scheduling

**Status: In progress.** Tracking issue: #1397. The read path as built is described in [FETCH_PLANNING.md](../_designs/FETCH_PLANNING.md), [READ_PIPELINE.md](../_designs/READ_PIPELINE.md) and [S3_STORAGE.md](../_designs/S3_STORAGE.md).

## Goal

A remote read takes as little wall time as the store allows, within the memory the user grants it. Wall time is the round trips on the read's critical path plus the bytes it fetches over the bandwidth it uses. Requests are a cost in that sum, and a store-specific one; they are not the target.

On a memory-mapped local file a fetch is a slice, and none of the machinery below applies.

## Where time goes today

Measured on main (`f04b6aa6`) with `run-s3.sh` (hardwood-benchmarks) under two profiles of the emulated store (see [Evidence](#evidence)): default (30 ms latency, 80 MB/s per connection) and distant (235 ms, 4 MB/s per connection). Requests and bytes are those of one read and are the same under both profiles; times are ms per read.

| Contender | Requests | Bytes | Default | Distant | Bound under the distant profile by |
|---|---|---|---|---|---|
| `hardwoodProjectedScan` | 8 | 14.2 MB | 258 | 3,028 | transfer, little overlapped: 14.2 MB at 4.2 MB/s is 3.4 s on one connection |
| `hardwoodFilteredScan` | 3 | 3.2 MB | 103 | 1,036 | transfer plus about one round trip |
| `hardwoodWideFilteredScan` | 44 | 27.8 MB | 1,067 | 10,110 | round trips: one data request per row group in sequence, 44 × 235 ms is 10.3 s |
| `hardwoodMultiFileScan` | 65 | 59.8 MB | 1,401 | 14,330 | transfer on one connection at a time: 59.8 MB at 4.2 MB/s is 14.3 s |
| `hardwoodBloomLookup` | 2 | 6.6 MB | 158 | 2,142 | one 6.6 MB window on one connection (1.6 s) and two round trips |
| `hardwoodLargeBloomLookup` | 7 | 100.7 MB | 1,533 | 26,372 | transfer of six 16 MB filters in sequence (24 s), where reading the 2 MB of data they guard takes about 1 s |

Each time is close to a serial model: the read's round trips one after another, plus its bytes over one connection's bandwidth. No contender is bound by unneeded bytes.

The remaining cost has three causes, none of which fewer requests can fix:

- **Requests that wait for each other.** An index window's ranges (#1394), the planning reads of the next row group and file, which start only after the current ones return (#1395), and pruning dictionaries one row group at a time (#1396).
- **One connection per transfer.** A coalesced region of up to 128 MB is one GET (#260), and a row group's first fetch does not overlap its decode (#259).
- **Structures not worth reading.** A Bloom filter or dictionary that costs more than the data it guards (#1402).

## Intended end state

### One scheduler per context

A `HardwoodContext` owns one scheduler, shared by every read of that context. Bandwidth and memory are process-wide resources: a scheduler per read would multiply both by the number of concurrent reads, and the cost model's aggregate term would no longer hold.

The context carries the totals, and a read may cap its own share:

```java
HardwoodContext context = HardwoodContext.builder()
        .maxBytesInFlight(512 * MB)      // every read of the context together
        .maxConcurrentRequests(64)
        .build();

ReaderConfig config = ReaderConfig.builder()
        .option("remote.maxBytesInFlight", "128m")  // this read's share
        .build();
```

These are the only public settings. The cost model's parameters are internal, with system properties for tuning, as `hardwood.internal.indexWindowBytes` is today.

### Plans declare ranges, the scheduler fetches them

Every stage of a read declares the byte ranges it needs, with what they depend on and how urgent they are:

| Stage | Ranges | Depends on |
|---|---|---|
| Open | footer (suffix read) | none |
| Prune | Bloom filters, dictionaries of the row groups ahead | footer |
| Index | ColumnIndex and OffsetIndex slices of the row groups ahead | footer; for a row group, its Bloom filters unless fetched together (#1400) |
| Data | surviving pages, or a whole chunk where it has no OffsetIndex | the row group's pruning and index |

The scheduler decides how the ranges are fetched: which merge into one request, which split into parts, how many run at once, how far ahead it reads, and which it does not fetch. Every request has a priority, and a higher one is never queued behind a lower one:

1. **Demand:** a range a reader is waiting for.
2. **Read-ahead:** a range of a row group or file the read will reach.
3. **Speculative:** a range the read may not need, such as the page index of a row group its Bloom filter may drop.

A read that stops early (`head(N)`, an abandoned iterator, `close()`) cancels its queued requests. Cancelling a request in flight drops its connection and the bytes already sent are billed, so speculative and read-ahead requests use smaller parts than demand requests, and waste less when cancelled.

A sequential plan (no OffsetIndex) declares its whole chunk, whose extent the footer gives, and walks page headers over the fetched parts in order; a read that stops early cancels the rest (#1025).

### Memory

A fetched buffer counts against the budget from the moment its request is issued until the last page sliced from it has been decoded. Read-ahead stops when the budget is full; demand requests are admitted past it so that a read cannot deadlock on its own read-ahead. For a full scan with read-ahead depth `D`, the budget is about `D × ` the projected bytes of a row group.

### Round trips

| Read | Critical path | Round trips |
|---|---|---|
| Small file (below the suffix threshold) | one suffix read returns the whole file (#1399) | 1 |
| Selective lookup | footer → Bloom filters and page index together (#1400) → surviving data | 3 |
| Selective lookup, structures fetched apart | footer → Bloom filters → page index → data | 4 |
| Many files, read-ahead depth `D` | | `files × round trips per file / D`, bounded below by bytes over aggregate bandwidth |

Read-ahead overlaps the round trips of upcoming row groups and files with the current one; it does not shorten the chain of a single row group.

## Cost model

Per backend, with these parameters:

| Parameter | Use |
|---|---|
| latency `L` | time to first byte of a request |
| per-connection bandwidth `B` | transfer rate of one request |
| aggregate bandwidth `A` | transfer rate of all requests together; parallel requests gain nothing once `A` is reached |
| request cost | money and request-rate limits (S3 limits GETs per prefix per second); a floor on part size |

From them:

- **Merge** two ranges when the gap between them costs less to transfer than a round trip: `gap < L × B` (about 2.4 MB at 30 ms and 80 MB/s). This replaces the fixed `CoalescingPolicy.GAP_BYTES`.
- **Split** a range into parts when one connection is the bottleneck and the store has aggregate headroom: part size from `L × B`, not below the floor. Published measurements on S3 point to parts of 8–16 MB (AnyBlob); calibrate.
- **Read ahead** as far as the memory budget allows and the store has aggregate headroom; beyond `A`, deeper read-ahead only adds memory.
- **Fetch a pruning structure** when `cost(structure) < P(prune) × cost(data it guards)` (#1402).
- **Fetch speculatively** (#1400) when the bytes at risk cost less than the round trip saved.
- **Hedge** a short request that has not completed past a threshold derived from `L` and its size (#1401).

The parameters start as static per-backend values: none for local files, measured defaults for S3. They are refined from measured requests only where measurements show the static values are wrong. Refinement needs time to first byte, which `S3InputFile` sees and the generic `InputFile` seam does not.

## Evidence

Each change that alters a request shape gets a `run-s3.sh` contender first, so the change shows as a before/after in requests, bytes and time. `run-s3.sh` emulates the store with S3Proxy behind Toxiproxy, which applies latency, jitter and per-connection bandwidth; profiles are environment settings of `s3-env.sh`.

| Profile | Latency | Per connection | Jitter | Use |
|---|---|---|---|---|
| default | 30 ms | 80 MB/s | none | same-region store, regression baseline |
| distant | 235 ms | 4 MB/s | none | distant region, from Perflab's traces (#828) |
| jitter | 30 ms | 80 MB/s | to be chosen | tail latency, for #1401 |

Toxiproxy limits each connection on its own, so no profile reproduces an aggregate cap, under which parallel requests stop helping (Perflab measured 3–4 MB/s in total from a distant region, whatever the number of streams). Until that is emulated, results that depend on concurrency are checked against real S3 before they set a default. Real-S3 calibration is the Perflab track (#827, #763).

## Delivery

| # | Stage | Issues | Depends on |
|---|---|---|---|
| 0 | Profiles and contenders: small files, wide file with gapped slices, dictionary lookup, jitter profile; baseline measurements | #1399, #1394, #1396, #1401 (contenders only) | |
| 1 | Independent wins | #1399, #1393 | 0 |
| 2 | Read-ahead spike on a throwaway branch, against the default and distant profiles | #1395 | 0 |
| 3 | Scheduler core: context limits, range declaration, priorities, cancellation, memory accounting; index windows fetched through it | #1264, #1394 | 2 |
| 4 | Read-ahead of planning reads and files; dictionary read-ahead | #1395, #1396 | 3 |
| 5 | Combined Bloom and page-index fetch; expected-value skipping | #1400, #1402 | 3 |
| 6 | Splitting large ranges; overlapping the first fetch with decode; part sizing | #260, #259 | 3 |
| 7 | Hedging | #1401 | 3 |

Stage 2 decides whether the scheduler's central assumption holds: if read-ahead does not improve the multi-file and large-filter contenders as the cost model predicts, the design is revisited before stage 3.

Independent of the scheduler, in any order: #837, #381, #378, #1354, #1364, #1306, #1025 (after #500), #501.

## Open questions

- **Aggregate cap in the emulated store.** Shaping loopback traffic needs `tc` (not in the dev container) and `CAP_NET_ADMIN` (not granted). A relay with one token bucket shared by all connections, placed in front of Toxiproxy in `s3-env.sh`, needs neither.
- **Where requests run.** The scheduler issues requests on virtual threads; whether the JDK `HttpClient`'s connection pool needs an explicit bound beside `maxConcurrentRequests` is to be measured.
- **Read workers and the scheduler.** Column retrievers block on chunk handles today; under the scheduler they wait on the ranges they declared. How a retriever's demand is raised in priority once it blocks is to be settled in stage 3.

## Prior art

| Project | Relevant part |
|---|---|
| AnyBlob (Durner, Leis, Neumann, VLDB 2023) | cost model for object-store reads, request sizing, concurrency, reissuing slow requests |
| Arrow C++ `ReadRangeCache` | static hole and range limits, concurrent fetch, no cost model; a `latency × bandwidth` helper no reader calls |
| DuckDB ([asynchronous I/O](https://duckdb.org/2026/07/31/asynchronous-io)) | `latency × bandwidth` merge gap, clamped and refined from measured requests |
| Lance | an I/O scheduler with a priority queue and back-pressure on bytes in flight |
