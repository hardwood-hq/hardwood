# Design: cross-column ranged-GET coalescing within a row group

**Status: Implemented.** Tracking issue: #374.

## Goal

Reduce the number of HTTP ranged GETs the Parquet pipeline issues per row
group on remote storage. Today every leaf column's `ChunkHandle` issues its
own `readRange`; for wide schemas (Overture: ~50 leaf columns × ~50 KB
compressed each), that's ~50 round trips per RG. The bytes are typically
stored back-to-back on disk, so they can be fetched in one ranged GET and
sliced per-column.

The pipeline already does exactly this for the per-RG index region
(`RowGroupIndexBuffers.fetch` issues one ranged GET covering every column's
OffsetIndex/ColumnIndex bytes, then slices). This design generalises that
pattern to data pages.

## What changes

A new internal abstraction `SharedRegion` represents a contiguous,
multi-column byte range fetched in one shot. `ChunkHandle` becomes a
view into a `SharedRegion`'s buffer rather than owning its own
`readRange`.

`RowGroupIterator.computeFetchPlans` gains a coalescing pass that runs
after the per-column plans are built:

1. Collect every `ChunkHandle`'s `(offset, length)` across all column plans.
2. Sort by offset.
3. Greedily group entries into `SharedRegion`s — extend the current region
   when the gap to the next handle is below `MAX_CROSS_COL_GAP` and the
   resulting span is below `MAX_COALESCED_BYTES`; otherwise start a new
   region.
4. Chain regions for one-ahead pre-fetch, replacing the per-column
   `nextChunk` chain (the per-column chain is unnecessary when many
   columns sit in one region).
5. Rewrite each `ChunkHandle` to delegate `ensureFetched` to its
   containing region, slicing the requested bytes after the region is
   loaded.

## Constants

- **`MAX_CROSS_COL_GAP`** — bridge tiny gaps between adjacent column
  chunks. Set to 64 KB initially. Adjacent chunks are typically
  back-to-back (zero gap); the budget covers writers that emit padding,
  checksums, or interleaved metadata. Larger and we'd be paying for
  dead bytes; smaller and we miss legitimately bridgeable cases.
- **`MAX_COALESCED_BYTES`** — already exists at 128 MB in
  `RowGroupIterator`. Reused. Caps any single coalesced region.

## SharedRegion

```java
final class SharedRegion {
    private final InputFile inputFile;
    private final long fileOffset;
    private final int length;
    private final String purpose;          // FetchReason tag
    private volatile ByteBuffer data;
    private volatile SharedRegion nextRegion;

    ByteBuffer slice(long absoluteOffset, int sliceLength) { … }
    void setNextRegion(SharedRegion next) { … }
    ByteBuffer ensureFetched() {
        // single readRange, FetchReason-scoped to `purpose`;
        // chains an async prefetch of `nextRegion` on first call
    }
}
```

`ChunkHandle` carries an optional `SharedRegion region` reference. When
set, its `ensureFetched()` slices from the region instead of issuing
its own `readRange`; the per-handle `nextChunk` chain is bypassed
because pre-fetch is driven at the region level via
`SharedRegion.nextRegion`. The construction shape stays the same; only
the fetch path changes.

## Edge cases

**Filter pushdown / page drops.** When `IndexedFetchPlan` drops pages, a
column's bytes become non-contiguous (multiple `ChunkHandle`s with intra-
column gaps). Coalescing across columns over those gaps would re-fetch
the dropped bytes — exactly what page-drop optimisation is meant to
avoid.

**`head(N)` truncation.** A `SequentialFetchPlan` whose `chunkSize` is
below the column's full length reads only the first chunk eagerly and
fetches the remainder on demand via the per-column chain. Bridging
such a column into a shared region would pull in the column's later
bytes that the per-column chain would later re-fetch (double-fetch).

Conservative rule, implemented as `CoalescableFirstChunk.isCoalesceSafe()`:
the plan's first chunk must represent the column's *full* byte range
— i.e. an `IndexedFetchPlan` with exactly one page group, or a
`SequentialFetchPlan` whose `chunkSize == columnChunkLength`.
Plans that fail the gate keep their per-column reads. Mixed RGs
partition into safe columns (coalesced) and unsafe columns
(per-column). The typical dive Data preview path on small / mid-size
files (≤ 4 MB per column, see #382) gets full coalescing.

**Refcount and lifecycle.** `RowGroupIterator.releaseWorkItem` already
evicts the per-workitem `FetchPlan[]` when its refcount reaches zero,
which today releases the `ChunkHandle.data` buffers. With shared
regions, eviction drops the `FetchPlan[]` references, the `ChunkHandle`s
release their `SharedRegion` references, and GC reclaims the buffers.
No explicit refcount changes needed.

**FetchReason tagging.** The current per-handle purpose string
(`rg=N col=foo seqChunk@K`) doesn't compose when one ranged GET covers
many columns. The region-level `purpose` becomes
`rg=N region=A..B` (A..B are the projection-column indices spanned by
the region) so log attribution stays intelligible. Per-column handles
that slice from the region keep their own purpose tag (e.g.
`rg=N col='foo' pageGroup=1 (region-backed)`) but never issue a
network call themselves — the actual `readRange` happens at the
region level.

**Cross-thread propagation.** The async prefetch chain
(`SharedRegion.nextRegion`, `IndexedFetchPlan.prefetch`,
`ChunkHandle.nextChunk`) schedules work via
`CompletableFuture.runAsync` and uses `FetchReason.bind` to carry the
calling thread's reason across the handoff; otherwise the worker
thread would log as `unattributed`.

## What this doesn't change

- **Bytes delivered to the consumer.** Same data, same decode paths,
  same row counts. Only the wire-level request count drops.
- **Page-level coalescing within a column** — the existing
  `coalescePages` step in `RowGroupIterator` operates one level below
  this and stays unchanged.
- **The InputFile API.** Still just `readRange(offset, length)`.

## Estimated impact

For Overture-shape: 50 leaf columns × ~50 KB compressed, near-zero
gaps. Today: 50 ranged GETs per RG, ~2.5 MB total. After: 1–2 ranged
GETs per RG (coalesced into 1–2 contiguous regions), same ~2.5 MB.
Latency saving: ~50 × RTT vs ~1–2 × RTT per RG, dominated by S3
round-trip latency at ~50 ms each.

Bytes-on-the-wire don't change; request count and per-RG latency do.

## Composition

- With **#373** (S3 byte-range LRU): coalesced regions are coarser
  cache units. Hits/misses align to region boundaries, not per-column
  ones. No conflict.
- With **#370** (per-column-worker over-fetch): the worker behaviour is
  unchanged; coalescing happens at plan-build time, before workers
  start. #370's fix and this one are orthogonal.
- With **#361** / **#377** (firstRow seek + dive window): unchanged.
  The window's refill path issues `readPreviewPage` which goes through
  the same iterator/plan code; coalescing applies transparently.
- With **#381** (page-level skip via OffsetIndex when seeking with
  firstRow): page drops introduce intra-column gaps and trigger the
  conservative "no coalescing for filtered columns" rule. The two
  features compose by partitioning each RG into coalesced (fully-read)
  and per-column (filtered) sub-plans.

## Testing

- **`RowGroupCrossColumnCoalesceTest`** (new): open a multi-column
  fixture wrapped in a `CountingInputFile`, read all rows, assert
  `readRange` count drops from N (one per column) to ≤ a tight bound
  for the no-filter case. The synthesised fixture (or a real
  Overture-shaped one) needs ≥ 10 leaf columns for the assertion to be
  meaningful.
- **Existing tests must pass unchanged.** Coalescing affects request
  count, not data delivered.
- **Filter test:** read a multi-column fixture with a filter that
  drops pages on one column; assert per-column reads on the filtered
  column (no over-coalescing into dropped bytes).
- **Lifecycle test:** drive enough RG transitions to exercise eviction
  and confirm the workitem refcount path correctly releases shared
  regions.

## Out of scope

- **Cross-RG coalescing.** Index regions already coalesce per-RG; data
  pages are typically far apart between RGs and not worth bridging.
- **Adaptive `MAX_CROSS_COL_GAP`** based on file size or per-RG
  characteristics. Static constant is sufficient for now; tune once
  real workloads surface a need.
- **Reordering reads.** The greedy offset-sorted walk is fine; no need
  for a smarter scheduler.
