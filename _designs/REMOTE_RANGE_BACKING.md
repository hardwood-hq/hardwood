# Design: whole-file range backing for `S3InputFile`

**Status: Proposed.** Tracking issue: #373.

## Goal

Cache fetched byte ranges inside `S3InputFile` so repeat reads of the
same offsets hit local memory instead of re-issuing HTTP GETs to S3.
The motivating workflow is dive's Data preview path: flip-flopping
between top and bottom of a remote file (`g`, `G`, `g`, …) currently
re-fetches the same row groups every time.

This sits one layer below the cross-column coalescing already
introduced in #374. Coalesced regions become coarser cache units —
one cached entry per region covers many columns at once.

## Approach: whole-file backing, not LRU

`MappedInputFile.open()` calls `channel.map(READ_ONLY, 0, fileSize)`,
reserves the full file as virtual address space, and lets the OS
lazy-fault pages on `slice` calls. There is no cache structure
because the mapping *is* the cache.

The remote analogue:

- On `S3InputFile.open()`, allocate a buffer of `fileSize` bytes and
  track which byte ranges have been populated.
- On `readRange(offset, length)`: if the range is fully within the
  populated set, return a slice of the buffer — zero-copy, no
  network I/O. If not, fetch the missing sub-range(s) from S3, write
  them into the buffer at their offsets, mark the ranges populated,
  and return the slice.
- On `close()`, release the buffer.

No eviction. The buffer's lifetime is the `S3InputFile`'s lifetime;
its size is the file's size. This is structurally symmetric with the
local mmap path — both expose "a buffer of size `fileSize` you can
slice into," and the rest of the pipeline doesn't know which is
which.

### Why not LRU

LRU was the obvious starting point but pays for capacity it doesn't
need given how the rest of the pipeline already shapes I/O:

- **Coalescing already produces stable request shapes.** A refill
  against a given window always re-issues the same
  `readRange(offset, length)` for the same `SharedRegion`. Exact-match
  caching catches ~all the wins; replacement policy is unused work.
- **Eviction race risk.** With an LRU of off-heap entries, eviction
  needs to deterministically free the buffer (`Arena.close()` or
  similar). Any concurrent slice still in use would crash. Whole-file
  backing has one buffer for the file's lifetime, no per-entry release.
- **"What's the right budget?" is unanswerable in the abstract.**
  The whole-file model doesn't ask the user to pick.

The trade-off is footprint: whole-file backing reserves `fileSize` of
*virtual* address space up front, regardless of how much of the file
the session actually touches. This matters for files > 2 GB or
many-file workloads — see [Limits](#limits) below — but is fine for
the dive workflow against typical analytics files.

## Implementation: sparse temp file + mmap

The naive form of "allocate a buffer of `fileSize`" is
`Arena.allocate(fileSize)` — but FFM requires zero-fill on
allocation, which eagerly commits the full size. For a 622 MB
Overture file that's ~600 ms of zeroing on first open and 622 MB
resident from then on, even if the session only paged through 80 MB.

Backing the buffer with a **sparse temp file** instead lets the OS
handle lazy commit:

```
open():
    create a temp file in `java.io.tmpdir` (or configurable);
    sparse-truncate to `fileSize` (`RandomAccessFile.setLength` /
        `FileChannel.truncate`, which on Linux/macOS / NTFS leaves a
        file with holes);
    mmap it READ_WRITE for the writer side, READ_ONLY for slice
        consumers;
    discover `fileSize` and pre-fetch the tail (`open-tail`,
        unchanged from today) — write those bytes into the mmap at
        their absolute offsets.

readRange(off, len):
    if filled.contains([off, off + len)):
        return mapping.slice((int) off, len)        // zero-copy
    else:
        fetch missing sub-ranges from S3;
        write each into the mapping at its absolute offset;
        filled.add(those ranges);
        return mapping.slice((int) off, len)

close():
    unmap;
    delete temp file
```

The OS commits pages only when written; never-touched holes occupy
neither RAM nor disk on filesystems that support sparse files (ext4,
xfs, apfs, ntfs). Logical address space = `fileSize`, real footprint
= touched bytes.

**Comparison vs in-memory variants:**

| Variant                             | Up-front cost          | Real footprint    | Lazy commit            |
|-------------------------------------|-----------------------|-------------------|------------------------|
| `Arena.allocate(fileSize)`          | zero whole file (~1 GB/s) | full `fileSize` | no                     |
| `Unsafe.allocateMemory(fileSize)`   | depends on glibc       | varies            | sometimes              |
| **Sparse temp file + mmap**         | sparse-truncate (instant) | touched bytes only | yes (kernel-managed) |

The third variant is the recommended implementation. The cost: an
extra disk write per fetched range (S3 → mmap'd file). On a
`tmpfs`-backed temp dir this stays in RAM (Linux default for
`/tmp` on many distros). On a disk-backed temp dir it incurs disk
I/O proportional to bytes fetched — typically a fraction of the S3
RTT savings, so net positive.

## API surface

`S3InputFile` exposes the same `InputFile` contract; the
implementation change is internal. New configuration on `S3Source`
(or `S3InputFile.Builder`):

```java
S3Source.builder()
    .rangeBacking(RangeBacking.NONE)             // default: per-call HTTP, tail-cache only
    .rangeBacking(RangeBacking.SPARSE_TEMPFILE)  // opt in, whole-file mmap-backed cache
    .tempDir(Path.of("/var/cache/hardwood"))     // optional override
    .build();
```

**Default: `NONE`** — keeps today's behaviour exactly. Every
`readRange` is a network GET, only the tail is cached. No new
failure modes (writeable temp dir, disk capacity), no resident-set
growth surprises for streaming callers (`convert`, `print`, one-shot
analytics jobs that read top-to-bottom once).

**Opt in to `SPARSE_TEMPFILE`** when the workload re-reads byte
ranges. The canonical opt-in is `DiveCommand`, which configures its
`S3Source` with sparse-tempfile backing on construction — every
file dive opens benefits, and every flip-flop / re-render turns
into a cache hit. Other callers leave the default and pay one HTTP
GET per fetch as today.

The configuration is on `S3Source` rather than per-file because the
opt-in is a workload-level decision (dive vs streaming), not a
per-file one.

### Why opt-in, not opt-out

Every `readRange` is correct under either backing — the cache is
only ever a win or a wash for HTTP traffic, never an extra fetch.
The case for opt-in is about *side effects*:

- **No behaviour change for existing callers.** Default-off means
  upgrading the library does not start writing to a temp dir, growing
  resident-set, or introducing a new `IOException` shape at `open()`
  for callers that didn't ask for caching.
- **Streaming readers' resident set stays bounded.** A one-shot
  top-to-bottom read finishes today with the resident set bounded
  by active row groups (~tens of MB). With sparse-tempfile backing
  on, the same workload finishes with the entire file resident
  (hundreds of MB on Overture-shape) — fine on `tmpfs`-backed temp
  dirs where the kernel will reclaim under pressure, surprising
  on small-RAM or disk-backed temp environments.
- **Reversibility.** Going opt-in → default-on later is a benign
  change (callers get a strict performance improvement); going
  default-on → opt-in later silently breaks every caller that came
  to rely on it. Pick the conservative starting point.

Dive is the workload where the cache hit rate is the whole point,
and `DiveCommand` already owns its `S3Source` configuration, so
flipping the opt-in for dive is a one-line change.

## Tracking populated ranges

A simple sorted interval set:

```java
final class RangeSet {
    // TreeMap from `start` → `end`. Invariant: no two entries overlap
    // or touch (a, b) and (b, c) get merged into (a, c).
    private final TreeMap<Long, Long> ranges = new TreeMap<>();

    boolean contains(long start, long end) { … }
    void add(long start, long end) { … }   // merges overlapping / touching
    /// Returns the gaps in `[start, end)` not yet populated.
    List<long[]> missing(long start, long end) { … }
}
```

`add` is `O(log n)` amortised; `contains` and `missing` are
`O(log n + k)` for `k` adjacent entries. With dive's coalesced
fetch shape we expect `n` to stay small (a few dozen entries per
file), so absolute cost is negligible.

## Composition with existing layers

- **Tail cache.** Already exists; populates the same buffer at
  `[fileLength - TAIL_SIZE, fileLength)` during `open()`. Becomes a
  trivial special case of the general flow — no separate code path.
- **`SharedRegion.data`** (#374). Per-RG cache that lives until
  `releaseWorkItem` evicts the workitem. Stacks above this layer:
  `SharedRegion.fetchData` calls `inputFile.readRange(...)`, which
  hits the file-level cache. `SharedRegion.data` is freed on
  workitem eviction; the bytes stay in the file-level cache for any
  subsequent `SharedRegion` covering the same range.
- **Cross-column coalescing** (#374). Coalesced regions are
  *exactly* the right cache unit — they're the typical
  `readRange(off, len)` shape after coalescing. Repeat dive
  refills against the same window become single-region cache hits.
- **Local files.** Unchanged. `MappedInputFile` already does the
  whole-file backing; the new `S3InputFile` shape just brings the
  remote path into structural alignment.

## Limits

- **2 GB cap.** Java's `MappedByteBuffer.slice(int, int)` and
  `FileChannel.map` are `int`-sized. A single 2 GB+ file cannot be
  mmapped in one segment. The existing `MappedInputFile`
  documentation already calls this out as a known limit
  (`ParquetFileReader` JavaDoc). Whole-file backing inherits it.
  The right fix is the multi-region mmap design (#75 on the
  roadmap, "multi-release mapping to bypass 2GB chunk limit") —
  same problem in two places, same solution.
- **Many-file workloads.** Each open `S3InputFile` reserves
  `fileSize` of address space + temp-file disk. Opening 50 × 1 GB
  files reserves 50 GB even if most files contribute one row group.
  Mitigated in practice by sparse-file lazy commit (real disk
  usage = touched bytes), but virtual address space is still
  reserved. For workloads that need a hard global memory cap, an
  LRU is the right shape and can be added later with the same
  underlying `RangeSet` abstraction.
- **Temp directory required.** The host needs a writeable temp dir
  with enough free space for worst-case touched bytes per file.
  `/tmp` typically suffices; environments with read-only or tiny
  `/tmp` (some container images) need `tempDir` configured.

## Concurrency

`S3InputFile.readRange` is called from many threads (column workers,
prefetch). The cache must be thread-safe:

- `RangeSet` operations under a single object monitor (or
  `ReentrantReadWriteLock` if profiling shows contention).
- Per-range fetch deduplication: when two threads simultaneously
  request the same missing range, exactly one issues the HTTP GET
  and the other waits. Implemented via a
  `ConcurrentHashMap<Range, CompletableFuture<Void>>` of in-flight
  fetches. Lookup and removal happen under the `RangeSet` lock to
  avoid a ABA window.
- Slices into the mmap are zero-copy `MappedByteBuffer.slice(int,
  int)` views; consumers can hold them across threads, the
  underlying mapping is alive until `close()`.

## Testing

- **`S3RangeCacheHitTest`** — open a `CountingS3Api` wrapper,
  read the same range twice, assert exactly one HTTP GET.
- **`S3RangeCachePartialHitTest`** — read `[0, 1000)`, then read
  `[100, 200)`. Assert the second read does not issue a network
  call (covered by the populated set).
- **`S3RangeCacheGapFetchTest`** — populate `[0, 100)` and
  `[200, 300)`, then read `[0, 300)`. Assert exactly one HTTP GET
  is issued for `[100, 200)` (the gap), the other ranges are
  served from the buffer.
- **`S3RangeCacheConcurrentFetchTest`** — two threads request the
  same uncached range simultaneously. Assert exactly one HTTP GET
  is issued.
- **`S3RangeCacheCloseTest`** — close the file, verify the temp
  file is deleted and the mapping is released.
- **`S3RangeCacheCrossRgFlipFlopTest`** — end-to-end: open a
  fixture against `LocalStack`, walk through dive's preview
  pattern (open → page-down × N → jump-to-end → jump-to-start),
  assert the second jump-to-start issues zero new HTTP GETs.

## Out of scope

- **LRU eviction.** Tracked separately if many-file or huge-file
  workloads surface a need.
- **Cross-process cache.** Each JVM has its own temp file; no
  inter-process sharing. Adding that would mean a stable cache
  directory keyed by `(bucket, key, etag)`, eviction by file-system
  capacity — different problem entirely.
- **Conditional GETs / `If-Match` revalidation.** Assumes the S3
  object doesn't change for the lifetime of an open `S3InputFile`.
  Reasonable for dive sessions; would need revisiting for
  long-lived readers (hours+).
- **Multi-region mmap for files > 2 GB.** Tracked as #75.

## Estimated effort

~150 lines of production code (RangeSet ~40, S3InputFile changes
~70, S3Source config ~20, lifecycle plumbing ~20) + tests.
Roughly half a day end-to-end including the dive verification run
against the Overture fixture.
