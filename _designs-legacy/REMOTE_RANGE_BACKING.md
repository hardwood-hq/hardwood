# Design: whole-file range backing for `S3InputFile`

**Status: Completed.** Tracking issues: #373, #560, #853.

## Goal

Cache byte ranges fetched by an `S3InputFile` so repeat reads of the
same offsets hit local memory instead of re-issuing HTTP GETs to S3.
The motivating workflow is dive's Data preview path: flip-flopping
between top and bottom of a remote file (`g`, `G`, `g`, …) currently
re-fetches the same row groups every time.

This sits one layer below the cross-column coalescing already
introduced in #374. Coalesced regions become coarser cache units —
one cached entry per region covers many columns at once.

## Approach: whole-file backing

`MappedInputFile.open()` calls `channel.map(READ_ONLY, 0, fileSize)`,
reserves the full file as virtual address space, and lets the OS
lazy-fault pages on `slice` calls. There is no cache structure
because the mapping *is* the cache.

The remote analogue is a cache layer wrapping the network path:

- On `open()`, allocate a buffer of `fileSize` bytes and track which
  byte ranges have been populated.
- On `readRange(offset, length)`: if the range is fully within the
  populated set, return a slice of the buffer — zero-copy, no
  network I/O. If not, fetch the missing sub-range(s) through the
  wrapped file, write them into the buffer at their offsets, mark the
  ranges populated, and return the slice.
- On `close()`, release the buffer.

No eviction. The buffer's lifetime is the open file's lifetime; its
size is the file's size. This is structurally symmetric with the
local mmap path — both expose "a buffer of size `fileSize` you can
slice into," and the rest of the pipeline doesn't know which is
which.

The trade-off is footprint: whole-file backing reserves `fileSize` of
*virtual* address space up front, regardless of how much of the file
the session actually touches. This matters for files > 2 GB or
many-file workloads — see [Limits](#limits) below — but is fine for
the dive workflow against typical analytics files.

## Implementation: sparse temp file + mmap

The buffer is backed by a **sparse temp file**, which lets the OS
handle lazy commit rather than committing `fileSize` up front:

```
open():
    open the wrapped fetcher (which discovers `fileSize` and
        pre-fetches the tail);
    create a temp file in the configured temp dir;
    sparse-truncate to `fileSize` (`FileChannel.truncate`, which on
        Linux/macOS / NTFS leaves a file with holes);
    mmap it READ_WRITE.

readRange(off, len):
    if filled.contains([off, off + len)):
        return mapping.slice((int) off, len)        // zero-copy
    else:
        fetch the missing sub-ranges from the wrapped fetcher;
        write each into the mapping at its absolute offset;
        filled.add(those ranges);
        return mapping.slice((int) off, len)

close():
    drop the mapping reference (the unmap itself is GC-driven);
    delete the temp file, deferring to JVM exit on platforms that
        refuse to unlink a file that still carries a mapping;
    close the wrapped fetcher
```

The OS commits pages only when written; never-touched holes occupy
neither RAM nor disk on filesystems that support sparse files (ext4,
xfs, apfs, ntfs). Logical address space = `fileSize`, real footprint
= touched bytes.

The cost: an
extra disk write per fetched range (S3 → mmap'd file). On a
`tmpfs`-backed temp dir this stays in RAM (Linux default for
`/tmp` on many distros). On a disk-backed temp dir it incurs disk
I/O proportional to bytes fetched — typically a fraction of the S3
RTT savings, so net positive.

### Class layout

Three classes, each with one job:

- **`RangeBackedInputFile`** (`dev.hardwood.internal.reader`) — the
  sparse-tempfile cache. An `InputFile` decorator over another
  `InputFile`; it knows nothing about S3 and is reusable over any
  remote backend. Holds the mapping and the `RangeSet`.
- **`S3Fetcher`** (package-private in `dev.hardwood.s3`) — the S3
  network path. Owns the `S3Api` handle, the bucket/key, the
  `open()` suffix-range tail fetch and its retained tail buffer, and
  the two network counters. Knows nothing about range caching.
- **`S3InputFile`** (public) — a facade over a single `InputFile`
  chosen in the constructor: the `S3Fetcher` under
  `RangeBacking.NONE`, or a `RangeBackedInputFile` wrapping it under
  `RangeBacking.SPARSE_TEMPFILE`. `open()`, `readRange()` and
  `close()` delegate to it unconditionally, so the read path carries
  no per-call branch on the backing mode.

Every `InputFile` method delegates to the chosen backing, including
`length()` and `name()`. `S3InputFile` keeps a direct reference to the
`S3Fetcher` for one purpose: `networkRequestCount()` /
`networkBytesFetched()` report network traffic only, and under
`SPARSE_TEMPFILE` a cache hit is served from the mapping and never
reaches the fetcher, so it cannot be counted.

The facade exists because `S3Source.inputFile(...)` returns
`S3InputFile`, not `InputFile`: callers reach the counters without a
cast, `ParquetModel#netStats()` can `instanceof S3InputFile`, and the
internal `RangeBackedInputFile` type never appears in public API.

## API surface

`S3InputFile` exposes the same `InputFile` contract; the
range cache is an internal implementation detail and does not change
the returned type. Configuration lives on `S3Source`:

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

- **Tail cache.** Lives in `S3Fetcher`, one layer *below* the range
  cache: the `open()` suffix-range GET fetches
  `[fileLength - TAIL_SIZE, fileLength)` and the fetcher serves any
  read fully inside that window from it without a network call. Under
  `SPARSE_TEMPFILE` a footer read therefore populates the mapping from
  the tail buffer rather than from the network, and neither layer
  counts it as network traffic.
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
  whole-file backing; the `S3InputFile` shape brings the remote path
  into structural alignment.

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
  `S3Source.Builder#build()` rejects a missing or read-only `tempDir`
  when `SPARSE_TEMPFILE` is selected, so the misconfiguration surfaces
  at configuration time rather than at the first `open()`.
- **Deferred reclamation.** `close()` deletes the backing file, but
  the unmap is GC-driven: the address space, and on Windows the file
  itself, are reclaimed at the next collection or at JVM exit rather
  than at `close()`. Java offers no portable forced unmap at the
  language level the project targets.

## Concurrency

`S3InputFile.readRange` is called from many threads (column workers,
prefetch). The cache must be thread-safe:

- `open()`, `readRange()` and `close()` on `RangeBackedInputFile`
  hold the instance monitor, which guards both the mapping and the
  `RangeSet`.
- Holding that monitor across the refill also deduplicates fetches:
  two threads requesting the same missing range serialize, and the
  second finds the range populated and issues no HTTP GET. Readers
  never observe a partially written range.
- Slices into the mmap are zero-copy `MappedByteBuffer.slice(int,
  int)` views; consumers can hold them across threads, the
  underlying mapping is alive until `close()`.

## Testing

- **`RangeSetTest`** (core) — the interval set in isolation:
  `contains` / `add` merging of overlapping and touching ranges, and
  the gaps returned by `missing`.
- **`RangeBackedInputFileTest`** (core) — the decorator over a
  counting in-memory `InputFile`: exact-match and enclosed-range
  repeat reads hit the mapping, a read spanning a hole fetches only
  the gap, and `close()` deletes the temp file.
- **`S3RangeBackingIT`** (s3) — end-to-end against an S3 proxy
  container with `RangeBacking.SPARSE_TEMPFILE`: an exact repeat
  `readRange` issues no new GET, and a second full pass with
  `ColumnReader` / `RowReader` over the same `S3InputFile` issues
  strictly fewer GETs and fetches strictly fewer bytes than the
  first. The counters carry the assertions, which is what pins the
  cache to network-only accounting.
- **`S3InputFileIT`** (s3) — the same file under the default
  `RangeBacking.NONE`, including that the counters cover the
  `open()` tail fetch and every network `readRange` but not
  tail-cache hits.
- **`S3SourceTempDirValidationTest`** (s3) — `build()` rejects a
  missing `tempDir` under `SPARSE_TEMPFILE`, accepts an existing one,
  and ignores the setting under `NONE`.

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
