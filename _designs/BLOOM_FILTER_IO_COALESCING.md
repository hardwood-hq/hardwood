# Design: coalesced, parallel bloom filter reads during row-group pruning

**Status: Implemented.** Tracking issue: #735.

## Goal

Bloom-filter predicate push-down (#105) fetches a column's bloom filter during work-list
construction, before any data page is read. Today each filter is a standalone ranged GET,
issued serially: `RowGroupIterator.filterRowGroups` evaluates row groups one at a time, and
`RowGroupBloomFilterSource.forColumn` issues an `inputFile.readRange(offset, length)` per
`(row group × predicate column)`. On remote backends this is latency-shaped exactly wrong —
many small sequential GETs — even though a column's filters across row groups are typically
written contiguously and could be fetched as one, and filters that are not adjacent could
still be fetched concurrently.

The data-page path already coalesces (`coalesceAcrossColumns`, `SharedRegion`, `ChunkHandle`)
and prefetches (`prefetchNextRowGroup`); bloom reads happen earlier, in `filterRowGroups`,
and bypass all of it. This design brings the same I/O shape to bloom filters without
changing which row groups are dropped.

## What changes

Three phases, in `dev.hardwood.internal.predicate`, all invisible to callers — no public
API changes:

1. **Plan.** Before the decision loop, a planner computes which `(row group, column)` pairs
   may consult a bloom filter, using metadata only. It runs
   `RowGroupFilterEvaluator.decideRowGroup` itself with a recording `BloomFilterSource`
   that answers `null` for every column and records which columns are asked for. Because a
   leaf's bloom check is reached only when its statistics decision is not `CANNOT_MATCH`,
   and bloom/dictionary results can only short-circuit evaluation *earlier* (never later),
   the recorded set is a superset of the columns the real evaluation will consult — and the
   eligibility rules (EQ/IN, physical types, NaN, FLOAT16) are the evaluator's own, never
   re-implemented.
2. **Fetch.** The planned candidates become byte regions — `[offset, offset+length)` for
   candidates with a footer-declared `bloom_filter_length`; for legacy candidates
   (length absent), a parallel 64-byte header probe derives the length first, and filters
   that fit the probe window are complete after that single read. Regions are sorted by
   offset and greedily merged, then fetched concurrently on the common pool with
   `CompletableFuture.allOf(...).join()` before the decision loop starts. Every fetched
   buffer is published through a pre-assigned slot; the coordinating thread assembles the
   lookup after the join, so no map is written from the pool.
3. **Evaluate, unchanged.** The existing sequential decision loop runs as before. Each
   row group's `RowGroupBloomFilterSource` receives a lookup over the prefetched bytes; a
   hit parses the filter from the cache with no `readRange`, a miss falls through to the
   existing lazy path.

`RowGroupFilterEvent` and every prune decision are unchanged.

## Constants

- **`BLOOM_COALESCE_GAP_BYTES`** — 64 KB, matching `MAX_CROSS_COL_GAP_BYTES`. Filters for
  a column across row groups are normally back-to-back; any real gap is padding or another
  column's filter, and bridging a small gap costs far less than a round trip.
- **`BLOOM_MAX_REGION_BYTES`** — 16 MB. Bloom filters are small (kilobytes at Parquet's
  default NDV/FPP); the cap exists only to bound a pathological footer, not to shape
  normal traffic.
- **`HEADER_PROBE_BYTES`** — 64 bytes, unchanged from the existing legacy probe; the clamp
  arithmetic moves into the shared probe helper so the lazy path and the planner cannot
  diverge.

## Legacy files (bloom_filter_length absent)

Writers that omit `bloom_filter_length` cost two reads per filter today: a probe window to
learn the bitset size, then the full region. The planner pays at most two *phases* total:

1. Probe every legacy candidate's header concurrently (one clamped 64-byte read each).
   `inputFile.length()` is obtained once per file and reused for every clamp. A probe
   window that already contains the whole filter needs no second read — the filter is
   complete after phase 1.
2. Candidates whose bitset extends past the window contribute exact
   `[offset, offset + totalLength)` regions, merged and fetched in the same pass as the
   length-known candidates.

## Prefetch lookup

The planner hands each row group's source a lookup keyed by `bloom_filter_offset`:

- A hit returns the exact filter bytes — a buffer positioned at the filter start plus the
  filter's total length (footer-declared, or probe-derived for legacy candidates). The
  source parses it through the same known-length code path it already has; validation,
  warnings and exception contexts stay in one place.
- A miss — not prefetched, prefetch disabled, skipped candidate, or failed fetch — falls
  through to the existing lazy path, which re-issues the read and reproduces today's exact
  behavior.

The lookup never changes an outcome: it only removes round trips when it can.

## Failure handling

Prefetch is best-effort. A region fetch, probe, or `length()` call that fails is logged at
DEBUG and treated as absent; the affected filters fall back to the lazy path, which
re-issues the read and surfaces today's exception type, context, and timing. An invalid
`bloom_filter_offset` (≤ 0) and a chunk naming another file (`requireSameFile`) are
excluded from prefetch and left to the lazy path's existing warning / exception.

## Opt-out

The internal system property `hardwood.internal.bloomPrefetch` (default `true`) disables
planning and prefetch, leaving the lazy path alone. Read per planner invocation, not
cached in a static field.

## Testing

- The planner is unit-tested over `bloom_filter_test.parquet` and synthetic metadata: EQ /
  IN / AND / OR candidates, statistics-short-circuited leaves, NaN (no consult), signed
  zero (consult), FLOAT16 (no consult), invalid offsets, split-file chunks, out-of-bounds
  indices, and the over-approximation bound (planner candidates ⊇ real consults).
- `BloomFilterIoCoalescingTest` pins the request counts over a multi-row-group fixture
  with contiguous filters: materially fewer bloom GETs than row groups, zero bloom I/O for
  ineligible queries, identical decisions with prefetch on and off, single-candidate and
  legacy probe counts, and fallback behavior under injected fetch failures.
