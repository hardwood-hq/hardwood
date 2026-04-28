# Design: dive Data preview row window cache

**Status: Implemented.** Tracking issue: #377. PR: this branch.

## Goal

Maintain a bounded buffer of pre-formatted Data preview rows around the
current viewport, so within-window navigation (typical PgUp/PgDn) triggers
zero I/O. Sliding the window across its edges fetches incrementally; far
jumps refill in one shot.

This subsumes the old `DataPreviewScreen.PAGE_CACHE` (and the `head(cap)`
workaround the previous version of #361 needed) under a single, predictable
buffering layer.

## Behaviour

**Refill-on-miss semantics.** The window covers a range `[start, end)` of
absolute row indices, sized at `20 × pageSize` (clamped at file boundaries).
On each `slice(model, firstRow, pageSize, logicalTypes)`:

- **Page fully inside the buffer:** return the slice — zero I/O.
- **Page outside the buffer:** *replace* the buffer with a fresh range
  `[firstRow − 10 × pageSize, firstRow + 10 × pageSize)` (clamped to file
  bounds), fetched in a single `readPreviewPage` call. As the user
  navigates forward across a miss boundary (e.g. firstRow advances from
  300 to 600), the previous front rows naturally fall outside the new
  range and get evicted — memory stays bounded at `20 × pageSize`
  rather than growing.
- **`logicalTypes` toggle does *not* invalidate.** Each cell is
  pre-formatted in **both** modes on fetch — `format(reader, …, true)`
  and `format(reader, …, false)`, plus the corresponding
  `formatExpanded` variants. Toggling `t` in dive just selects which
  pair of strings the slice returns; no fetch, no decode. The cost is
  ~4× the per-cell string memory (still a few MB total for typical
  Overture-shape schemas), bounded by the window size and far cheaper
  than re-fetching on every toggle.

Concrete trace, pageSize=30:

```
slice(firstRow=0)    → buffer=[0, 300)    1 fetch (clamped at file start)
slice(firstRow=30)   → page in buffer     0 fetches
... up to firstRow=270                    all hit
slice(firstRow=300)  → MISS, buffer=[0, 600)   1 fetch
slice(firstRow=330)  → page in buffer     0 fetches
... up to firstRow=570                    all hit
slice(firstRow=600)  → MISS, buffer=[300, 900) 1 fetch (rows [0, 300) drop)
```

Refill-on-miss is the right shape here — not "slide on every edge crossing."
With slide-on-edge, every PgDn that crosses the upper bound triggers a
small fetch (one page worth) and racks up many round-trips during typical
PgUp/PgDn navigation. With refill-on-miss, the user can navigate freely
within the ±10 × pageSize horizon with zero I/O; only crossing the
boundary triggers a refill, and the refill spans the whole range so the
*next* batch of within-window navigation is also free.

The window is bound to one `ParquetModel` instance; switching files
(a new model) clears the buffer.

## Memory

For a 30-row viewport and Overture-shaped schemas (~50 top-level fields),
the resident buffer is ~21 × 30 × 50 = ~31,000 cells × ~50 chars × 2
(`format` + `formatExpanded`) ≈ a few MB. Predictable and bounded;
doesn't depend on row group sizes.

## Bounded cursor — the `head(count)` rule

Each `ParquetModel.readPreviewPage(firstRow, count, …)` call builds
the cursor with `firstRow(firstRow).head(count)`, where `count` is
the row count the caller passed. That bound is load-bearing: without
it, the per-column workers in the v2 pipeline race many row groups
ahead of what the consumer asks for (#370). The queued pre-fetch
sits blocked while the window is satisfied with a single page, then
**drains catastrophically** when *anything* later unblocks the
workers (a far-jump cursor close, a screen auto-resize, the next
consumer request) — surfacing as "phantom" S3 fetches long after the
user moved on.

Symptom from a real run before the rule was applied: cluster A
fetched ~25 MB during the initial preview, then a *second* burst of
~12 MB fetched the same byte range *87 seconds later*, triggered by
the next user keystroke. The bytes were sitting queued in the
unbounded cursor's worker pool the whole time. With `head(count)`,
each call's workers can only queue work for the rows they were asked
for; nothing leaks across cursor lifetimes.

The window's call shape is therefore exactly one of:

- Refill: `model.readPreviewPage(refillStart, 20 × pageSize, …)` —
  one big fetch covering the new range. `head(20 × pageSize)`.
- Hit: no `readPreviewPage` call at all. The slice comes from memory.

Within-window navigation never crosses the model layer; only refills
do. There are no incidental forward extensions — every miss replaces
the buffer.

## What it replaces

- `DataPreviewScreen.PAGE_CACHE` — gone. The window covers the same
  responsibility (avoid re-walking the reader on PgUp) plus the
  bounded-prefetch behaviour the old `head(cap)` workaround was reaching
  for.
- `DataPreviewScreen.cachedFor` — gone. The window tracks its bound
  model internally and invalidates on change.

## Non-goals

- **Holding typed values rather than pre-formatted strings.** Considered
  for the `logicalTypes` toggle (avoid double-cache by re-formatting on
  demand), rejected on complexity grounds: the screen renders via
  `RowValueFormatter.format(reader, …)` which calls typed `RowReader`
  accessors (`getDate`, `getTimestamp`, `getString`, `getValue`, …); a
  snapshot would need to either cache all of those per cell or
  re-implement logical-type conversions inside an adapter. Pre-formatted
  strings, with mode-toggle invalidation, are the simpler shape.
- **Cross-file windowing.** The window is tied to one `ParquetModel`
  (one file). Multi-file Data preview is not on the roadmap.
- **Async refill.** Fetches happen on the calling thread. #331 covers
  async I/O separately.

## Testing

`PreviewWindowTest`:

- `initialSliceYieldsRequestedPage` — first call returns the requested
  page with the expected row content.
- `withinWindowNavigationStaysAccurate` — forward and backward
  navigation within the window yield correct rows.
- `farJumpYieldsCorrectRows` — jump-to-last produces the right rows;
  subsequent nearby moves stay correct (no stale state).
- `logicalTypesToggleProducesFreshSlice` — toggle path goes through
  refill correctly.
- `requestNearStartClampsLowerBound` / `requestNearEndClampsUpperBound`
  — boundary clamping works.

Byte-delta assertions are not used here — the existing fixture is small
(~10 KB) so the v2 pipeline pre-fetches the whole file regardless of
which rows we request, making byte-counting at the screen layer
unreliable. The byte-level wins are visible end-to-end via dive's
`--log-file` against larger fixtures (Overture).
