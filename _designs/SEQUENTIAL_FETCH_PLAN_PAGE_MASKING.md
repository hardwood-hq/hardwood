# Plan: Per-Page Row Masking in `SequentialFetchPlan` (#371)

**Status: Proposed**

## Context

#277 / #369 introduced the per-page row mask machinery: each kept page is paired with a [`PageRowMask`](../core/src/main/java/dev/hardwood/internal/reader/PageRowMask.java) that tells the assembler which records to keep, computed from a row-group-wide [`RowRanges`](../core/src/main/java/dev/hardwood/internal/reader/RowRanges.java) via `RowRanges.maskForPage(pageFirstRow, pageLastRow)`. The mask is honoured exclusively by [`IndexedFetchPlan`](../core/src/main/java/dev/hardwood/internal/reader/IndexedFetchPlan.java) — the OffsetIndex-driven path. [`SequentialFetchPlan`](../core/src/main/java/dev/hardwood/internal/reader/SequentialFetchPlan.java) — used for any column lacking an OffsetIndex — emits every page's full content from offset 0 and ignores `RowRanges` entirely.

That leaves two gaps:

1. **Latent correctness gap in mixed-OffsetIndex configurations.** When the filter column has an OffsetIndex but a sibling projected column does not, the indexed column applies its mask while the sequential column does not. Sibling pages are no longer row-aligned. In practice this is masked by writers being all-or-nothing on Page Index, but it is the same misalignment story as #277.
2. **Tail-mode fast path is gated off** for files without Page Index. [`ParquetFileReader.canFastSkipTail`](../core/src/main/java/dev/hardwood/reader/ParquetFileReader.java) returns `false` if any projected column lacks an OffsetIndex, falling back to decode-and-discard.

The expected win is primarily **CPU**: dropped pages skip the codec, value decoding, and rep/def-level walks. I/O savings are a possible follow-up that requires `chunkSize` to adapt to `RowRanges` (see *Out-of-scope follow-ups*).

## Scope

This plan extends the per-page mask machinery to `SequentialFetchPlan` for the page formats where we can count records without invoking the codec:

- **Flat columns** (`maxRepetitionLevel == 0`), v1 or v2. The page header's `num_values` *is* the record count.
- **Nested v2** (`DATA_PAGE_V2`). Repetition levels live in an uncompressed RLE prefix sized by `repetition_levels_byte_length`; we count records (rep-level == 0) without touching the codec.

Explicitly **out of scope**:

- **Nested v1** (`DATA_PAGE`). Repetition levels are inside the compressed area, so counting records requires decompressing the page anyway. The skip-without-decompress win evaporates and the implementation balloons (`PageInfo.decompressedData` cache plumbing through to `PageDecoder` to avoid double-decompression). When *any* projected column in a row group is nested-v1-without-OffsetIndex, the entire row group falls back to today's behaviour: no masks anywhere, tail-mode decode-and-discard. See *Row-group-wide gate* below for why this has to be all-or-nothing.

## Design

### Row-group-wide gate

The gate is a row-group-level decision, not per-column. The reason: if column A has an OffsetIndex and applies a mask, and sibling column B is nested-v1-without-OffsetIndex and emits everything, A and B are no longer row-aligned (the existing #277 hazard, just with B as the offending column instead of A).

Per row group, we compute `masksApplicable` as:

```
for each projected column C:
    if C has OffsetIndex                 → mask-capable
    else if C.maxRepetitionLevel == 0    → mask-capable (flat)
    else if firstDataPageOf(C) is v2     → mask-capable (nested v2)
    else                                 → NOT mask-capable (nested v1)

masksApplicable = all columns mask-capable
```

If `masksApplicable` is false, the row group's effective `matchingRows` is forced to `RowRanges.ALL` for *every* plan in that row group, and tail-mode falls back to decode-and-discard. The IndexedFetchPlan path is bypassed in that case as well, preserving the alignment invariant.

If `masksApplicable` is true, every plan applies masks as expected — `IndexedFetchPlan` as today, `SequentialFetchPlan` via the new path described below.

### Detecting v1 vs v2 at plan time

The format check is the only new I/O introduced by this plan. It runs in `RowGroupIterator.computeFetchPlans` immediately before plans are built, and only when both:

1. `matchingRows.isAll()` is false (no filter pushdown, no tail skip → mask wouldn't affect anything anyway), **and**
2. There is at least one projected nested column lacking an OffsetIndex.

For each such column we peek the first data page header by reading a small range starting at `metaData.dataPageOffset()` (or `dictionaryPageOffset()` if present, walking past the dictionary page). The peek is bounded by `INITIAL_PAGE_HEADER_PEEK_SIZE` (1 KiB, with the existing growth-on-EOF loop); typically far less than a single network round-trip. Detection happens once per row group and is cached on `SharedRowGroupMetadata`.

The check returns one of:

- `MaskCapability.YES` — every nested-without-OffsetIndex column is v2 (or there are no such columns)
- `MaskCapability.NO`  — at least one such column is v1

`MaskCapability.NO` causes `RowGroupIterator.computeFetchPlans` to override the local `matchingRows` to `RowRanges.ALL` before building any plan in this row group. The `SharedRowGroupMetadata.matchingRows()` getter exposes the *original* unfiltered ranges (predicate pushdown at the row-group level may still drop entire row groups). Only the per-page application is suppressed.

For files where every column has an OffsetIndex (parquet-mr default since 1.11), no peek is performed and plan time is unchanged.

### `SequentialFetchPlan` accepts `RowRanges`

Pass `matchingRows` and `rowGroup.numRows()` through `SequentialFetchPlan.build(...)`:

```
SequentialFetchPlan.build(
    inputFile, columnSchema, columnChunk,
    context, rowGroupIndex, fileName,
    perRgMaxRows, leaves,
    matchingRows,       // new (RowRanges.ALL when gate is closed)
    rowGroupRowCount)   // new — needed only for the last page's pageLastRow
```

`RowRanges.ALL` triggers the existing fast path with no per-page work added (a single `isAll()` check at the top of `scanNextPage`). All current callers either pass `RowRanges.ALL` or the row-group's matching ranges.

### Cursor model

The iterator gains a `recordsRead` cursor in addition to the existing `valuesRead`:

- For flat columns, `recordsRead == valuesRead` always (one value per record).
- For nested v2, the two diverge — `recordsRead` advances by the count of `rep == 0` levels; `valuesRead` advances by `header.num_values`.

On entry to `scanNextPage()`:

```
long pageFirstRow  = recordsRead;
int  recordsInPage = recordCountForPage(header, position, headerSize);
long pageLastRow   = pageFirstRow + recordsInPage;
PageRowMask mask   = matchingRows.maskForPage(pageFirstRow, pageLastRow);
```

`recordCountForPage` is the only new decode-aware logic:

| Column shape                                    | Source                                                                                       |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------- |
| Flat (`columnSchema.maxRepetitionLevel() == 0`) | `header.num_values` (v1 or v2).                                                              |
| Nested, `DATA_PAGE_V2`                          | Walk the rep-level RLE prefix (`header.dataPageHeaderV2().repetitionLevelsByteLength()`), count `rep == 0`. Bytes are uncompressed and live immediately after the page header. |
| Nested, `DATA_PAGE` (v1)                        | Unreachable — the gate has already promoted `matchingRows` to `RowRanges.ALL`, so this code path is short-circuited at the top of `scanNextPage` and `recordCountForPage` is never called. |

The rep-level walk lives in a small static helper alongside the existing rep/def-level decoding code; it counts zeros without materialising a level array.

### Skip-without-decompress

After computing `mask`:

```
if (mask == null) {
    position    += totalPageSize;       // header + compressedPageSize
    recordsRead += recordsInPage;
    valuesRead  += headerNumValues;     // see invariants below
    pageCount++;                        // dropped pages still count for telemetry
    continue;                           // try the next page
}
ByteBuffer pageData = readBytes(position, totalPageSize);
PageInfo pageInfo = new PageInfo(pageData, columnSchema, metaData, dictionary, mask);
```

The crucial property: when `mask == null` we do **not** call `readBytes(position, totalPageSize)` for the body. The page header bytes are already in the chunk handle (we just parsed them); the body bytes are skipped from the iterator's working set. They may still have been fetched as part of a larger chunk (chunk sizing is unchanged in this plan), so the immediate win is CPU — no codec invocation, no value decoding, no rep/def-level walk for dropped page bodies. Adapting `chunkSize` to `RowRanges` is left as a follow-up.

### Coexistence with inline-stats null-placeholders

`SequentialFetchPlan` already has a separate page-drop mechanism: `canDropByInlineStats` emits `PageInfo.nullPlaceholder(numValues, ...)` when inline `Statistics` prove no row can match. With masks plumbed in:

```
if (mask == null) {
    /* drop the page entirely — no placeholder, no read */
    advanceCursors();
    continue;
}
PageInfo pageInfo;
if (canDropByInlineStats(header)) {
    int placeholderRecords = mask.isAll() ? recordsInPage : mask.totalRecords();
    pageInfo = PageInfo.nullPlaceholder(placeholderRecords, columnSchema, metaData);
}
else {
    ByteBuffer pageData = readBytes(position, totalPageSize);
    pageInfo = new PageInfo(pageData, columnSchema, metaData, dictionary, mask);
}
```

Three cases worth calling out:

1. **`mask == null`**: the row-range drop wins; no placeholder is emitted (sibling columns must also drop these rows entirely).
2. **`mask != null`, inline stats also drop**: the placeholder reports `mask.totalRecords()` rows, not `recordsInPage`, so cross-column row counts agree.
3. **`mask.isAll()`, inline stats drop**: `recordsInPage` is used, matching today's behaviour for flat columns. For nested v2 this is the *record* count (top-level rows), not the value count — matching what the assembler expects from a placeholder.

Today's flat-only inline-stats path is unaffected when `matchingRows.isAll()` (no `recordCountForPage` call, `recordsInPage` defaults to `header.num_values` which equals the record count for flat).

### Invariants

The existing value-count guard (`valuesRead == metaData.numValues()` at end-of-iteration) is load-bearing — it has caught real bugs in the past. The new code must preserve it under skipped pages. Concrete invariants:

- **`valuesRead` accumulates `header.num_values` for every data page encountered**, kept or dropped. The existing guard continues to fire end-of-iteration.
- **`recordsRead` accumulates `recordsInPage` for every data page encountered**. End-of-iteration must satisfy `recordsRead == rowGroupRowCount` for non-empty column chunks (a new guard worth adding alongside the existing one — protects against rep-level walk bugs).
- **`position` advances by `totalPageSize` for every page, kept, dropped, or null-placeholder.**

A dedicated test exercises a multi-page column where the mask drops some pages, keeps some, and trims the straddling one, asserting both guards hold.

### Tail-mode gate

`ParquetFileReader.canFastSkipTail` keeps its name and signature but loosens the check to delegate to the same `MaskCapability` logic used by `RowGroupIterator`. For files where every projected column has an OffsetIndex, behaviour is unchanged. For files where some columns lack OffsetIndex, the gate now opens iff every such column is flat or nested-v2. The synthesized `RowRanges.range(skip, numRows)` flows through the same plumbing as a filter `RowRanges`.

The detection happens at row-group level via the cached `SharedRowGroupMetadata.maskCapability()`. When `canFastSkipTail` runs (in `buildTailRowReader`, before the row reader is built), the metadata cache is empty, so detection is performed inline. The cost is a single small read per row group with at least one nested-without-OffsetIndex column — typical files have zero such columns and pay nothing.

### Trailing-page early-exit

Without OffsetIndex we discover page boundaries by sequential header scanning, so we *must* scan up through and including the last kept page — there's no shortcut for finding row N otherwise. But the trailing region — pages where `pageFirstRow >= matchingRows.endRow()` — produces a `null` mask for every page and contributes nothing. The existing iterator would still parse every one of those headers before tripping `valuesRead == metaData.numValues()` and stopping.

For a filter matching `[50 000, 60 000)` in a 1 M-row group with ~1024-row pages, that's ~920 wasted header parses per column; wide schemas multiply by column count. The cost scales linearly with the gap between the last matching row and the end of the row group — exactly where selective filter pushdown is supposed to be a win.

The fix is small enough to fold into this plan:

1. Add `RowRanges.endRow()` returning the `end` of the last interval (or `Long.MAX_VALUE` for `RowRanges.ALL`, which short-circuits the check below).
2. In `SequentialPageIterator.checkHasNext`, short-circuit when the cursor has crossed the last matching row:

   ```java
   if (!matchingRows.isAll() && recordsRead >= matchingRows.endRow()) {
       exhausted = true;
       earlyExited = true;
       emitEvent();
       return false;
   }
   ```

3. Loosen the existing `valuesRead == metaData.numValues()` guard so it doesn't false-positive after a deliberate early-exit. Either skip the check when `earlyExited`, or replace it with a stronger invariant: "we exited deliberately, *or* we consumed every value." The mismatch-detection intent (the column chunk's metadata disagrees with the page stream) is preserved for the unfiltered path.

The tail-mode case (`RowRanges.range(skip, total)`) extends to end-of-row-group, so `matchingRows.endRow() == rowGroupRowCount` and the early-exit fires only when iteration would have ended anyway — no behavioural change for tail mode. Filters that match near end-of-row-group similarly see no improvement. Filters that match in the middle or at the front of an unsorted row group are the beneficiaries.

This work lives at the end of the implementation, after the core mask plumbing is in and tested — it's a self-contained add-on with its own test.

### Out-of-scope items, called out explicitly

- **`maxRows + filter` semantics under masking.** `checkHasNext` early-stops on `valuesRead >= maxRows`. With masks, dropped pages still increment `valuesRead`, so truncation is against page-value-count, not kept-record-count. `IndexedFetchPlan`'s `truncateToMaxRows` is page-aware, so the two plans behave differently under combined `maxRows + filter`. This plan does not unify them — both behaviours are pre-existing and changing them is a separate question.
- **`chunkSize` adapting to `RowRanges`.** Today's chunk sizing is driven by `metaData.totalCompressedSize` and `maxRows`. Teaching it about `RowRanges` (e.g. for tail mode, fetch only the bytes after `pageFirstRow >= skip`) would convert the CPU win into an I/O win on remote backends. Not done here; would need to interact with cross-column coalescing.
- **Closing the mixed-OffsetIndex correctness gap when nested-v1 is present.** This requires the decompress-and-cache path. Today's row-group-wide fallback preserves the pre-#371 behaviour; closing the gap is a separate follow-up.
- **Multi-file tail reads.** `buildTailRowReader` rejects multi-file readers — orthogonal.

## Test plan

- **Fixture** `tools/simple-datagen.py` → `misaligned_pages_no_index.parquet`: same shape as `misaligned_pages.parquet` (narrow INT32 + wide BYTE_ARRAY, coprime per-column page periods) but written with column index disabled. Both columns flat → fully covered by the new mask path.
- **Fixture** `tools/simple-datagen.py` → `misaligned_pages_nested_v2.parquet`: a nested-v2 column without OffsetIndex alongside a flat column, with deliberately misaligned page boundaries. Drives the rep-level walk path.
- **Fixture** `tools/simple-datagen.py` → `nested_v1_no_index.parquet`: a nested-v1 column without OffsetIndex (parquet-mr writer with `WriterVersion.PARQUET_1_0`). Drives the gate's fallback path.
- **`MisalignedPageBoundariesNoIndexTest`** mirrors `MisalignedPageBoundariesTest` against the flat no-index fixture:
  - `filterOnNarrowReturnsCorrectlyAlignedWideValues` — drives the correctness fix.
  - `tailReadReturnsAlignedRowsViaPerPageMaskFastPath` — drives the perf fix.
  - `fullScanStillPairsColumnsCorrectly` — regression guard.
- **`NestedV2NoIndexMaskingTest`** — same shape, nested v2 fixture; assertions cover record-count vs value-count semantics.
- **`NestedV1NoIndexFallbackTest`** — asserts that a row group containing a nested-v1 column without OffsetIndex falls back to today's behaviour: results are correct, tail-mode uses decode-and-discard (assertable via `RowGroupScannedEvent.scanStrategy`), and sibling indexed columns also bypass mask application.
- **Flip** `MisalignedPageBoundariesTest.tailReadFallsBackWhenColumnsLackOffsetIndex` to a positive assertion. The `inline_page_stats.parquet` fixture is flat, so the gate now opens.
- **`SequentialFetchPlanRowRangesTest`** drives `SequentialFetchPlan.build` directly with a synthesized `RowRanges` and verifies per-page drop / mask attachment for flat v1, flat v2, and nested v2; also verifies the `valuesRead` and `recordsRead` invariants.
- **Early-exit test** — drives a filter whose last matching row lies well before end-of-row-group on a multi-page column and asserts the iterator stops scanning headers after the last kept page (assertable via `pageCount` on the emitted `RowGroupScannedEvent`, or by counting header parses through a test hook).
- **Existing `inline_page_stats.parquet` predicate-pushdown tests** must continue to pass.
- **Performance** — `TailReadBenchmarkTest` extended with a no-OffsetIndex variant. Confirms the new fast path picks up the expected speedup; OffsetIndex case must not regress.

## Acceptance criteria

- New no-OffsetIndex fixtures (flat, nested-v2, nested-v1) exist and tests pass.
- Filter pushdown on the flat and nested-v2 fixtures is row-aligned across columns.
- Tail-mode on the flat and nested-v2 fixtures is row-aligned and routes through the per-page mask fast path.
- Nested-v1 fixture: tail-mode falls back to decode-and-discard; predicate pushdown continues to work via row-group filtering and row-reader-level filtering; no silent corruption.
- `MisalignedPageBoundariesTest.tailReadFallsBackWhenColumnsLackOffsetIndex` flips from "fallback expected" to "fast path expected" and is renamed.
- `valuesRead == metaData.numValues()` and `recordsRead == rowGroupRowCount` invariants hold after iteration in all paths *except* the deliberate early-exit, which is detectable via an `earlyExited` flag.
- Trailing-page early-exit fires when `recordsRead >= matchingRows.endRow()`; tail-mode and unfiltered scans are unaffected.
- All existing predicate-pushdown and inline-stats tests continue to pass.
- `./mvnw verify` is green.
