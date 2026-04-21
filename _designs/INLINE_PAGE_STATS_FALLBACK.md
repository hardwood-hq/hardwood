# Plan: Inline `DataPageHeader.statistics` as page-level pushdown fallback (#273)

**Status: Implemented**

## Context

Parquet stores statistics in three places:

| Location | Thrift field | Status in Hardwood |
|---|---|---|
| `ColumnMetaData.statistics` (footer, per chunk) | field 12 | supported via `StatisticsReader` |
| `ColumnIndex` (out-of-band, per page) | separate struct | supported via `ColumnIndexReader` + `PageFilterEvaluator` |
| `DataPageHeader.statistics` (inline, per page) | field 5 | **now consumed** by `SequentialFetchPlan` as a per-page skip |
| `DataPageHeaderV2.statistics` | field 8 | same |

`ColumnIndex` is the modern page-level mechanism. When a writer omits it, the legacy inline per-page stats remain the only page-level signal on disk. Arrow C++ and pyarrow shipped with `write_page_index=false` as the default through release 19.0.0 (`DEFAULT_IS_PAGE_INDEX_ENABLED = false` in `cpp/src/parquet/properties.h`); the default was flipped to `true` in Arrow 20.0.0. Inline `DataPageHeader.statistics` has always been written by default (`DEFAULT_ARE_STATISTICS_ENABLED = true`). So files produced by default-configured Arrow ≤ 19.x ship no `ColumnIndex` / `OffsetIndex` but do carry inline per-page stats. Without consuming them, `PageFilterEvaluator` silently falls back to `RowRanges.all()` and page-level pushdown is a no-op for these files. Files produced by Arrow ≥ 20.0 with defaults carry `ColumnIndex`; #273 is effectively a no-op for them.

## Design principles

- **Lazy.** Inline stats are consulted only as pages are walked by `SequentialFetchPlan`. No upfront scan, no extra I/O, no double-fetch.
- **Row-aligned.** Skipped pages are replaced with a null-placeholder `PageInfo` carrying the correct `numValues`. Decoding short-circuits to an all-null typed page. Sibling columns stay row-aligned; the record-level filter drops the rows via SQL three-valued logic.
- **Conservative.** Only leaves whose falsification falsifies the whole predicate (AND-necessary leaves) drive the skip. Leaves under an `OR` are ignored. Only optional columns (`maxDefinitionLevel > 0`) participate — required columns cannot carry nulls.
- **Same abstractions.** The skip decision reuses `MinMaxStats.of(Statistics)` and `StatisticsFilterSupport.canDropLeaf` — exactly the same machinery `PageFilterEvaluator` uses for `ColumnIndex`.

## Components

### `DataPageHeader` / `DataPageHeaderV2` parsing

Both `DataPageHeaderReader` and `DataPageHeaderV2Reader` read the inline `Statistics` field (v1 field 5, v2 field 8) into a new optional `statistics` component on the internal records. `null` means "writer omitted inline stats for this page".

### `PageDropPredicates`

**File:** `core/src/main/java/dev/hardwood/internal/predicate/PageDropPredicates.java`

- `byColumn(ResolvedPredicate)` — walks the predicate AST and returns, per column, the set of AND-necessary leaves. Recurses into `And`, stops at `Or`, keeps leaves. `ResolvedPredicate` has no `Not` (negation is desugared by `ResolvedPredicate.negate`), so the walk is trivial.
- `canDropPage(leaves, Statistics)` — given a list of AND-necessary leaves for a column and the inline stats of a single page, returns `true` if any leaf proves the page cannot match. Calls `StatisticsFilterSupport.canDropLeaf(leaf, MinMaxStats.of(stats))`. Returns `false` for deprecated-only stats or when `stats` is `null`.

`StatisticsFilterSupport.canDropLeaf` already returns `false` for `IsNullPredicate` and `IsNotNullPredicate`, so those never trigger a skip — correct behaviour since a null-placeholder would falsely satisfy `IS NULL`.

### `PageInfo` — null-placeholder mode

**File:** `core/src/main/java/dev/hardwood/internal/reader/PageInfo.java`

New factory `PageInfo.nullPlaceholder(int numValues, ColumnSchema, ColumnMetaData)` that constructs a `PageInfo` with `pageData == null` and a positive `placeholderNumValues`. Accessors: `isNullPlaceholder()`, `placeholderNumValues()`. Regular construction is unchanged.

### `PageDecoder.nullPage`

**File:** `core/src/main/java/dev/hardwood/internal/reader/PageDecoder.java`

`nullPage(numValues)` allocates a typed `Page` of the column's physical type with:

- Values array: zero-filled (irrelevant — every slot is null).
- `definitionLevels`: zero-filled (`defLevel == 0 < maxDefLevel` → null for every row).
- `repetitionLevels`: zero-filled if `maxRepetitionLevel > 0`, else `null`.

Throws `IllegalStateException` if invoked on a required column — defensive guardrail; the caller (`SequentialFetchPlan`) already gates on `maxDefinitionLevel > 0`.

### `ColumnWorker.decode`

Checks `pageInfo.isNullPlaceholder()` and routes to `pageDecoder.nullPage(numValues)` instead of `decodePage(...)`. Decompression, CRC validation, and value decoding are bypassed for the placeholder path.

### `SequentialFetchPlan` — per-page skip

**File:** `core/src/main/java/dev/hardwood/internal/reader/SequentialFetchPlan.java`

A new `build(..., List<ResolvedPredicate> dropLeaves)` overload accepts the per-column AND-necessary leaves. Inside `scanNextPage`, after parsing each `DATA_PAGE` / `DATA_PAGE_V2` header:

1. If `dropLeaves` is empty OR the column is required (`maxDefLevel == 0`), decode the page normally.
2. Otherwise, pull the inline `Statistics` from the page header and call `PageDropPredicates.canDropPage`.
3. If the page can be dropped, emit `PageInfo.nullPlaceholder(numValues, ...)`. `valuesRead` and `position` advance as if the page had been decoded.

The existing peek-buffer growth (#272, now 1 KiB → 1 MiB cap) ensures headers with long `min_value` / `max_value` binaries are readable.

### `RowGroupIterator` — threading the leaves

**File:** `core/src/main/java/dev/hardwood/internal/reader/RowGroupIterator.java`

Derives `dropLeavesByColumn = PageDropPredicates.byColumn(filterPredicate)` once when the filter is set. In `computeFetchPlans`, each call to `SequentialFetchPlan.build` receives `dropLeavesByColumn.getOrDefault(originalIndex, List.of())`. `IndexedFetchPlan` is unchanged — columns with `ColumnIndex` still use the `RowRanges`-driven path.

## What #273 does NOT change

- `PageFilterEvaluator` still returns `RowRanges.all` when `ColumnIndex` is absent — it does not attempt inline-stats pushdown. Cross-column row-range pushdown requires proper `RowRanges` propagation through every column's `SequentialFetchPlan`, which is larger in scope and deferred to a follow-up.

## Surfacing in `inspect pages`

For files without a `ColumnIndex`, `InspectPagesCommand` now fills Min / Max / Nulls from the parsed inline `Statistics` carried on each `DataPageHeader`. The column headers stay identical; cells simply stop being `-` for such files. Dictionary pages remain dashed. `(deprecated)` is shown for stats carrying only legacy unsigned-sort min/max, since those bytes are unsafe to surface as typed values.

## Testing

- Unit: `PageDropPredicates` is exercised indirectly via the end-to-end tests.
- End-to-end: `PredicatePushDownTest` gains four cases using `inline_page_stats.parquet` (one row group, 10 000 rows, id required, value nullable, no ColumnIndex):
  - Leaf filter narrows result to 1 000 rows (`value < 2000`).
  - Impossible filter → empty result.
  - Trivially-true filter → all 10 000 rows.
  - `OR` of two leaves → no page drop from either branch; result correct regardless.
- `./mvnw verify` passes across all modules.

## Non-goals

- Writing inline statistics on the write path — Hardwood does not write Parquet.
- Replacing `ColumnIndex` consumption with inline stats when both are present. `ColumnIndex` stays primary.
- Proper cross-column inline-stats pushdown (where sibling columns also skip I/O on the row-ranges of dropped pages). Tracked separately; would benefit files with many projected columns where the predicate column dominates decode time.
- Required-column skipping (needs either a min-value substitution strategy or a downstream way to represent "synthetic null" in REQUIRED columns without breaking invariants).

## Rollout

- [x] Public API: no new surface — all additions are in `internal` packages.
- [x] ROADMAP §9.4 updated to reflect inline-stats support.
