# Windowed Page-Index Reads

**Status: Proposed.** Tracking issues: #708, #1113. Part of epic #1262 (`REMOTE_READ_PATH.md`, step 3).

## Goal

A read fetches the page-index slices it needs — and no others — in as few requests as the gap
policy allows, while the index bytes it holds stay bounded by a budget rather than by the size of
the file.

## How large a page index is

The page index grows with the number of pages, not with the file. Per page and column:

| Entry | Size |
|---|---|
| OffsetIndex (`offset`, `compressed_page_size`, `first_row_index`) | ~12–15 bytes |
| ColumnIndex, numeric column (min, max, null page flag, null count) | ~20–25 bytes |
| ColumnIndex, string column (bounds truncated to 64 bytes by parquet-java) | up to ~130 bytes |
| Size-statistics histograms, nested columns | more on top |

parquet-java limits a page to 20 000 rows, so narrow numeric columns get pages of tens of
kilobytes. A file of 10⁹ rows and 200 columns has about 10⁷ pages and roughly 500 MB of page
index; long string bounds or larger files reach gigabytes. Holding a whole file's page index is
therefore not an option, and neither is one request spanning it: a `ByteBuffer` holds at most
2 GB (#1113).

## The slices a read needs

For each row group it reads:

| Slice | For |
|---|---|
| OffsetIndex | every decoded column the row group reads, to plan page fetches; every filter column under page filtering, to map pages to rows |
| ColumnIndex | every filter column, when page filtering applies |

Page filtering applies under a filter with metadata filtering on, in a row group statistics left
undecided. In a row group they proved to match in full, every value of every page matches, so no
page bound can rule a page out: it is not page-filtered, needs no ColumnIndex, and its skipped
filter-only columns (`SkippedColumnFetchPlan`) need no OffsetIndex either.

A read that projects 3 of 200 columns and filters on one of them needs 4 of the 400 slices a row
group has.

Writers place the page index by structure, then row group, then column: all ColumnIndexes, then
all OffsetIndexes, each block ordered `rg0: c0 … cN, rg1: c0 … cN, …`. A column's slices are
strided through a block, so the needed slices of consecutive row groups lie close together
whenever the read needs most columns, and far apart when it needs few.

## Windows

The row groups of a file are fetched in **windows**: runs of consecutive work items of one file
whose needed slices are fetched together, in one set of merged requests per structure.

- **Formation.** Windows are formed when a file is planned, from the work items planning keeps,
  under the lock planning already holds. Each `WorkItem` carries its `IndexWindow`. A window
  extends over the following work item while the merged requests of the window's slices,
  gaps included, stay within the budget. A window always holds at least one row group, whatever
  its size. A file whose page index fits the budget is one window, fetched in one request per
  structure for the whole read. Planning forms the windows and fetches none of them.
- **Merging per structure.** The ColumnIndex slices of a window merge with each other and the
  OffsetIndex slices with each other, never one structure's slices with the other's. The gap
  between a window's last ColumnIndex slice and its first OffsetIndex slice holds the
  ColumnIndexes of every later row group, which the following windows fetch themselves. Bridging
  that gap would fetch those bytes once per window. Merged per structure, consecutive windows
  fetch disjoint bytes and the read fetches every byte of the index at most once. A filtered
  read issues one request per structure and window, an unfiltered read one per window.
- **Budget.** `hardwood.internal.indexWindowBytes`, 16 MiB by default, counted in bytes fetched:
  the needed slices plus the gaps the merges bridge between them, summed over both structures.
  It bounds what a window holds; a read that stops early has fetched at most one budget of index
  it does not use. The page index is a small fraction of the data the read fetches ahead of its
  consumer anyway, so the budget is not a reason to fetch less at a time.
- **Fetch.** The first request for any member's index buffers fetches the whole window: its
  slices merged under `CoalescingPolicy` per structure, one `readRange` per merged request. The prefetch of the
  next row group issues this request for the next window when the read enters the last row
  group of the current one.
- **Release.** A window counts down its members as `RowGroupIterator.releaseWorkItem` releases
  them and drops its merged buffers when the last one is released. Columns advance on their own
  cursors, so the windows holding buffers run from the one the slowest column is in to the one
  the fastest column's next-row-group prefetch opened.
- **Close.** A read that stops early never releases its last members, so their windows keep
  their buffers until `RowGroupIterator.close()`, which releases every window of the read. A
  closed iterator holds no index bytes, however long the work list stays reachable.

Windows are formed from the work list, so a row group statistics or bloom filters drop has no
slices fetched. A row group its dictionaries drop (#1259) is dropped on entry, before it asks for
its window; its slices are fetched and unused only when another member of the window asks. They
ride in the window's merged requests, so the cost is bytes, not requests.

## `IndexWindow`

```java
IndexWindow window = workItem.indexWindow();
RowGroupIndexBuffers buffers = window.buffersFor(workItem);   // fetches the window on first call
window.release(workItem);                                     // from releaseWorkItem
```

- `buffersFor` is synchronized. The first call fetches; concurrent callers — every projected
  column's retriever and the prefetch task — wait on the monitor and are served from the result.
  A failed fetch stores nothing, so the next call fetches again: a failure on the prefetch
  thread reaches the demand path as that path's own failure.
- `buffersFor` for a released member throws `IllegalStateException`. Nothing asks for one: every
  column has moved past it, and the next row group's prefetch skips a released work item and a
  closed iterator. That guard is what keeps a late prefetch task from reaching a released member.
- `computeSharedMetadata` calls `buffersFor` from its `computeIfAbsent` mapping function, after
  the dictionaries, so a row group they drop alone in its window costs no index read. The fetch
  then holds that entry's bin lock and takes no further lock but the window's monitor, which
  never waits on the map or the iterator, so no lock cycle forms. The window holds no reference
  to the iterator's caches.

## `RowGroupIndexBuffers`

A row group's buffers record which columns were fetched. `forColumn` on a column outside the
needed set throws `IllegalStateException`; on a fetched column it returns the column's buffers,
with a `null` OffsetIndex or ColumnIndex where the file has none. A column without an index and
a column the read failed to ask for are therefore distinct: the first falls back to sequential
fetching or keeps all pages, the second fails.

The `dive` TUI reads all columns' indexes of one row group; it asks for a needed set of every
column.

## `CoalescedRanges`

The windows are built on one primitive, which the bloom filters (#735) and later the data ranges
(#1264) use as well. A window holds one instance per structure it reads:

```java
CoalescedRanges ranges = CoalescedRanges.of(List<Range> needed);   // merged under CoalescingPolicy
ranges.fetch(inputFile);                                            // one readRange per merged request
ByteBuffer slice = ranges.slice(range);                             // a view, no copy
long bytes = ranges.fetchedBytes();                                 // merged request lengths, gaps included
```

- Merging stops at `CoalescingPolicy.MAX_SPAN_BYTES`. A single slice larger than that — one
  column chunk's ColumnIndex can exceed it — is a request of its own. A slice's length is an
  `i32`, so no request needs a buffer beyond 2 GB, whatever the distance between two slices.
- A range is served from exactly one merged request; no byte is fetched twice within an
  instance.
- Fetching is explicit and happens once; `slice` on an unfetched instance fails.
- `fetchedBytes` is known before fetching; window formation compares it against the budget.

## S3

`S3InputFile` answers a read that falls entirely inside the tail it fetched on open without a
request. A merged request that lies wholly in the last 64 KB is therefore free; one that starts
before the tail and ends inside it is fetched in full.

## Tests

Measured at the `InputFile` boundary with `CountingInputFile`:

- A filtered read of a multi-row-group file within one window issues one index request per
  structure and fetches only the needed slices, bridged by gaps within
  `CoalescingPolicy.GAP_BYTES`, each byte once.
- A filtered read spanning several windows fetches disjoint bytes per window: no byte of the
  index is fetched twice across the read.
- A narrow projection of a wide schema fetches the slices of the projected and filter columns
  only.
- A file whose page index fits the budget is fetched in one request per structure; with a small
  budget, each window's fetched bytes, gaps included, stay within the budget, except a single
  row group larger than it.
- A read that stops after the first row group fetches the page index once.
- An unfiltered `head(N)` read satisfied by the first row group fetches that row group's slices
  only.
- A window whose fetch fails on the prefetch thread is fetched again by the demand path, which
  reports the failure.
- Asking a row group's buffers for a column outside the needed set throws.
- Slices further apart than `MAX_SPAN_BYTES` are fetched by separate requests (#1113), and a
  slice larger than `MAX_SPAN_BYTES` by a request of its own, shown on the requests
  `CoalescedRanges` plans, without fetching.

## Delivery

One PR, in five commits. Each builds and passes on its own, carries its tests, and updates the
design text its change makes wrong.

1. **The plan.** This file, and step 3 of `_plans/REMOTE_READ_PATH.md`.
2. **`CoalescedRanges`.** The primitive and its unit tests: merging under `CoalescingPolicy`, the
   `MAX_SPAN_BYTES` split and a slice larger than it, each range served from one request,
   `fetchedBytes` before fetching, `slice` on an unfetched instance failing. Nothing uses it yet.
3. **Needed slices per row group.** `RowGroupIndexBuffers.fetch` takes the needed set and fetches
   it through one `CoalescedRanges` per structure; `forColumn` outside the set throws; `dive`
   asks for every column. The span over the whole row group and its `UnsupportedOperationException`
   go (#1113). A filtered read issues up to two index requests per row group. Tests: a narrow
   projection of a wide schema, an unfetched column, slices further apart than `MAX_SPAN_BYTES`.
   `FETCH_PLANNING.md` step 2 and the paragraph on the span describe the needed slices.
4. **Windows.** `IndexWindow`, formation in `planNextFile`, the budget,
   `computeSharedMetadata` reading from the window, release and close. The index fetch reason
   names the window's row groups (`rg=A-B indexes`). Tests: one request per structure within a
   window, disjoint bytes across windows, the budget, a read that stops early, an
   unfiltered `head(N)`, a failed prefetch fetched again on demand. `FETCH_PLANNING.md` describes
   the windows and drops the #708 part of the open-gaps entry; `_plans/REMOTE_READ_PATH.md` marks
   #708 and #1113 done in step 6; this plan is deleted.
5. **Proven row groups.** No page filtering, ColumnIndex or index of skipped filter-only columns
   in a row group statistics proved to match. Tests pin that such a row group is returned whole
   even where its ColumnIndex disagrees with its statistics, and that its read fetches no
   ColumnIndex. `STATISTICS_PRUNING.md` and `FETCH_PLANNING.md` state the rule.
