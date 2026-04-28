# Design: row-based seek in RowReader

**Status: Implemented.** Tracking issue: #361. PR: this branch.

## Goal

Allow callers of `ParquetFileReader` to create a `RowReader` that begins at an
arbitrary absolute row index, instead of only at row 0 or only at the tail.
The reader internally locates the containing row group and skips the within-RG
residue; consumers see a clean "iterate from row N onwards" surface and don't
need to know about row-group structure.

This makes "go to row N" jumps in dive's Data preview cost a single row
group's worth of projected-column bytes on remote files, instead of walking
from row 0 and fetching everything in between.

## Public API

A new method on the existing `RowReaderBuilder`:

```java
/// Begin reading from the given absolute row index. Earlier row groups
/// are not opened — their pages are not fetched or decoded — so this
/// is an O(1 RG) seek on remote backends.
///
/// `firstRow == 0` is the no-op default. `firstRow == totalRows`
/// produces an empty reader. Indexes into the *first* file's rows for
/// multi-file readers; cross-file `firstRow` is out of scope. Mutually
/// exclusive with [#tail]; composes with [#head] for a bounded
/// `[firstRow, firstRow + maxRows)` window.
public RowReaderBuilder firstRow(long firstRow)
```

Composes with existing builder methods:

```java
// Read rows [N, end) of the file, no projection, no filter
file.buildRowReader().firstRow(N).build();

// Read rows [N, N+M) — bounded window
file.buildRowReader().firstRow(N).head(M).build();

// Same with projection + filter
file.buildRowReader()
        .projection(ColumnProjection.columns("id", "name"))
        .filter(FilterPredicate.gt("id", 100))
        .firstRow(500)
        .head(50)
        .build();
```

Mutually exclusive with `tail(N)` (the two select different ends of the
file).

## Implementation

In `ParquetFileReader.buildRowReader(projection, filter, maxRows, firstRow)`:

1. If `firstRow == 0`, delegate to the existing path (no behaviour change).
2. Otherwise, locate the target row group by walking cumulative
   `RowGroup.numRows()` from the first file's row-group list. Compute
   `withinRg = firstRow - rowGroupFirstRow(targetRg)`.
3. If `firstRow >= totalRows`, return an empty reader (`rowGroups.subList(end, end)`).
4. Open a reader scoped to `rowGroups.subList(targetRg, end)`. Apply
   `head(maxRowsAdjusted)` where `maxRowsAdjusted = maxRows + withinRg` (the
   `head` budget bounds *yielded* rows, and the within-RG skip yields rows
   too — without the residue term, jumps near the end of an RG would
   exhaust the budget on the skip and yield zero data).
5. Walk `next()` `withinRg` times to discard the within-RG residue. The
   bytes for those leading rows *are* fetched (they live in the row
   group's first pages); the consumer just doesn't see them.

## What this fixes

Against a multi-row-group file on remote storage:

| Action | Before | After |
|---|---|---|
| `firstRow(0)` | Open from row 0 | Same (no-op) |
| `firstRow(N)` where N is in row group K | Walk row 0 → N, fetching all of row groups 0..K | Skip directly to row group K, fetch only its bytes |
| `firstRow(totalRows)` | Walk to end (full file) | Empty reader (no fetches) |

## Dive wiring

`ParquetModel.readPreviewPage(firstRow, count, consumer)` builds a
fresh `RowReader` on every call:

```java
try (RowReader cursor = reader.buildRowReader()
        .firstRow(firstRow)
        .head(count)
        .build()) {
    while (read < count && cursor.hasNext()) {
        cursor.next();
        consumer.accept(cursor);
    }
}
```

No cursor reuse. The eager close is intentional: it releases the
iterator's metadata and chunk-handle bytes before the next call, and
the dive viewport window cache (#377) absorbs all within-buffer
navigation, so cross-call cursor reuse would buy nothing while
risking the "phantom fetch" issue (#370) where an unbounded cursor's
worker pool drains long after the user moved on. `head(count)` caps
each cursor to exactly the rows asked for; combined with `firstRow`,
the cursor is scoped to the target row group onwards and never
fetches earlier ones.

The screen-level `DataPreviewScreen.PAGE_CACHE` and the `head(cap)`
workaround from the earlier version of this design are gone — the
dedicated viewport window cache (#377) replaces them.

## What's out of scope

- **Page-level skip via OffsetIndex.** Within the target row group, this
  design still walks `next()` `withinRg` times — bytes for leading pages
  *are* fetched. With an OffsetIndex we know the byte offset and
  `firstRowIndex` of every page and could drop leading pages at the byte
  level. Tracked as #381; non-trivial because it requires adjusting the
  reader's row-yielding contract (the reader currently always starts at
  row 0 of the row group).

- **Page-level seek for files without OffsetIndex.** Synthesising
  page-location info from a header walk is #378.

- **Multi-file `firstRow`.** Indexes into the first file's rows only.
  Cross-file (e.g. "row N across the concatenated dataset") is a separate
  problem and not on the roadmap.

- **Backwards iteration.** RowReader stays forward-only.

- **Async I/O.** Seeks happen on the calling thread; #331 covers async.

## Testing

- `ParquetReaderTest.firstRowSkipsEarlierRowGroups` — `firstRow(150)` on a
  3-RG file (100 rows each) yields the right rows starting at id 151.
- `ParquetReaderTest.firstRowAtRowGroupBoundary` — `firstRow(100)` lands
  at the start of RG 1 with no within-RG residue.
- `ParquetReaderTest.firstRowComposesWithHead` — `firstRow(150).head(20)`
  yields exactly rows 150..169.
- `ParquetReaderTest.firstRowAtTotalRowsYieldsEmpty` — boundary at
  end-of-file.
- `ParquetReaderTest.firstRowRejectsNegative` and
  `firstRowAndTailAreMutuallyExclusive` cover the validation paths.
- `DataPreviewIoTest` exercises the dive integration: jump-to-last-page
  yields the right rows AND reads less than half the file's bytes;
  forward intra-RG steps reuse the cursor; backward seek rebuilds within
  the current RG only.
