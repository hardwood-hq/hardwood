# Design: dive list-screen viewport virtualization

**Status: Implemented.** Tracking issue: #401.

## Goal

Make Up/Down navigation on every list-shaped dive screen feel instant
regardless of how large the underlying collection is. Per-keystroke work
must scale with the visible viewport, not with the total row count, so a
file with hundreds of thousands of dictionary entries, thousands of
pages, or tens of thousands of file-wide index entries navigates as
smoothly as a small file does.

## Behaviour

Each list screen's `render` constructs `Row` objects only for the rows
the table will actually paint, derived from the current selection and
the available viewport height. The visible window is bottom-pinned on
the selection, matching what `TableState.scrollToSelected` would
produce when the state is constructed fresh each frame:

```
viewport = max(1, area.height() - <chrome>)   // chrome varies per screen
sel      = clamp(selection, 0, total - 1)
start    = sel >= viewport ? sel - viewport + 1 : 0
end      = min(total, start + viewport)
```

The math lives in a shared helper, [`RowWindow.bottomPinned(selection,
total, viewport)`](../cli/src/main/java/dev/hardwood/cli/dive/internal/RowWindow.java),
which returns the `[start, end)` range plus `selectionInWindow` (the
slice-relative selection index). Each screen builds rows for that range
only, then sets `TableState.select(window.selectionInWindow())` so the
table draws the cursor at the right row of the slice. The underlying
widget keeps its existing rendering behaviour and never sees the rest
of the collection.

The user-visible behaviour is unchanged: range header
(`Plurals.rangeOf`), search bar (where present), modal, and key bar
all still operate against the full row count.

## Affected screens

Applied to every list-shaped screen that previously built a `Row` per
collection item:

| Screen | Row source | Realistic worst N |
|---|---|---|
| `DictionaryScreen` | filtered dictionary entries | hundreds of thousands |
| `PagesScreen` | pages in a column chunk | thousands |
| `ColumnIndexScreen` | pages with stats | thousands |
| `OffsetIndexScreen` | page locations | thousands |
| `FileIndexesScreen` | row groups × columns × index types | tens of thousands |
| `ColumnChunksScreen` | columns in a row group | thousands on wide schemas |
| `ColumnAcrossRowGroupsScreen` | row groups for one column | hundreds–thousands |
| `RowGroupIndexesScreen` | columns × index types per RG | thousands on wide schemas |
| `RowGroupsScreen` | row groups | hundreds–thousands |

`DataPreviewScreen` is intentionally not on the list. It already solves
the problem one layer up: its `state.rows()` is the page-sized slice
returned from `PreviewWindow.slice(model, firstRow, pageSize, …)`,
sized to the viewport. Iterating it is already O(viewport), and the
selection is already slice-relative.

`PagesScreen` carries one extra detail: per-data-page stats (offset
index, column index, inline statistics) are addressed by a sequential
`dataPageIdx` that advances only on non-dictionary pages. After
slicing, the screen recovers `dataPageIdx` at `window.start()` by
counting non-dictionary pages in the skipped prefix — a cheap O(start)
header-type scan, not formatting work.

## Why not just keep memoising

The Dictionary screen already memoised its filter result so the *filter
predicate* did not re-run on every keystroke. That cache helped when
the filter was non-trivial, but it did not reduce the dominant
per-frame cost: building N `Row`/`Cell` objects and formatting N
values. The Table widget walks the full row list to compute its
visible window via cumulative `Row.totalHeight()`, so even off-screen
rows were constructed. Other screens did not even have a filter cache.

Slicing the rows up front fixes both at every screen: less allocation,
and the table no longer iterates the full list.

## Filter cache representation (Dictionary screen only)

The Dictionary screen still memoises its filter result, but with a
representation chosen so the empty-filter cache-hit allocates nothing.
The cache uses an `int[]` instead of `List<Integer>`, with a `null`
sentinel for the empty-filter (and "every entry matches") case
representing the identity mapping `[0, size)`. The cache key holds the
`Dictionary` via `WeakReference` so an entry evicted from the model's
bounded `dictionaryCache` is not pinned by this static slot for the
JVM lifetime.

Callers go through a small `FilterIndex` helper:

```java
private record FilterIndex(int size, int[] indices) {
    boolean isEmpty() { return size == 0; }
    int at(int i) { return indices == null ? i : indices[i]; }
}
```

This optimisation is Dictionary-specific and does not transfer — the
other list screens have no `/`-style filter.

## Convention

`CLAUDE.md` carries the rule in its Dive TUI section: list-shaped
screens must build `Row` objects only for the visible viewport, using
`RowWindow.bottomPinned(...)`. New list screens inherit this from the
start.
