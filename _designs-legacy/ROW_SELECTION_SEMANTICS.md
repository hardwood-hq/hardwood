# Design: row selection semantics

**Status: In progress.** Tracking issues: #538 (`head` + filter), #540 / #541
(`skip` + filter), #542 (`tail` + filter), #543 (`firstRow` → `skip` rename).
The reader-facing counterpart is `docs/content/concepts/row-selection.md`.

## Scope

How the row-selection controls on `RowReaderBuilder` — `head`, `tail`, `skip` —
relate to the two filtering mechanisms — the row-level `FilterPredicate` and the
group-level `RowGroupPredicate` (e.g. `byteRange`). The goal is a single model
under which the controls compose predictably, so a caller can reason about
`skip(a).filter(p).head(b)` without knowing the file's row-group structure or
the filter's selectivity.

## The model: two axes

Row selection has two independent axes.

**Physical layout** — *where data lives in the file* — is addressed only through
`RowGroupPredicate`, at row-group granularity. `byteRange(start, end)` keeps a
row group if and only if its midpoint falls in `[start, end)`. Across a
partition of the file into disjoint byte ranges, every row group lands in
exactly one range — this is the work-splitting lever for parallel readers.

**Logical result** — *which rows the caller wants* — is addressed by `head`,
`tail`, `skip`, and the row-level `FilterPredicate`.

The **surviving relation** is the set of rows in the row groups kept by the
`RowGroupPredicate`, intersected with the rows matching the `FilterPredicate`,
in file order.

## The one rule

`head`, `tail`, and `skip` count over the surviving relation, in file order:

- `skip(n)` discards the first `n` rows of the relation (SQL `OFFSET`).
- `head(n)` keeps at most the first `n` rows (SQL `LIMIT`).
- `tail(n)` keeps at most the last `n` rows.

There is no second mode. The controls always count over the relation; they do
not switch between "physical" and "logical" behavior.

When no `FilterPredicate` is present, every row of the kept row groups is in the
relation, so a logical offset coincides with a physical row position:
`skip(n)` begins at physical row `n` and remains an O(1-row-group) seek — earlier
row groups are never opened. With a `FilterPredicate`, the controls count matched
rows and that coincidence breaks: row-group statistics bound min/max values, not
match *counts*, so the reader must decode earlier groups in order to count
matches (groups whose statistics prove no match can still be pruned).

## Composition

`RowGroupPredicate` and `FilterPredicate` compose by intersection — a row is in
the relation only if its row group is kept *and* the row matches. The logical
controls then count over that intersection. So `byteRange(...).skip(n)` skips `n`
rows *within the split*, and `byteRange(...).filter(p).head(n)` returns the first
`n` rows of the split that match `p`.

| Control      | No `FilterPredicate`                                              | With `FilterPredicate`                                                  |
|--------------|------------------------------------------------------------------|------------------------------------------------------------------------|
| `skip(n)`    | begin at physical row `n`; earlier row groups not opened (O(1 RG) | discard the first `n` matched rows; earlier groups scanned to count     |
| `head(n)`    | first `n` rows                                                   | first `n` matched rows                                                  |
| `tail(n)`    | last `n` rows                                                    | last `n` matched rows (deferred — see #542)                            |
| `byteRange`  | selects row groups by byte range (the split)                    | intersects: a group is read iff it is in range and not statistics-pruned |

## Non-goals

- **Physical-row addressing under a filter.** There is no API that takes a
  physical absolute row number when a `FilterPredicate` is present. Physical
  positioning is row-group granular via `byteRange`; a checkpoint/resume point is
  expressed as a `(byteRange, logical offset)` pair, not a physical row index.

- **Sub-row-group physical splitting.** `byteRange` is row-group granular,
  matching the I/O unit. Finer physical partitioning is not offered.

- **`tail` + filter (for now).** Tracked as #542 and deferred past 1.0. Unlike
  the forward-streaming `head`/`skip`, the last `n` matched rows have no
  row-group-statistics shortcut, so it requires a reverse scan or a sliding
  window over a full forward scan. Until then `tail` + filter is rejected at
  `build()`.
