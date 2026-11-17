<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Row Selection

Hardwood gives a `RowReader` five controls for
narrowing what comes back: `filter`, `head`, `tail`, `skip`, and the `byteRange` row-group
predicate. The column readers take the same controls except `tail`, plus `rowRange`. **Row
selection counts over the result set, not over the file**, except for `byteRange` and `rowRange`,
which name positions in the file.

## Two questions: which rows, and where they live

Selection has two independent axes, and each control belongs to exactly one of them.

- **Which rows you want: the logical result.** A `FilterPredicate` (`WHERE`), and the
  positional controls `head`, `tail`, and `skip`.
- **Where data lives in the file: the physical layout.** `RowGroupPredicate.byteRange(...)`,
  at row-group granularity, the lever for splitting a file across parallel readers; and, on the
  column readers, `rowRange(start, end)`, at row granularity.

Keeping these apart is what makes the controls predictable: physical positioning is the job of
`byteRange` and `rowRange`, and of nothing else.

## The result set

The `FilterPredicate` defines a **result set**: the rows that match, in file order. The
positional controls count over that result set, exactly like SQL clauses count over the rows that
survive a `WHERE`:

| Control   | SQL analogue | Meaning over the result set                          |
|-----------|--------------|------------------------------------------------------|
| `filter`  | `WHERE`      | the rows that match the predicate, in file order     |
| `head(n)` | `LIMIT n`    | the first `n` matching rows                          |
| `skip(n)` | `OFFSET n`   | discard the first `n` matching rows, keep the rest   |

So `skip(n).head(k)` is `OFFSET n LIMIT k`: it returns at most `k` matching rows, starting after
the first `n` matches. None of these count physical rows, so a matching row sitting deep in the
file is the first row `head(1)` returns. (`tail` is not in this table because it cannot
combine with a filter; see [Supported combinations](#supported-combinations).)

## The no-filter coincidence

With **no filter**, every row matches, so the result set *is* the whole file and a logical
position coincides with a physical one. That is the case where `skip(n)` can be a true seek:
it begins at physical row `n`, and earlier row groups are never opened. It is an O(1-row-group) jump
that fetches none of the bytes in between. `head(n)` is the first `n` rows of the file.

Add a filter and the coincidence breaks. Row-group statistics bound the *values* in a group, not
the *count* of rows that match, so the reader cannot know how many matches lie ahead without
looking. `head` and `skip` then stream over the matched rows, decoding earlier groups to count
them, though groups whose statistics prove no row can match are skipped wholesale.

## Physical splitting is separate

`byteRange(start, end)` assigns each row group to exactly one of a set of disjoint byte ranges
(see [Split-Aware Reading](../how-to/query-controls.md#split-aware-reading)). It composes with the
logical controls by intersection: under `byteRange(...).filter(p).skip(n).head(k)`, the result
set is the matching rows *within this reader's row groups*, and `skip`/`head` count over that.

## Physical row ranges

`rowRange(start, end)` selects the rows at positions `[start, end)` of the file, before the filter.
With a filter, the result is the rows inside the range that match, so the filter narrows the range
and never moves it. The positions are known from the file's metadata alone, so the reader opens
only the row groups the range overlaps, whatever the filter. Which pages it fetches in the row
groups at either end is in [Reading a Row Range](../how-to/query-controls.md#reading-a-row-range-rowrange).

Under a filter, `rowRange` and `skip` answer different questions. On a file whose rows 0–99 hold
no match, `filter(p).skip(100)` discards the first 100 *matches*, which lie past row 99, while
`filter(p).rowRange(100, end)` discards the first 100 *rows*, none of which match, and returns
every match from row 100 on. `rowRange` composes with `byteRange` by counting the rows of the row
groups `byteRange` keeps.

## Supported combinations

`head`, `skip`, `byteRange` and `rowRange` each compose with a `FilterPredicate`. `tail` + filter is
rejected at `build()`: the *last* `n` matching rows have no row-group-statistics shortcut, so it
would require a reverse scan over the whole file. `rowRange` + `head` and `rowRange` + `skip` are
rejected at `build()`, since the range already fixes the first row and the row count.

## Further reading

- [Predicate Pushdown, Projection, Limits, and Splits](../how-to/query-controls.md) — the
  step-by-step guide for each control.
- [How a Parquet File Is Laid Out](parquet-layout.md) — row groups, pages, and why statistics
  let whole groups be skipped.
