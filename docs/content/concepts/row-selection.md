<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Row Selection

You rarely want every row of a Parquet file. Hardwood gives a `RowReader` five controls for
narrowing what comes back — `filter`, `head`, `tail`, `skip`, and the `byteRange` row-group
predicate. They compose predictably once you hold one idea: **row selection counts over the
result set, not over the file.** For the step-by-step recipes, see
[Predicate Pushdown, Projection, Limits, and Splits](../how-to/query-controls.md).

## Two questions: which rows, and where they live

Selection has two independent axes, and each control belongs to exactly one of them.

- **Which rows you want — the logical result.** A `FilterPredicate` (`WHERE`), and the
  positional controls `head`, `tail`, and `skip`.
- **Where data lives in the file — the physical layout.** `RowGroupPredicate.byteRange(...)`,
  at row-group granularity. This is the lever for splitting a file across parallel readers, not
  for shaping what a single reader returns.

Keeping these apart is what makes the controls predictable: physical positioning is `byteRange`'s
job, and nothing else's.

## The result set

The `FilterPredicate` defines a **result set** — the rows that match, in file order. The
positional controls count over that result set, exactly like SQL clauses count over the rows that
survive a `WHERE`:

| Control   | SQL analogue | Meaning over the result set                          |
|-----------|--------------|------------------------------------------------------|
| `filter`  | `WHERE`      | the rows that match the predicate, in file order     |
| `head(n)` | `LIMIT n`    | the first `n` matching rows                          |
| `skip(n)` | `OFFSET n`   | discard the first `n` matching rows, keep the rest   |

So `skip(n).head(k)` is `OFFSET n LIMIT k`: it returns at most `k` matching rows, starting after
the first `n` matches. None of these count physical rows — a matching row sitting deep in the
file is still the first row `head(1)` returns. (`tail` is not in this table because it cannot
combine with a filter — see [Currently supported combinations](#currently-supported-combinations).)

## The no-filter coincidence

With **no filter**, every row matches, so the result set *is* the whole file and a logical
position coincides with a physical one. That is the case where `skip(n)` can be a true seek:
it begins at physical row `n`, and earlier row groups are never opened — an O(1-row-group) jump
that fetches none of the bytes in between. `head(n)` is simply the first `n` rows of the file.

Add a filter and the coincidence breaks. Row-group statistics bound the *values* in a group, not
the *count* of rows that match, so the reader cannot know how many matches lie ahead without
looking. `head` and `skip` then stream over the matched rows, decoding earlier groups to count
them — though groups whose statistics prove no row can match are still skipped wholesale. This is
why `skip(n)` is an O(1) seek without a filter but a forward scan with one: same control, but the
relation it counts over changed.

## Physical splitting is separate

`byteRange(start, end)` keeps a row group when its midpoint falls in `[start, end)`. Across a
partition of the file into disjoint ranges, every row group lands in exactly one — the basis for
handing each parallel reader its own slice of the file. It composes with the logical controls by
intersection: under `byteRange(...).filter(p).skip(n).head(k)`, the result set is the matching
rows *within this reader's row groups*, and `skip`/`head` count over that.

Reach for `byteRange` to decide *which bytes a reader owns*; reach for `filter`/`skip`/`head` to
decide *which rows it returns*. Using `skip` to position a scan physically under a filter is the
wrong tool — that is what `byteRange` is for.

## Which do I reach for?

| You want…                                          | Use                              |
|----------------------------------------------------|----------------------------------|
| the first `n` rows matching a predicate            | `filter(p).head(n)`              |
| rows `n … n+k` of the matching rows                | `filter(p).skip(n).head(k)`      |
| row `n` of the file, ignoring content              | `skip(n)` (no filter — a seek)   |
| the last `n` rows of the file                      | `tail(n)` (no filter)            |
| this reader to own a byte range of the file        | `filter(byteRange(a, b))`        |

## Currently supported combinations

`head`, `skip`, and `byteRange` each compose with a `FilterPredicate`; `head`/`skip` count over
the matched rows. `tail` + filter is **not** supported and is rejected at `build()`: unlike the
forward-streaming `head`/`skip`, the *last* `n` matching rows have no row-group-statistics
shortcut, so it would require a reverse scan over the whole file. Until that lands, take the tail
of the unfiltered file, or filter and keep the trailing matches yourself.

## Further reading

- [Predicate Pushdown, Projection, Limits, and Splits](../how-to/query-controls.md) — the
  step-by-step guide for each control.
- [How a Parquet File Is Laid Out](parquet-layout.md) — row groups, pages, and why statistics
  let whole groups be skipped.
