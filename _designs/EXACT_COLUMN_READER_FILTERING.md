# Design: exact column-reader filtering

**Status: Implemented.** Tracking issue: #624 (milestone 1.0.0.CR2). Builds on the
drain-side machinery (`_designs/DRAIN_SIDE_RECORD_FILTERING.md`) and the
record-filter compiler (`_designs/RECORD_FILTER_COMPILATION.md`). Related
architecture: #500 (late-materialization fork — out of scope here), #74 / #70
(shared predicate cache — a follow-up optimization).

## Scope

`buildColumnReader(col).filter(pred)` and
`buildColumnReaders(projection).filter(pred)` must return **only** the rows
satisfying `pred` — exact, byte-for-byte equal to the row reader's filtered
result, with **no client-side residual**. A direct aggregate over the filtered
output is correct.

This holds for every predicate shape the `RowReader` supports — self-column,
cross-column, a predicate column absent from the projection, eligible
(drain-side) and ineligible (nested paths, unsupported `(type, op)`, binary,
geospatial) alike — and for flat **and** nested payload and predicate columns.

The guarantee is **exact post-decode filtering**: payload columns are fully
decoded and then compacted to matching rows. Skipping the decode of payload
columns for pruned rows (selective decode / late materialization) is a separate
architectural change tracked in #500 and is explicitly **not** part of this
design.

## The contract

A `ColumnReader` or `ColumnReaders` configured with `.filter(pred)`:

- `getRecordCount()` returns the count of **matching** records in the batch.
- `getInts()` / `getLongs()` / `getStrings()` / the layer offsets / validity /
  every typed accessor return values for the matching records only, in file
  order, row-aligned across all columns of the projection.
- For a `ColumnReaders` projection, every `getColumnReader(name)` exposes the
  same matching set; per-batch counts agree across columns.
- For independently built single `ColumnReader`s over the same file and
  predicate, per-batch counts and values agree with each other and with the
  grouped reader and the row reader.

## Model: augmented projection + per-batch selection

A filtered column read is a grouped drain over the **augmented projection**

```
augmented = userColumns ∪ predicateColumns
```

plus a per-batch **selection** — a record-level bitmap over `[0, recordCount)`,
set bit = record matches `pred` — that compacts each *user-visible* column down
to the matching records. Predicate columns that the user did not request are
decoded to evaluate the predicate but are not exposed through
`getColumnReader(...)`.

The single-column `buildColumnReader(col).filter(pred)` is the degenerate case:
a grouped drain over `{col} ∪ predicateColumns` that exposes exactly one column.
Both entry points share one engine.

```
        ┌──────────────── augmented projection ─────────────────┐
        │  payload cols (exposed)     predicate cols (may be     │
        │                             hidden)                    │
RowGroupIterator ── workers ── BatchExchange[] ──┐
                                                 │
                                        SelectionEngine
                                         (per aligned batch)
                                                 │
                                      record-level selection bitmap
                                                 │
                       ┌─────────────────────────┴───────────────┐
                  compact payload col 0   …   compact payload col k
                       │                                         │
                  getInts()/getLongs()/… return matching rows only
```

The selection is computed **once** per aligned batch and shared across all
exposed columns. The page-range and row-group pruning already performed by
`RowGroupIterator` is unchanged — it shrinks the superset; the selection then
makes the result exact.

## SelectionEngine

`SelectionEngine` (package-private in `dev.hardwood.reader`, alongside
`ColumnReader`/`ColumnReaders`/`FilterCoordinator`) owns predicate evaluation for
one grouped read. It lives in this package rather than `internal.reader` because
it reads each reader's current batch and drives compaction through
`ColumnReader`'s package-private hooks (`currentFlatBatch()`, `applySelection`,
`rawNextBatch`); keeping it here avoids widening those to a public/internal
surface. After the aligned batches for a step are available, it produces the
selection through one of two backends, chosen once at construction by
`BatchFilterCompiler.tryCompile`:

**Eligible backend (fast).** When `tryCompile` returns a `CompiledBatchFilter`
(flat, top-level, supported `(type, op)`), the predicate columns' workers run
their `ColumnBatchMatcher` on the drain thread and write `Batch.matches`, exactly
as for the row reader. The engine merges the per-column bitmaps with the
`MergePlan` via `MergePlanEvaluator`. This is the existing
`FlatRowReader.intersectMatches` logic, extracted so both readers share it.

**Fallback backend (parity).** When `tryCompile` returns `null` — nested
predicate paths, binary, geospatial, unsupported `(type, op)` — the engine
evaluates the compiled `RowMatcher` (`RecordFilterCompiler`) per record over a
**batch-backed `StructAccessor`** view of the aligned predicate-column batches,
setting one selection bit per matching record. Flat predicate columns are served
directly from their typed arrays; nested predicate columns are served through the
nested record-navigation logic over their `NestedBatch`. This reuses the row
reader's predicate evaluation wholesale, so the fallback covers every shape the
`RowReader` does — no second predicate evaluator.

Both backends emit the same selection representation (the set bits, plus a
matching-record index map derived from them); downstream compaction is
backend-agnostic.

## Compaction

The selection is a record-level mask. Compaction differs by column shape, and in
both cases reuses the index-map compaction already present on `ColumnReader`
(`compactPrimitive` / `compactBinary`):

**Flat column.** Record == leaf. Build the matching-record index map from the
selection and compact the value array, the validity bitmap, and (for binary) the
offset array through it. `getRecordCount()` / `getValueCount()` return the
matching count.

**Nested column.** Compaction happens on the **raw** `(definitionLevels,
repetitionLevels, values)` triplet, before the real-items view is computed: each
selected top-level record is a maximal level run beginning at `repetitionLevel ==
0`, so compaction copies the level/value runs of the selected records into a new
triplet. The existing `NestedLevelComputer.computeRealView` then runs unchanged
over the compacted triplet to produce layer offsets, leaf validity, and the
real-items leaf array. Compacting before the real-view keeps all nested
bookkeeping in one place and avoids re-deriving layer offsets by hand.

## Wiring

**`ColumnReaders` (grouped).** Built over the augmented projected schema.
`nextBatch()` advances every reader, then asks the `SelectionEngine` for the
batch selection and shares it with each exposed `ColumnReader`. The hidden
predicate columns are excluded from the `readersByName` / `readersByIndex` view.
The record-count alignment guard compares post-compaction counts — all columns
share one selection, so they remain equal; the exhaustion/empty-batch path is
preserved.

**`buildColumnReader(col).filter(pred)` (single).** Resolves the predicate,
forms the augmented projection `{col} ∪ predicateColumns`, builds a private
grouped engine, and exposes only `col`. Distinct single readers over the same
file each decode the predicate column independently — a redundant decode the
issue accepts for now. Sharing the computed selection across sibling readers on
the owning `ParquetFileReader` is the #74 / #70 follow-up; it is a pure
optimization and does not change results.

**Augmented-projection indexing.** Column ordering in the augmented projected
schema and the `toProjectedIndex` mapping handed to `BatchFilterCompiler` and the
fallback adapter must stay consistent, so the predicate fragments and the
record-navigation view address the same columns the workers fill.

## Testing

Correctness is the bar, so the matrix is exhaustive across predicate shape,
column shape, and entry point. The reference oracle is the `RowReader`'s filtered
result over the same file and predicate.

**Predicate placement**

- Self-column: read `x`, filter on `x`.
- Cross-column: read `amount`, filter on `event_time`.
- Predicate column **not** in the projection (decoded only to filter).

**Predicate eligibility**

- Eligible / drain-side (flat top-level `(type, op)`), including `AND`/`OR`.
- Ineligible / fallback: nested-path predicate, binary, an unsupported
  `(type, op)`, and a `NOT` lowering.

**Column shape**

- Flat payload columns.
- **Nested payload columns** (e.g. `list<int>`, `struct`, `list<string>`):
  the compacted layer offsets, leaf validity, and real-items leaf array must
  match the row reader's per-record view for the matching records.
- **Nested predicate column** (filter references a field inside a nested group):
  exercises the fallback backend's nested record navigation.

**File structure**

- A **non-page-aligned** threshold (the case that exposed the bug) on a
  monotonic `event_time` file — assert exact count and correct aggregate, not
  just a count that happens to land on a page boundary.
- Multiple row groups, and a row group fully pruned by statistics.
- Nulls in payload and predicate columns (SQL three-valued logic: NULL never
  matches a comparison).

**Equivalence**

For every combination above, assert **distinct single readers == grouped
`ColumnReaders` == `RowReader`** on both matching record count and a
column aggregate (sum / concatenation), per batch and in total.

## Documentation

- Rewrite the `ColumnReaderBuilder.filter(FilterPredicate)` and
  `ColumnReadersBuilder.filter(FilterPredicate)` Javadoc: replace the "row groups
  whose statistics prove no row matches are skipped" wording with the exact
  semantics — only matching rows are returned, no client-side residual.
- Update the affected `docs/content/` reference and how-to pages for the
  column-reader API to state the exact-filter contract.
- Mark the column-reader filtering line in `ROADMAP.md` once implemented.

## Non-goals

- **Selective decode / late materialization.** Payload columns are fully decoded
  before compaction. Gating payload decode on the predicate result is #500
  (option 2) and is not addressed here.
- **Shared predicate-selection cache across distinct readers.** Independently
  built single readers redundantly decode the predicate column. The shared cache
  on `ParquetFileReader` is #74 / #70; it is an optimization layered on top of
  this contract, not a correctness dependency.
- **Pushing the predicate into encoded/dictionary space.** Orthogonal (#196).
