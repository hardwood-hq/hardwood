# Design: Word-shape validity bitmap (#440)

**Status: Implemented.** Tracking issue: #440.

## Context

`Validity` is the per-item null bitmap surfaced through `ColumnReader` for every nullable scope (leaf, STRUCT layer, REPEATED layer). It carries set-bit-= -present semantics: a set bit means the item is present, a clear bit means it is null. The sealed-type shape (`NoNulls` singleton vs. `Backed` wrapper) makes the no-nulls fast path explicit and identity-stable.

The public surface and internal carrying format are decoupled today. Internally, `FlatColumnWorker` accumulates the bitmap in a `java.util.BitSet`, clones it at publish, and hands a `BitSet` to `BatchExchange.Batch.validity`. The nested pipeline does the same: `NestedLevelComputer` builds `BitSet[] layerValidity` / `BitSet leafValidity` inside `RealView`. Consumers — `FlatRowReader`'s row-by-row accessors, the per-column `*BatchMatcher`s, and `ColumnReader.getLeafValidity` / `getLayerValidity` — receive the `BitSet`. `FlatRowReader` and the matchers route around `BitSet` either via `BitSet.toLongArray()` (every batch) or via per-row `BitSet.get(int)` calls. `ColumnReader` wraps the `BitSet` in a `Validity.Backed` whose `isNull(i)` is `!bits.get(i)`.

This design changes the carrying format end-to-end to a packed `long[]` and exposes the underlying word array on `Validity` so consumers can choose between per-row, per-word, or per-run loop shapes against the same value.

## Design properties

### Single representation, all the way through

Every place the pipeline touches validity — producer, exchange, consumer accessors, public API — speaks `long[]`. Per-batch `BitSet.toLongArray()` conversions disappear. `BitSet.clone()` at publish disappears (the producer hands the packed array forward).

Set-bit-= -present polarity is unchanged. The word at index `w` covers rows `[w*64, w*64+64)`, low bit = lowest row. Each validity array has length `(rowCount + 63) >>> 6`. Bits past `rowCount` in the last word are undefined and never read.

### Sparse "no nulls" stays free

`null` continues to mean "every item at this scope is present in the current batch" — both inside `BatchExchange.Batch.validity` and at the public API. `Validity.NO_NULLS` is the singleton callers see; `Validity.of(null)` returns it. Producers that observe no absents in a batch skip the validity allocation entirely.

### The public API exposes the word array

A new accessor `Validity.words()` returns the backing `long[]`. For `Backed`, the array is a defensive copy — callers may mutate the returned array freely. For `NoNulls`, `words()` returns `null` (mirroring the sparse representation; the caller branches on the result or uses the existing `isNull(i)` path which is a no-op for `NoNulls`). Bits at indices `>= count` are undefined and must not be read.

This gives perf-sensitive consumers three loop shapes against the same `Validity` value: per-row `isNull(i)` (idiomatic, JIT-inlinable), inlined word/mask reading directly from `words()` (avoids the method call), and a word-wise outer loop that iterates set bits via `Long.numberOfTrailingZeros` plus the `present &= present - 1` clear-lowest-bit trick. The third lets callers skip null-dense regions in O(words-with-runs) instead of O(rows) and gives the JIT a vectorizable inner loop.

### Caller-owned sizing contract

`long[]` has no intrinsic logical bit-count cap. Rather than carrying a row count on `Validity.Backed`, the API places the sizing responsibility on the caller: the array handed to `Validity.of(long[])` must have at least `(count + 63) >>> 6` words for any `count` later passed to `nullCount` / `nextNull` / `nextNotNull`, and indices into `isNull` / `isNotNull` must stay within the same bound. The library performs no per-call bound check — keeping `isNull(i)` reduced to a word load and mask test in the hot path. By construction every internal producer already allocates an exactly-sized array (`NestedLevelComputer` and `FlatColumnWorker` trim with `Arrays.copyOf` to `(n + 63) >>> 6` words), so the contract holds automatically for the in-pipeline code paths.

## Components

### `Validity` (`core/src/main/java/dev/hardwood/reader/Validity.java`)

`Backed` stores `long[] words` instead of `BitSet bits`. Method implementations:

- `isNull(i)`: `(words[i >>> 6] & (1L << i)) == 0L`
- `isNotNull(i)`: `(words[i >>> 6] & (1L << i)) != 0L`
- `hasNulls()`: `true` (the factory only constructs `Backed` when the producer indicated at least one null; `NoNulls` takes the no-nulls path).
- `nullCount(int count)`: sum of `Long.bitCount(~words[w] & tailMask(count, w))` across `(count + 63) >>> 6` words, with the last word masked to the live bit range. Returns the count of clear bits in `[0, count)`.
- `nextNull(int from, int count)`: word scan of `~words[w]` (masked below `from` and above `count`), `Long.numberOfTrailingZeros` to locate the bit. Returns `-1` if no null in range.
- `nextNotNull(int from, int count)`: same against `words[w]` (un-negated).

The new accessor:

- `long[] words()`: on `Backed`, returns `words.clone()` — a defensive copy that callers may freely mutate. On `NoNulls`, returns `null`.

The factory:

- `Validity.of(long[] words)`: returns `NO_NULLS` when `words == null`; otherwise wraps in a fresh `Backed`. The wrapper does not copy the input — callers must not mutate the bitmap after handing it to `Validity.of`. The packed-bitmap form replaces `Validity.of(BitSet)`.

The `@Experimental` annotation already permits the factory signature change.

### `BatchExchange.Batch.validity` (`core/src/main/java/dev/hardwood/internal/reader/BatchExchange.java`)

Field type changes from `BitSet` to `long[]`. JavaDoc records the layout: `(recordCount + 63) >>> 6` words, set-bit-= -present, `null` for the sparse all-present batch.

### `FlatColumnWorker` (`core/src/main/java/dev/hardwood/internal/reader/FlatColumnWorker.java`)

`currentValidity` is a worker-owned `long[]` of length `(batchCapacity + 63) >>> 6`, allocated once when the column is nullable (`maxDefinitionLevel > 0`). It is the scratch the worker fills during `markNulls`. At publish, the worker emits `Arrays.copyOf(currentValidity, (rowsInCurrentBatch + 63) >>> 6)` into `Batch.validity` (trimmed to the active word range) and `Arrays.fill(currentValidity, 0L)` before the next batch.

The single-bit set and range-set patterns become:

- Single bit: `currentValidity[i >>> 6] |= 1L << i` (Java's shift-by `i & 63` for `long` is implicit).
- Range set (the `defLevels == null` fast path and the first-absent backfill from `[0, destPos + i)`): a private `setBitRange(long[] words, int fromInclusive, int toExclusive)` helper that fills the first word with `~0L << fromInclusive`, intermediate words with `~0L`, and the last word with `~0L >>> -toExclusive`.

The "no absents in this batch → emit `validity = null`" sparse rule remains: at publish, `Batch.validity` is set to `null` when `currentBatchHasAbsents` was never raised.

### `FlatRowReader` (`core/src/main/java/dev/hardwood/internal/reader/FlatRowReader.java`)

`flatValidity` is `long[][]`. The `ALL_PRESENT` sentinel is a `long[]` of all-ones words sized for `BatchSizing.MAX_BATCH`; it covers any in-range `rowIndex` so the per-row check reads as present without a null guard.

`isNull(int columnIndex)` and the five typed accessors (`getInt`, `getLong`, `getFloat`, `getDouble`, `getBoolean`) read the bit inline as a word load, mask shift, and zero test — no method call, no bounds check. `loadNextBatch` sets the per-column slot to `batch.validity` when non-null, or to `ALL_PRESENT` when the producer emitted the sparse all-present sentinel.

### Per-column `*BatchMatcher`s (`core/src/main/java/dev/hardwood/internal/predicate/matcher/`)

Every comparison matcher (≈28 classes across `booleans/`, `doubles/`, `floats/`, `ints/`, `longs/`) drops its `BitSet.nextClearBit` loop in favour of a word-wise AND of `batch.validity` into `outWords` across the active word range. `IsNullBatchMatcher` and `IsNotNullBatchMatcher` (`core/src/main/java/dev/hardwood/internal/predicate/matcher/nulls/`) collapse to a single word loop (bit-inverted for IS NULL) or `System.arraycopy` (for IS NOT NULL). The `validity.toLongArray()` + `Math.min` trim that exists today disappears — `Batch.validity.length` already matches `(recordCount + 63) >>> 6`.

### Nested path

- `NestedLevelComputer.RealView` (`core/src/main/java/dev/hardwood/internal/reader/NestedLevelComputer.java`): `layerValidity: BitSet[]` becomes `long[][]`; `leafValidity: BitSet` becomes `long[]`.
- `NestedLevelComputer.computeElementValidity`: builds and returns `long[]`. Same lazy "switch on bitmap only after first absent" pattern, written word-wise.
- `NestedLevelComputer.computeRealView`: allocates `long[]` per nullable layer.
- `NestedBatch.elementValidity` (`core/src/main/java/dev/hardwood/internal/reader/NestedBatch.java`): `BitSet` becomes `long[]`.
- `NestedBatchDataView` (`core/src/main/java/dev/hardwood/internal/reader/NestedBatchDataView.java`): every `BitSet validity = ...` site becomes `long[] validity = ...`; per-element checks become the inlined `(words[i >>> 6] & (1L << i)) == 0L` form.
- `NestedBatchIndex.elementValidity` (`core/src/main/java/dev/hardwood/internal/reader/NestedBatchIndex.java`): `BitSet[]` becomes `long[][]`; read sites updated.

### `ColumnReader`

- `getLeafValidity()`: flat path returns `Validity.of(currentFlatBatch.validity)`; nested path returns `Validity.of(realView.leafValidity())`.
- `getLayerValidity(int layer)`: returns `Validity.of(realView.layerValidity()[layer])`.

Neither path wraps a `BitSet` anymore.

### Tests

- `core/src/test/java/dev/hardwood/ValidityTest.java`: assertions hold; construction sites swap to `Validity.of(long[])`. New tests exercise `words()` against `Backed` and `NoNulls`.
- `core/src/test/java/dev/hardwood/internal/predicate/ColumnBatchMatcherTest.java`: keeps `BitSet` for terse test scratch but uses a `BitSet → long[]` helper at the matcher boundary (`BitSet.toLongArray()` + pad).
- `core/src/test/java/dev/hardwood/internal/predicate/DrainSideOracleTest.java`: same pattern for the `nullsToValidity` helper.
- `core/src/test/java/dev/hardwood/internal/reader/ColumnWorkerTest.java`: a small `popcount(long[])` helper replaces the `BitSet.cardinality()` call used to verify non-trivial null counts.

### User-facing documentation

Per `CLAUDE.md`'s public-API rule, the `Validity` section under `docs/content/` is updated to describe the `long[]` shape, the `words()` accessor, and the recommended loop shapes. Snippets call `Validity.of(long[], int)` and demonstrate the word-wise scan.

## What this design does NOT change

- The drain-side per-column matcher contract (matchers still receive a `Batch` and write into `outWords`; matchers only change how they read `batch.validity`).
- The `Validity` sealed-type shape (`NoNulls` vs `Backed`). Callers using the `switch (validity)` pattern continue to work.
- The `Batch.validity == null` sparse encoding for "every item present in this batch". A separate effort can elide the null-vs-non-null branch but the existing semantics are preserved here.
- Page-level null statistics — `DataPageHeaderV2.numNulls` remains parsed-and-discarded after this design lands (separate follow-up).

## Testing

- `./mvnw -pl core test` — the existing 1077 core tests cover all consumer paths and predicate matchers and should continue to pass with no behavioural changes.
- `./mvnw verify -pl '!s3'` — full build excluding S3 (S3 module requires Docker; unrelated failures).
- New `ValidityIterationBenchmark` (`performance-testing/micro-benchmarks/src/main/java/dev/hardwood/benchmarks/ValidityIterationBenchmark.java`) — compares the per-row `isNotNull(i)` loop against the word-wise outer loop (`Long.numberOfTrailingZeros` + `present &= present - 1`) across `count ∈ {1024, 65536}` and `nullDensity ∈ {0.0, 0.05, 0.95}`. Establishes the perf ceiling that the word-shape API unlocks.
- `RecordFilterBenchmarkTest` with its nullable-column scenarios — regression gate; not expected to move materially because the per-row check is not the end-to-end bottleneck.

## Non-goals

- Faster end-to-end `RowReader` throughput on nullable columns. The win this design exposes is consumer-controlled (word-wise iteration); `FlatRowReader`'s row-by-row API contract cannot consume it. End-to-end gains for that API shape require carrying def-level RLE through the pipeline (separate design).
- Use of page-level `numNulls` to skip validity work entirely on no-null pages (separate design).
- Arrow bit-compatible memory layout. The shared piece remains the model, not the bytes — see [COLUMN_READER_ARROW_LAYOUT.md](COLUMN_READER_ARROW_LAYOUT.md).
- A no-copy publish path in `FlatColumnWorker` (the `Arrays.copyOf` at `publishCurrentBatch` stays; structural cleanup tracked separately).
