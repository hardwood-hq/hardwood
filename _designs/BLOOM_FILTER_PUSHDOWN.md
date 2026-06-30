# Plan: Bloom Filter Predicate Push-Down (#105)

**Status: Implemented**

## Context

A Parquet column chunk may carry a [split-block bloom filter](https://github.com/apache/parquet-format/blob/master/BloomFilter.md) at `bloom_filter_offset`. The read path that decodes that filter and exposes a membership primitive already exists (see [BLOOM_FILTER_SUPPORT.md](BLOOM_FILTER_SUPPORT.md)): `BloomFilterReader` parses the header and bitset, and `BloomFilter.mightContain(long hash)` answers a probabilistic membership query (`false` is definitive, `true` may be a false positive).

This feature consults that filter during row-group pruning. Min/max statistics can only drop a row group when the predicate value falls outside `[min, max]`; a bloom filter additionally drops a row group when the value lies *inside* the statistics range but was never written — the case statistics cannot catch. It applies to equality (`EQ`) and membership (`IN`) predicates and complements, rather than replaces, statistics-based filtering: both run, and either one dropping the row group is sufficient.

Pruning is automatic whenever a filter predicate is supplied; there is no public API or CLI surface, and all new types live in `dev.hardwood.internal.*`.

## Scope

Bloom-filter pruning applies to a leaf predicate when **all** of the following hold:

- The operator is `EQ`, or the predicate is an `IN` list.
- The column's physical type is one of `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BYTE_ARRAY`, or `FIXED_LEN_BYTE_ARRAY` — the types for which the resolved predicate value maps unambiguously to the value's plain encoding, which is what the writer hashed. This corresponds to the resolved leaf types `IntPredicate` / `IntInPredicate` (→ `XxHash64.hash(int)`), `LongPredicate` / `LongInPredicate` (→ `XxHash64.hash(long)`), `FloatPredicate` (→ `XxHash64.hash(float)`), `DoublePredicate` (→ `XxHash64.hash(double)`), and `BinaryPredicate` / `BinaryInPredicate` (→ `XxHash64.hash(byte[])`). `FLOAT` / `DOUBLE` are `EQ`-only — there is no floating-point `IN` leaf.
- For `FLOAT` / `DOUBLE`, the probe value is neither `±0.0` nor `NaN`. Bloom hashing is over the raw IEEE-754 bits, which distinguish `-0.0` from `+0.0` and every NaN payload, whereas value equality treats `-0.0 == +0.0` and `NaN != NaN`. Probing those by raw bits could prove a value absent that an equal stored value would match, so they are never pruned (statistics still apply). `hash(float)` / `hash(double)` reuse the `int` / `long` primitives via `floatToRawIntBits` / `doubleToRawLongBits`, matching parquet-java's `putFloat` / `putDouble`.
- The column chunk actually carries a bloom filter (`bloomFilterOffset != null`).

Everything else falls through to statistics only: `NOT_EQ` and range operators (a bloom filter cannot prove them), `FLOAT16` / `BOOLEAN` columns, `±0.0` / `NaN` floating-point equality, and any column without a bloom filter. The `signed` flag on `BinaryPredicate` (FLBA decimals) does not affect hashing — the writer hashes the raw value bytes regardless of sign interpretation — so signed and unsigned binary equality are both eligible.

`EQ` drops the row group when the value is absent. An `IN` list drops the row group only when **every** listed value is absent (any possibly-present value keeps it).

## Design

### `internal/predicate/BloomFilterSource.java`

A column-indexed accessor for a row group's bloom filters, decoupling the evaluator from I/O so it stays unit-testable:

```java
interface BloomFilterSource {
    /// The bloom filter for the given original column index, or `null` if the
    /// column chunk has none.
    BloomFilter forColumn(int columnIndex);
}
```

### `internal/predicate/RowGroupBloomFilterSource.java`

The I/O-backed implementation over one `(InputFile, RowGroup)` pair. It reads a column's filter lazily — only when the evaluator probes it — and caches the result (including absence) for the lifetime of one row group's evaluation, so an `IN` list probing the same column reads the filter once. Evaluation of a row group is single-threaded (a sequential `stream().filter(...)`), so the cache — parallel `BloomFilter[]` / `boolean[]` arrays indexed by column position — needs no synchronization.

Reading a filter:

- `bloomFilterLength` present (the common case; written by current parquet-mr and PyArrow): one `readRange(offset, length)`, then `BloomFilterReader.read`.
- `bloomFilterLength` absent (legacy writers): read a small fixed probe window to parse the header, derive the total length as `headerBytes + numBytes`, then `readRange(offset, total)` and `BloomFilterReader.read`.

An `IOException` while reading a filter the footer declared is treated as corruption and surfaced as an `UncheckedIOException` carrying the file name (fail-early), consistent with `RowGroupIndexBuffers.fetch`.

### `RowGroupFilterEvaluator`

`canDropRowGroup` gains a `BloomFilterSource` parameter, threaded unchanged through the `And` / `Or` recursion. The existing two-argument overload delegates with a `null` source (statistics only), preserving current callers and tests.

For each eligible leaf the evaluator yields `statisticsDrop || bloomDrop`:

- statistics are evaluated first (no I/O);
- the bloom filter is consulted only if statistics did not already drop the group and the source is non-null — so a query with no equality/`IN` predicates, or a file with no bloom filters, performs no bloom I/O.

A `null` source short-circuits every bloom check to "cannot drop", so the bloom path is inert unless a caller supplies a source.

### Callers

Bloom pruning is wired in a single place. `RowGroupIterator.filterRowGroups` constructs a `RowGroupBloomFilterSource` for each row group under test and passes it to `canDropRowGroup`. This is the only statistics/bloom evaluation site: both multi-file scans and the single-file reader path run through the same iterator, so the single-file path reaches it there too. `ParquetFileReader.filterRowGroups` applies only the byte-range `RowGroupPredicate` (split selection) and never constructs a bloom source or evaluates statistics.

Row groups dropped by a bloom filter are counted in the existing `RowGroupFilterEvent.rowGroupsSkipped`, alongside statistics drops.

## Testing

- **`RowGroupBloomFilterSource`**: reads a real filter from `bloom_filter_test.parquet` for a bloom-bearing column and returns `null` for the column without one (`value`).
- **Evaluator** (`BloomFilterPushDownTest`) against `bloom_filter_test.parquet` (single row group, 64 rows; bloom filters on `id` INT64 `0..63`, `code` INT32 `0,3,…,189`, `name` STRING `""`…`"x"*63`, `price` FLOAT `0,2,…,126`, `ratio` DOUBLE `0,0.5,…,31.5`, `dec` DECIMAL(38,0) stored as FLBA(16) `0,2,…,126`, `ts` TIMESTAMP(us,UTC) at even-second offsets from `2024-01-01`; `value` INT64 has none):
  - `eq("code", 1)` — `1 ∈ [0, 189]` so statistics keep the group, but `1` is not a multiple of 3, so the bloom filter drops it. This is the discriminating case statistics cannot catch.
  - `eq("code", 3)` — present; the group is kept.
  - `eq("name", "w")` — sorts within `["", "x"*63]` but was never written; bloom drops it.
  - `eq("price", 1.0f)` / `eq("ratio", 0.25)` — in range but never written; bloom drops them. `eq("price", 2.0f)` / `eq("ratio", 0.5)` — present; kept.
  - `eq("dec", 1)` — in range but never written; bloom drops it. `eq("dec", 2)` — present; kept. Covers the FLBA equality path with real fixed-length bytes (a DECIMAL/FLBA column, since a raw unannotated FLBA column is not reachable through the public predicate API).
  - `eq("ts", Instant)` — a logical-type predicate resolves to the physical INT64 (epoch micros); an odd-second instant is in range but never written, so the bloom filter drops it on the physical value. Confirms logical types (`Instant`/`LocalTime`/`LocalDate`/`BigDecimal`/`UUID`) prune through their resolved physical encoding.
  - `eq("price", -0.0f)` — `+0.0` is stored and `-0.0f == +0.0f`, so the `±0` carve-out keeps the group rather than pruning on the raw-bit hash.
  - `eq("value", …)` — no bloom filter; falls back to statistics only.
  - `in("code", [1, 2])` — all absent; dropped. `in("code", [1, 3])` — `3` present; kept.
  - An `AND` whose bloom-eligible leaf is absent drops the group; an `OR` requires every branch to drop.
- **Reader, end-to-end** (`BloomFilterEndToEndTest`): the same prune is exercised through the public `ColumnReader`, `ColumnReaders`, and `RowReader` APIs — an absent in-range value yields zero rows (the only row group is dropped), a present value returns its single row — including the `ts` logical-type path through `RowReader`.
- **parquet-java oracle** (`BloomFilterParquetJavaOracleTest`, test-scoped `org.apache.parquet:parquet-column`): cross-checks against the reference implementation in two parts that compose into decision-for-decision parity — (1) `XxHash64.hash` equals `BlockSplitBloomFilter.hash` for `INT32` / `INT64` / `FLOAT` / `DOUBLE` / binary; (2) on a bitset built and serialized by parquet-java, Hardwood's `BloomFilter.mightContain` matches `BlockSplitBloomFilter.findHash` for every stored value and a wide sweep of absent ones (false positives included).
- The existing statistics-only `RowGroupFilterEvaluator` / `FilterPredicateTest` suites continue to pass unchanged via the two-argument overload.
