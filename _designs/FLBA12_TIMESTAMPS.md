# Design: FLBA(12) extended-precision timestamps

**Status: Implemented.** Tracking issue: #921.

## Goal

Read and write Parquet columns whose `TIMESTAMP` logical type is carried on a
`FIXED_LEN_BYTE_ARRAY(12)` physical type, as parquet-format defines it after its 2.14.0 release
([apache/parquet-format#601](https://github.com/apache/parquet-format/pull/601)), with the
accessor surface, filter predicates, statistics pruning and CLI rendering that the `INT64` form
has.

## The format

A value is 12 bytes holding a **signed two's complement little-endian 96-bit integer**, counting
the logical type's `TimeUnit` since the Unix epoch. The annotation is the ordinary
`TimestampType`: all three units and both `isAdjustedToUTC` settings are valid, and mean exactly
what they mean for `INT64`.

96 bits of nanoseconds spans roughly ±1.26 × 10¹² years, so the form covers the ANSI SQL
`TIMESTAMP(9)` range of years 0001–9999 that `INT64` nanoseconds, good for about 585 years,
cannot.

The sort order is signed: the represented value, compared as a two's complement integer. Pre-epoch
timestamps are negative, and their stored bytes do not order as unsigned byte strings.

No new encodings are involved: `PLAIN`, `RLE_DICTIONARY`, `DELTA_BYTE_ARRAY` and
`BYTE_STREAM_SPLIT` already apply to `FIXED_LEN_BYTE_ARRAY`.

The deprecated `TIMESTAMP_MILLIS` and `TIMESTAMP_MICROS` converted types annotate an `INT64` only.
A `FIXED_LEN_BYTE_ARRAY(12)` timestamp carries the `LogicalType` union member alone.

This is unrelated to the legacy `INT96`, which is also 12 bytes but packs a Julian day beside a
count of nanoseconds of the day and carries no annotation.

## Schema

`LogicalTypeConverter.conversionFault` accepts `TIMESTAMP` on `INT64` and on
`FIXED_LEN_BYTE_ARRAY(12)`. A `TIMESTAMP` on any other width, and a `TIMESTAMP_MILLIS` /
`TIMESTAMP_MICROS` converted type on anything but `INT64`, is a faulted annotation: `FileSchema`
drops it with a warning and `BoundsReadability` reads no bounds for the column, as for every other
faulted annotation.

`LogicalTypeValidator` refuses the same pairings to the writer. `LogicalTypeAnnotations.of` takes
the physical type and derives no converted type for the `FIXED_LEN_BYTE_ARRAY(12)` carrier.

## Range and the Java types

The accessors return `Instant` and `LocalDateTime`, whose range (about a billion years either side
of the epoch) contains the SQL range the format targets but is narrower than the 96-bit space. A
stored value outside that range fails to decode with `DateTimeException`, the exception `java.time`
raises for an instant past its range, naming the stored bytes. Both ways a value can be out of range
report through that one exception: a count of seconds that exceeds 64 bits, and one that fits but
lands past `Instant` or `LocalDateTime`. The untruncated value stays reachable as the stored bytes
through `getBinary`, `getRawValue` and `ColumnReader`.

The writer has no range to check: every `Instant` and every `LocalDateTime`, counted in nanoseconds,
fits 96 bits.

## Decoding

`dev.hardwood.internal.conversion.Flba12Timestamps` is the one place that knows the layout. The flat
row reader, the nested and list paths, `getValue`, the writer's encoder and the CLI all go through
it.

A 96-bit value is carried as its two machine words: `hi`, the top 32 bits as a signed `int`, and
`lo`, the low 64 bits as an unsigned `long`. The stored layout is little-endian, so each word is one
unaligned native load through a `byteArrayViewVarHandle`.

Splitting the count into seconds and nanoseconds of the second is a division by a small positive
constant `d` (1 000, 1 000 000 or 1 000 000 000). It runs in integer arithmetic on 32-bit limbs,
never `BigInteger`: for a nanosecond column the format exists for values that exceed a `long`, so
the division is on the hot path for ordinary data.

```
magnitude  = |V|, as three unsigned 32-bit limbs a2:a1:a0
rem = 0
for each limb a from a2 down to a0:
    acc  = (rem << 32) | a          // rem < d < 2^30, so acc < 2^62: no overflow
    q_i  = acc / d
    rem  = acc % d
```

`q2` must be zero and `q1` below 2³¹ for the seconds to fit a `long`; otherwise the value is out of
range. A negative value is negated to its magnitude before the division and the quotient adjusted
afterwards, so the result floors, as `Math.floorDiv` does on the `INT64` path.

The sub-second part needs no second pass over the limbs. `value = seconds * d + remainder` holds
over the integers, so it holds modulo 2⁶⁴, and the remainder is below 2³⁰: `lo - seconds * d` in
wrapping `long` arithmetic is the remainder exactly.

## Encoding

`Flba12Timestamps.encode` is the inverse: the epoch second times `d`, plus the nanoseconds of the
second divided by the unit's nanoseconds, as a 96-bit value in two words, sign-extended into the
top word. `epochSecond * d` overflows a `long` for nanoseconds, so it is computed as a
`Math.multiplyHigh` / wrapping-multiply pair and the unit count added with carry.

The writer's `PhysicalValueConverter` narrows a value finer than the column's unit under the
column's `PrecisionLossPolicy` exactly as for `INT64`: `REJECT` refuses it, `TRUNCATE` floors it.
The nanoseconds of the second are never negative, so dropping sub-unit digits floors the value.

## Accessors

| Path | FLBA(12) `TIMESTAMP` |
|---|---|
| `getTimestamp` / `getLocalTimestamp` (flat, struct, map value) | decoded from the batch's packed bytes at the value's offset, without copying them out |
| `PqList.timestamps()` / `localTimestamps()` | the same, per element |
| `getValue` | `Instant` or `LocalDateTime`, as the UTC flag says |
| `getRawValue`, `getBinary`, `ColumnReader` | the 12 stored bytes |

`TimestampAccessorKind` checks the UTC flag exactly as for `INT64`. The readers branch on the
physical type where they read the stored value: a `long[]` for `INT64`, packed binary for
`FIXED_LEN_BYTE_ARRAY`.

## Filter predicates

The literal rule is the one `PREDICATE_LITERALS.md` states for every column.

- **Typed literal.** `Instant` on a UTC column, `LocalDateTime` on a local one, for every operator.
  The literal is measured in the column's unit against the 96-bit range, which every `Instant` fits,
  so only the unit can put a literal between two held values: an equality literal finer than the
  unit is refused, an order literal moves to the nearest held value on the side its operator
  admits. The held value is encoded as the 12 bytes the column stores.
- **`byte[]` literal.** Twelve bytes, equality and membership only, compared as the value they
  encode, which has exactly one byte encoding. `lt`, `ltEq`, `gt` and `gtEq` with a `byte[]` throw,
  naming the typed literal that orders the column.

Both resolve to `BinaryPredicate` / `BinaryInPredicate` with `Comparison.FIXED_TIMESTAMP`:

- `compare` is `BinaryComparator.compareSignedLittleEndian`, which compares the top byte signed and
  the remaining eleven unsigned, from the most significant byte down.
- `byteExact` is true: a value has one 12-byte encoding, so the dictionary and Bloom filter answer
  for it.

Row-group statistics and the page index are read in that order through `MinMaxStats.BinaryStats`;
an inverted pair is discarded as for every other binary order. Record-level filtering compares the
same way. The batch path has no binary matchers, so a `FIXED_TIMESTAMP` leaf is evaluated per
record, as every other binary leaf is.

## Writing

A column declared `FIXED_LEN_BYTE_ARRAY(12)` with `LogicalType.timestamp(isAdjustedToUTC, unit)`:

- `setTimestamp` / `setLocalTimestamp` and the list and map builders' counterparts encode the value
  through `PhysicalValueConverter` into 12 bytes; `setBinary` stores 12 bytes as given.
- `ColumnBatch` takes the 12-byte values as for any `FIXED_LEN_BYTE_ARRAY`.
- `BinaryStatistics.forColumn` selects `BinaryStatisticsCollector.Order.SIGNED_LITTLE_ENDIAN`,
  which compares through `BinaryComparator.compareSignedLittleEndian`, so a chunk's bounds and a
  predicate over them compare in the same order. Bounds are never truncated.
- The footer carries the `TimestampType` union member and no converted type.

No public API is added: the schema builder, the setters and `ColumnBatch` already cover the column.

## Other modules

- **CLI.** `ValueFormatter` renders values, dictionary entries and statistics bounds as the
  `Instant` or `LocalDateTime`, and `inspect` and `dive` go through it.
- **Avro.** Avro has no 96-bit timestamp, so the column maps to `fixed(12)` holding the stored
  bytes, as `INT96` does.
- **parquet-java compat.** The compat schema gives the column no `OriginalType`, since
  `TIMESTAMP_MILLIS` / `TIMESTAMP_MICROS` annotate an `INT64` only; values reach a `Group` as the
  stored `Binary`.

## Tests

- `Flba12TimestampsTest` covers decode and encode at the limb boundaries, negative values, every
  unit, and both out-of-range failures.
- Reader tests run over two fixtures: `flba12_timestamp.parquet` from apache/parquet-testing,
  written by parquet-java, whose six documented values include two beyond `INT64` nanoseconds; and a
  `simple-datagen.py` fixture with local and UTC columns, nulls, a dictionary-encoded column, a
  list, and statistics written in the signed order.
- The parquet-testing fixture has no parquet-java oracle: no release up to 1.18.1 reads the
  annotation (apache/parquet-java#3680 adds it), so `ParquetComparisonTest` skips it and a dedicated runner test pins its values.
- Predicate tests cover resolution, statistics and page-index pruning across the epoch, and the
  refused `byte[]` order.
- Writer tests round-trip every unit and both flags through the row and column writers, and compare
  the statistics a written file carries with the ones parquet-java wrote into the parquet-testing
  fixture for the same values.

## Benchmark

`Flba12TimestampDecodeBenchmark` in `performance-testing/micro-benchmarks` decodes 1 000 000 values
from an in-memory buffer at two levels, for both carriers:

- **raw** (`flba12Words`, `int64Long`): bytes to the integer representation, summed into a sink.
- **instant** (`flba12Instant`, `int64Instant`): the same values to an `Instant[]`.

Three further arms measure the byte order: `flba12WordsBigEndianSwap` (native load plus a byte
swap), and `flba12WordsByteByByte` / `flba12WordsBigEndianByteByByte` for the assemble-from-bytes
form at both orders.

Measured on an Intel N300 (Gracemont, pinned at 1.50 GHz, one core, nanosecond unit), with
`-f 3 -i 10` overriding the class's fork and measurement counts:

| | FLBA(12) | INT64 | ratio |
| --- | --- | --- | --- |
| raw load | 1.383 ns/value | 0.801 ns/value | 1.73x |
| to `Instant` | 38.25 ns/value | 23.62 ns/value | 1.62x |

Both arms of the second row construct the same `Instant`s, which accounts for most of either figure.
Of the 14.6 ns/value between them, the wider load accounts for 0.6 ns; the rest is the 96-bit floor
division, three chained divides against one `Math.floorDiv`.

| Byte order | ns/value |
| --- | --- |
| little-endian, native load | 1.376 |
| big-endian, native load + byte swap | 1.934 |
| little-endian, assembled byte by byte | 12.907 |
| big-endian, assembled byte by byte | 10.125 |

On a little-endian machine each word of the stored layout is one unaligned load; big-endian costs a
byte reversal per word. Assembling from single bytes is an order of magnitude slower at either
order, and reverses which order wins, so the comparison between layouts is made at native loads.
