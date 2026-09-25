# Annotation range checks (#9, stage 16a)

**Status: Complete.** Tracking issue: #9. Delivery stage 16a (Gate) of
[WRITER_SUPPORT.md](../_plans/WRITER_SUPPORT.md).

## Context

A logical-type annotation narrows what its physical type may hold. An `INT32` carries any
of its 2^32 bit patterns, but an `INT32` annotated `INT(8)` carries 256 of them, and an
`INT32` annotated `DECIMAL(9, 2)` carries the unscaled values of at most nine digits. The
narrowing is not decoration: a consumer reads the column through the annotation, so a
`uint8` reader returns 44 for a stored 300, and the `min`/`max` the writer puts in the
column chunk's statistics describe values the annotation says cannot exist — a reader that
prunes on those bounds prunes against a range no value of the column can occupy.

Stage 16 gave the row-oriented layer a range check on `setInt`. It left the two write APIs
disagreeing about the same value: `RowWriter.setInt` rejects 300 on a `UINT_8` column while
`ColumnBatch.ints` takes it and writes it. Which entry point the caller picked decided
whether the file came out conformant.

## Goal

Neither write API produces a value outside the range its column's annotation declares. The
check is a property of the column, resolved from its declared type, and both APIs apply the
same one.

## What an annotation bounds

The bound is on the *stored* value — the physical value that lands in the page, which is
what both APIs' physical setters take.

| Column | Bound on the stored value |
|---|---|
| `INT32` annotated `INT(8)` / `INT(16)` | `[-2^(w-1), 2^(w-1) - 1]` |
| `INT32` annotated `UINT_8` / `UINT_16` | `[0, 2^w - 1]` |
| `INT32` / `INT64` annotated `TIME(unit)` | `[0, one day of unit)` |
| `INT32` annotated `DECIMAL(p, s)` | `[-(10^p - 1), 10^p - 1]` |
| `INT64` annotated `DECIMAL(p, s)` | `[-(10^p - 1), 10^p - 1]` |
| `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` annotated `DECIMAL(p, s)` | a non-empty two's complement value of magnitude at most `10^p - 1` |
| any column annotated `UNKNOWN` | no value at all — every row is null |
| anything else | none |

`DECIMAL` bounds the unscaled value, not the number it denotes: the scale is a fixed
divisor the annotation carries, so `p` digits of unscaled value is the whole constraint.
A precision that already spans the physical type — `DECIMAL(10)` or wider on an `INT32`,
`DECIMAL(19)` or wider on an `INT64` — bounds nothing, and is treated as unbounded rather
than checked per value.

### What is deliberately not bounded

- **`UINT_32` and `UINT_64`.** Every bit pattern of the underlying `int` / `long` is a
  valid value of the column, and spelling one above `Integer.MAX_VALUE` / `Long.MAX_VALUE`
  as a negative is the only way to reach it — which is also how the reader returns it.
  A bound here would cost expressiveness rather than buy conformance. `INT(32)` and
  `INT(64)` are unbounded for the same reason: they are the physical type's own range.
- **`DATE` and `TIMESTAMP`.** parquet-format is explicit for `TIMESTAMP` — *"Every possible
  `int64` number represents a valid timestamp"* — and every `int32` day count is a date
  `LocalDate` can hold, so neither bounds anything.
- **Fixed byte lengths.** `FIXED_LEN_BYTE_ARRAY` values are already checked against the
  declared type length, on both APIs, since stage 12.

### `TIME`, where the format is silent

`TIME` is the one annotation whose bound the format states only by implication. It defines the
value as *"the number of milliseconds after midnight"* (and the micro/nano equivalents) without
naming a range or saying what a reader does with a value past a day.

The bound is enforced anyway, because the alternative is a file this project's own reader cannot
read through the column's annotation: `LogicalTypeConverter.convertToTime` materializes a
`LocalTime`, which rejects a value of a day or more. A full day is outside the range rather than
the last value in it, matching `LocalTime` and rejecting the `24:00:00` spelling some producers
emit for the end of a day.

### The two empty ranges

Two annotations admit fewer values than a range expresses, and both were reachable before this
increment:

- **An empty binary `DECIMAL` value.** Two's complement has no zero-byte encoding, so an empty
  value denotes no unscaled value at all and `convertToDecimal` raises `Zero length BigInteger`
  on it. It is rejected wherever it is handed over. The row layer used exactly that empty value
  as the placeholder of a slot an absent ancestor makes unreachable, so under a `DECIMAL`
  annotation the placeholder is a decodable zero instead — the same adjustment a
  `FIXED_LEN_BYTE_ARRAY` placeholder needs to carry the declared width.
- **`UNKNOWN`.** The annotation says the column holds only nulls, and the reader throws on any
  value found under one. The schema validator already refuses to declare a `REQUIRED` column
  `UNKNOWN` for that reason; accepting values on the `OPTIONAL` one it does allow left the same
  file unreadable by a longer route, so every row of an `UNKNOWN` column must be null.

## Where the check lives

`LogicalTypeValueRange` (`dev.hardwood.internal.writer`) resolves a column's bound once from its
`ColumnSchema` and answers `contains(long)` for an integral column and
`containsUnscaled(byte[])` for a binary `DECIMAL`. An unbounded column resolves to a
shared instance that reports `isBounded() == false`, so a caller skips the scan entirely.

`UNKNOWN` sits at the other end and is the one bound the object does not answer with a value
predicate: its range is empty, so there is no value to test. It reports `holdsNoValue()` instead,
and each API enforces it on the shape it has to hand — `ColumnBatch` on the column's nulls, the
row layer by refusing the setters. The derivation still lives in one place, which is what keeps
the two from disagreeing about which columns hold nothing.

Both APIs consume that one object, which is what keeps them from drifting apart:

- **`RowWriter`** resolves it per leaf when the row plan is built, and checks in the setter,
  before anything is staged. The rejection names the field the caller set (`Field addr.zip:
  300 is out of range for a UINT_8 column`), which is the row layer's whole idiom.
- **`ColumnBatch`** takes the array the writer resolved once per file, and scans the values
  a setter is handed, skipping the rows the batch's [Validity] marks null — exactly where
  and how `validateBinaryValues` already checks fixed byte lengths. The rejection names the
  column and the offending row (`Column 3 (zip) has value 300 at row 17, out of range for a
  UINT_8 column`).

A slot an absent ancestor makes unreachable is not a value either API checks. The row layer marks
that slot null when the leaf is `OPTIONAL`, which is what it is, and the batch skips every null
row; only a `REQUIRED` leaf, which has no null bit to set, keeps a placeholder — and its
placeholder is a value the column can hold, the declared width for a `FIXED_LEN_BYTE_ARRAY` and a
decodable zero under a `DECIMAL`.

The row layer's setter check is the one that reports usefully; the batch scan is the
backstop under it. A row-written batch therefore passes its values through both, which is
one extra bounds compare per value of a narrowly annotated column — the row path's own
per-value work already dwarfs it.

### Cost

An unannotated or unbounded column pays a single `isBounded()` test per setter call and
nothing per value, which is every column in a benchmark-shaped schema. A bounded integral
column pays two compares per value against fields resolved before the loop. A binary
`DECIMAL` pays a byte-length compare per value, and constructs a `BigInteger` only for a
value long enough to possibly exceed the precision: a value of `L` bytes cannot exceed
`10^p - 1` when `2^(8L-1) <= 10^p - 1`, so the largest always-safe length is derived once
per column from the bound's bit length. That skip is what a `BYTE_ARRAY` column buys, whose
values carry their own length and are usually far shorter than the declared precision
allows. A `FIXED_LEN_BYTE_ARRAY` never reaches it: the schema validator caps the declared
precision at what the width holds, which leaves the widest values of that width above the
bound, so every value is decoded.

## Testing

- Per bounded annotation, the extremes of the declared range are accepted and the values
  just outside it are rejected, through `ColumnBatch` and through `RowWriter`, so neither
  API can regress alone.
- The columnar rejection names the row it found, including under a null mask, where the
  ignored values at null rows must not be checked at all.
- A `BYTE_ARRAY` `DECIMAL` accepts a value too short to reach its precision and rejects a
  longer one that exceeds it, so the always-safe length is exercised on both sides.
- A `TIME` rejects a full day of its unit at both entry points, so the boundary the reader
  enforces and the one the writer enforces are the same one.
- The unbounded annotations (`UINT_32`, `UINT_64`, `DATE`, `TIMESTAMP`) keep accepting the
  values that only they can express, including the negative spellings of the large unsigned
  ones.
- `WriterReaderSymmetryTest` writes the extremes of what every annotation admits and reads them
  back through the accessor for that annotation, so the writer's accepted range and the reader's
  materializable range are asserted to be the same range rather than assumed to be.
