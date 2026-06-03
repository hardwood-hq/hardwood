# Local timestamp accessor

**Status: Implemented**

## Context

The Parquet TIMESTAMP logical type carries an `isAdjustedToUTC` flag that distinguishes two distinct semantic kinds:

- `isAdjustedToUTC = true` — the stored value is an **instant** normalized to UTC. The natural Java mapping is [Instant].
- `isAdjustedToUTC = false` — the stored value is a **local** wall-clock timestamp with no timezone. It is *not* an instant. The natural Java mapping is [LocalDateTime].

The reader exposes a typed accessor pair, one per kind. Calling the wrong one for a column is a programming error that throws at runtime — the same model already used for other "wrong accessor for the column" mismatches (e.g. `getInt` on a `LONG` column).

The Variant binary format carries four timestamp tags (`TIMESTAMP`, `TIMESTAMP_NANOS` and the `*_NTZ*` variants). The Variant decoders mirror the row-side split.

## Public API

`FieldAccessor` / `StructAccessor` / `PqVariantObject` (the by-name + by-index pair):

```java
Instant       getTimestamp(String name);       // requires isAdjustedToUTC = true
Instant       getTimestamp(int fieldIndex);
LocalDateTime getLocalTimestamp(String name);  // requires isAdjustedToUTC = false
LocalDateTime getLocalTimestamp(int fieldIndex);
```

`PqMap.Entry`:

```java
Instant       getTimestampValue();
LocalDateTime getLocalTimestampValue();
```

`PqList` (typed list views):

```java
List<Instant>       timestamps();
List<LocalDateTime> localTimestamps();
```

`PqVariant`:

```java
Instant       asTimestamp();        // TIMESTAMP / TIMESTAMP_NANOS tags
LocalDateTime asLocalTimestamp();   // TIMESTAMP_NTZ / TIMESTAMP_NTZ_NANOS tags
```

`FieldAccessor.getValue(name)` returns `Instant` for UTC-adjusted TIMESTAMP columns and `LocalDateTime` for local-wall-clock columns. `getRawValue` is unchanged — both kinds surface their underlying `Long` micros / nanos.

`ColumnReader` has no typed timestamp accessor and stays unchanged; the perf-first column-batch path leaves the int64-to-temporal conversion to the caller.

## Errors

`IllegalStateException` thrown by the accessor with a message of the form:

```
Column 'tpep_pickup_datetime' is a local-wall-clock TIMESTAMP (isAdjustedToUTC=false); use getLocalTimestamp instead
```

The exception type, message shape, and the column-name + flag-value content are part of the documented contract — `docs/content/how-to/row-reader.md` directs callers to inspect `LogicalType.TimestampType.isAdjustedToUTC()` ahead of time when the kind isn't statically known.

## Legacy INT96

INT96 TIMESTAMP columns have no `isAdjustedToUTC` field. The reader treats them as UTC-adjusted (Spark / Hive convention) and surfaces them through `getTimestamp` as `Instant`. `getLocalTimestamp` rejects INT96.

## TIME accessor

`getTime` returns `LocalTime` for both `isAdjustedToUTC` values because `LocalTime` is already zoneless. No second accessor is added; the flag is informational and is documented as such — callers who care inspect `LogicalType.TimeType.isAdjustedToUTC()`.

## Internal converters

`LogicalTypeConverter` exposes two split helpers:

```java
Instant       convertToTimestamp(Object value, PhysicalType pt, LogicalType.TimestampType tt);
LocalDateTime convertToLocalTimestamp(Object value, PhysicalType pt, LogicalType.TimestampType tt);
```

Each enforces the `isAdjustedToUTC` precondition (throws `IllegalStateException` if called for the wrong kind) as defense in depth; accessor sites do the user-facing check earlier so the rejection message names the column.

The generic `LogicalTypeConverter.convert` switch dispatches the `TimestampType` arm on `isAdjustedToUTC` so `ValueConverter.convertValue` (which backs `getValue` and the `PqList.values()` / `PqMap.Entry.getValue()` fallback path) automatically returns the right Java type.

`VariantValueDecoder` splits along the same line:

```java
Instant       asTimestamp(byte[] buf, int offset);       // TIMESTAMP / TIMESTAMP_NANOS
LocalDateTime asLocalTimestamp(byte[] buf, int offset);  // TIMESTAMP_NTZ / TIMESTAMP_NTZ_NANOS
```

Each rejects the wrong tag set via `VariantErrors.expectedOneOf`.

The shared `TimestampAccessorKind` helper produces the standard rejection message so every row-level accessor implementation (`FlatRowReader`, `NestedBatchDataView`, `PqStructImpl`, `PqMapImpl`, `PqListImpl`) emits the same wording.

## Tests

`LocalTimestampTest` covers the row-reader path against a generated `local_timestamp_test.parquet` fixture with three columns at the same wall-clock values: NTZ MILLIS, NTZ MICROS, UTC MICROS. It exercises:

- `getLocalTimestamp` on local columns returns the wall-clock value verbatim.
- `getTimestamp` on a local column throws `IllegalStateException` naming the column and `isAdjustedToUTC=false`.
- `getLocalTimestamp` on a UTC column throws naming the column and `isAdjustedToUTC=true`.
- `getTimestamp` on the UTC column still returns the expected `Instant`.
- `getValue` on a local column returns `LocalDateTime`; on a UTC column returns `Instant`.

`VariantValueDecoderTest` covers the Variant tags (`asTimestamp` for `TIMESTAMP` / `TIMESTAMP_NANOS`, `asLocalTimestamp` for the NTZ variants).

`YellowTripDataTest` (core + integration-test) was updated to read `tpep_pickup_datetime` / `tpep_dropoff_datetime` via `getLocalTimestamp` — the file is a real-world NYC TLC sample whose TIMESTAMP columns are `isAdjustedToUTC = false`, which was being silently misread as UTC before the fix.
