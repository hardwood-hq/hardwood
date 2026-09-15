/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import dev.hardwood.row.PqInterval;

/// A predicate filtering the rows a reader returns.
///
/// A reader returns exactly the rows the predicate matches, except for [#intersects], which
/// decides whole row groups. It also pushes the predicate down: row groups and pages whose
/// statistics, dictionary or Bloom filter prove that no row can match are skipped without being
/// decoded. A literal is a value the column's accessors return; the Query Controls reference lists
/// the literals and operators each column type takes.
///
/// Usage examples:
/// ```java
/// // Simple comparison
/// FilterPredicate filter = FilterPredicate.gt("age", 21);
///
/// // Compound predicate
/// FilterPredicate filter = FilterPredicate.and(
///     FilterPredicate.gtEq("salary", 50000L),
///     FilterPredicate.lt("age", 65)
/// );
///
/// // Use with reader
/// try (ColumnReader reader = fileReader.buildColumnReader("salary").filter(filter).build()) {
///     while (reader.nextBatch()) { ... }
/// }
/// ```
///
/// ## Null handling
///
/// All comparison predicates (`eq`, `notEq`, `lt`, `ltEq`, `gt`, `gtEq`, `in`)
/// follow SQL three-valued logic: comparing a null column value
/// against any operand yields UNKNOWN, and rows whose predicate is UNKNOWN are
/// not returned. In practice this means **rows with a null in the tested column
/// are never returned by a comparison predicate**. Use `isNull` / `isNotNull`
/// for explicit null checks, or `or(...)` to include null rows alongside a
/// comparison — e.g. `or(gt("age", 30), isNull("age"))`.
///
/// `not(p)` preserves this behavior: rows where `p` is UNKNOWN stay UNKNOWN
/// under negation and are dropped. The SQL identity `not(gt(x, v)) ≡ ltEq(x, v)`
/// holds on all rows, including null ones.
///
/// This matches the SQL semantics of `WHERE` predicates and differs from
/// parquet-java's `notEq`, which treats `null <> v` as true and therefore
/// includes null rows. To reproduce parquet-java's behavior in Hardwood, write
/// the null-inclusion explicitly: `or(notEq("x", v), isNull("x"))`.
///
/// ## Float and double comparisons
///
/// Predicates on `float` and `double` columns use the [Float#compare] /
/// [Double#compare] total order, not IEEE 754 equality. Two consequences
/// matter in practice:
///
/// - `-0.0` is strictly less than `+0.0`. `eq(0.0)` matches only `+0.0`
///   values; to match either zero, use `in(column, 0.0, -0.0)`.
/// - `NaN` sorts above every other value, and every `NaN` equals every other.
///   `eq(NaN)` matches only `NaN` (whereas IEEE `NaN == anything` is always
///   false). `lt` against any value, and `ltEq` against a number, never match
///   `NaN` rows; `gt` and `gtEq` against a number, and `ltEq` and `gtEq`
///   against `NaN`, include them.
///
/// `in(column, float...)` and `in(column, double...)` apply the same total order to each
/// listed value. `in(column, float...)` takes a `FLOAT` or `FLOAT16` column and
/// `in(column, double...)` a `DOUBLE` column, as `eq` does.
///
/// Row-group and page pruning never drops a `NaN` row that a predicate
/// matches. Statistics whose `min` or `max` is `NaN` are not used for pruning.
/// A predicate that a `NaN` value satisfies — `notEq`, `gt` or `gtEq` against
/// a number, or `eq`, `ltEq` or `gtEq` against `NaN` — prunes a `FLOAT`,
/// `DOUBLE` or `FLOAT16` column from its statistics only where the row group
/// or page records a `nan_count` of zero.
public sealed interface FilterPredicate
        permits FilterPredicate.IntColumnPredicate,
                FilterPredicate.LongColumnPredicate,
                FilterPredicate.FloatColumnPredicate,
                FilterPredicate.DoubleColumnPredicate,
                FilterPredicate.BooleanColumnPredicate,
                FilterPredicate.BinaryColumnPredicate,
                FilterPredicate.StringColumnPredicate,
                FilterPredicate.UUIDColumnPredicate,
                FilterPredicate.IntInPredicate,
                FilterPredicate.LongInPredicate,
                FilterPredicate.FloatInPredicate,
                FilterPredicate.DoubleInPredicate,
                FilterPredicate.BinaryInPredicate,
                FilterPredicate.StringInPredicate,
                FilterPredicate.DateInPredicate,
                FilterPredicate.InstantInPredicate,
                FilterPredicate.LocalDateTimeInPredicate,
                FilterPredicate.TimeInPredicate,
                FilterPredicate.DecimalInPredicate,
                FilterPredicate.UUIDInPredicate,
                FilterPredicate.IntervalInPredicate,
                FilterPredicate.DateColumnPredicate,
                FilterPredicate.InstantColumnPredicate,
                FilterPredicate.LocalDateTimeColumnPredicate,
                FilterPredicate.TimeColumnPredicate,
                FilterPredicate.DecimalColumnPredicate,
                FilterPredicate.IntervalColumnPredicate,
                FilterPredicate.IsNullPredicate,
                FilterPredicate.IsNotNullPredicate,
                FilterPredicate.And,
                FilterPredicate.Or,
                FilterPredicate.Not,
                FilterPredicate.IntersectsPredicate {

    // ==================== Operators ====================

    enum Operator {
        EQ, NOT_EQ, LT, LT_EQ, GT, GT_EQ;

        /// Returns the logical inverse of this operator, which `not` applies to each leaf below it.
        /// For example, `not(gt(x, 5))` becomes `ltEq(x, 5)`.
        public Operator invert() {
            return switch (this) {
                case EQ -> NOT_EQ;
                case NOT_EQ -> EQ;
                case LT -> GT_EQ;
                case LT_EQ -> GT;
                case GT -> LT_EQ;
                case GT_EQ -> LT;
            };
        }
    }

    // ==================== INT32 Predicates ====================

    static FilterPredicate eq(String column, int value) {
        return new IntColumnPredicate(column, Operator.EQ, value);
    }

    static FilterPredicate notEq(String column, int value) {
        return new IntColumnPredicate(column, Operator.NOT_EQ, value);
    }

    static FilterPredicate lt(String column, int value) {
        return new IntColumnPredicate(column, Operator.LT, value);
    }

    static FilterPredicate ltEq(String column, int value) {
        return new IntColumnPredicate(column, Operator.LT_EQ, value);
    }

    static FilterPredicate gt(String column, int value) {
        return new IntColumnPredicate(column, Operator.GT, value);
    }

    static FilterPredicate gtEq(String column, int value) {
        return new IntColumnPredicate(column, Operator.GT_EQ, value);
    }

    // ==================== INT64 Predicates ====================

    static FilterPredicate eq(String column, long value) {
        return new LongColumnPredicate(column, Operator.EQ, value);
    }

    static FilterPredicate notEq(String column, long value) {
        return new LongColumnPredicate(column, Operator.NOT_EQ, value);
    }

    static FilterPredicate lt(String column, long value) {
        return new LongColumnPredicate(column, Operator.LT, value);
    }

    static FilterPredicate ltEq(String column, long value) {
        return new LongColumnPredicate(column, Operator.LT_EQ, value);
    }

    static FilterPredicate gt(String column, long value) {
        return new LongColumnPredicate(column, Operator.GT, value);
    }

    static FilterPredicate gtEq(String column, long value) {
        return new LongColumnPredicate(column, Operator.GT_EQ, value);
    }

    // ==================== FLOAT Predicates ====================

    static FilterPredicate eq(String column, float value) {
        return new FloatColumnPredicate(column, Operator.EQ, value);
    }

    static FilterPredicate notEq(String column, float value) {
        return new FloatColumnPredicate(column, Operator.NOT_EQ, value);
    }

    static FilterPredicate lt(String column, float value) {
        return new FloatColumnPredicate(column, Operator.LT, value);
    }

    static FilterPredicate ltEq(String column, float value) {
        return new FloatColumnPredicate(column, Operator.LT_EQ, value);
    }

    static FilterPredicate gt(String column, float value) {
        return new FloatColumnPredicate(column, Operator.GT, value);
    }

    static FilterPredicate gtEq(String column, float value) {
        return new FloatColumnPredicate(column, Operator.GT_EQ, value);
    }

    /// Creates a set-membership predicate for a `FLOAT` or `FLOAT16` column, matching a row whose
    /// value is any of `values`. Each probe is an equality literal, so on a `FLOAT16` column a
    /// probe no IEEE half represents throws `IllegalArgumentException` at reader creation.
    static FilterPredicate in(String column, float... values) {
        requireValues(Objects.requireNonNull(values, "values").length);
        return new FloatInPredicate(column, values);
    }

    // ==================== DOUBLE Predicates ====================

    static FilterPredicate eq(String column, double value) {
        return new DoubleColumnPredicate(column, Operator.EQ, value);
    }

    static FilterPredicate notEq(String column, double value) {
        return new DoubleColumnPredicate(column, Operator.NOT_EQ, value);
    }

    static FilterPredicate lt(String column, double value) {
        return new DoubleColumnPredicate(column, Operator.LT, value);
    }

    static FilterPredicate ltEq(String column, double value) {
        return new DoubleColumnPredicate(column, Operator.LT_EQ, value);
    }

    static FilterPredicate gt(String column, double value) {
        return new DoubleColumnPredicate(column, Operator.GT, value);
    }

    static FilterPredicate gtEq(String column, double value) {
        return new DoubleColumnPredicate(column, Operator.GT_EQ, value);
    }

    // ==================== BOOLEAN Predicates ====================

    static FilterPredicate eq(String column, boolean value) {
        return new BooleanColumnPredicate(column, Operator.EQ, value);
    }

    static FilterPredicate notEq(String column, boolean value) {
        return new BooleanColumnPredicate(column, Operator.NOT_EQ, value);
    }

    /// Creates a less-than predicate for a `BOOLEAN` column, which orders `false` before `true`:
    /// `lt(column, true)` matches the rows holding `false`, and `lt(column, false)` matches none.
    static FilterPredicate lt(String column, boolean value) {
        return new BooleanColumnPredicate(column, Operator.LT, value);
    }

    /// Creates a less-than-or-equal predicate for a `BOOLEAN` column. See
    /// [#lt(String,boolean)].
    static FilterPredicate ltEq(String column, boolean value) {
        return new BooleanColumnPredicate(column, Operator.LT_EQ, value);
    }

    /// Creates a greater-than predicate for a `BOOLEAN` column. See [#lt(String,boolean)].
    static FilterPredicate gt(String column, boolean value) {
        return new BooleanColumnPredicate(column, Operator.GT, value);
    }

    /// Creates a greater-than-or-equal predicate for a `BOOLEAN` column. See
    /// [#lt(String,boolean)].
    static FilterPredicate gtEq(String column, boolean value) {
        return new BooleanColumnPredicate(column, Operator.GT_EQ, value);
    }

    // ==================== String Predicates ====================

    /// Creates an equals predicate for a text column, one of `STRING`, `ENUM`, `JSON` and an
    /// unannotated `BYTE_ARRAY`. The value compares as its UTF-8 bytes, which is what such a
    /// column stores. A row storing bytes that are not well-formed UTF-8 matches no `String`,
    /// including the one `getString` returns for it; filter it with its `byte[]`. Any other
    /// column rejects a `String` with `IllegalArgumentException` at reader creation.
    static FilterPredicate eq(String column, String value) {
        return new StringColumnPredicate(column, Operator.EQ, value);
    }

    /// Creates a not-equals predicate for a text column. See [#eq(String,String)].
    static FilterPredicate notEq(String column, String value) {
        return new StringColumnPredicate(column, Operator.NOT_EQ, value);
    }

    /// Creates a less-than predicate for a text column. See [#eq(String,String)].
    static FilterPredicate lt(String column, String value) {
        return new StringColumnPredicate(column, Operator.LT, value);
    }

    /// Creates a less-than-or-equal predicate for a text column. See [#eq(String,String)].
    static FilterPredicate ltEq(String column, String value) {
        return new StringColumnPredicate(column, Operator.LT_EQ, value);
    }

    /// Creates a greater-than predicate for a text column. See [#eq(String,String)].
    static FilterPredicate gt(String column, String value) {
        return new StringColumnPredicate(column, Operator.GT, value);
    }

    /// Creates a greater-than-or-equal predicate for a text column. See [#eq(String,String)].
    static FilterPredicate gtEq(String column, String value) {
        return new StringColumnPredicate(column, Operator.GT_EQ, value);
    }

    // ==================== Binary (BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96) Predicates ====================

    /// Creates an equals predicate for a binary column, whose literal is the stored bytes —
    /// the value [RowReader#getBinary] returns for it. A row matches when it stores exactly these
    /// bytes.
    ///
    /// Ordered predicates compare the bytes unsigned lexicographically. A `DECIMAL`, a `FLOAT16`,
    /// a legacy `INT96` timestamp and a `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP` column order by the
    /// value their bytes encode, and take a `byte[]` for `eq`, `notEq` and `in` only; an ordered
    /// predicate on one takes a [BigDecimal], a `float`, an [Instant] or a [LocalDateTime]. A
    /// fixed-width column takes an equality literal of its own width only, a `FLOAT16` two bytes and
    /// an `INT96` twelve.
    ///
    /// The array is copied, so a caller reusing it does not change the predicate.
    static FilterPredicate eq(String column, byte[] value) {
        return new BinaryColumnPredicate(column, Operator.EQ, value);
    }

    /// Creates a not-equals predicate for a binary column. See [#eq(String,byte[])].
    static FilterPredicate notEq(String column, byte[] value) {
        return new BinaryColumnPredicate(column, Operator.NOT_EQ, value);
    }

    /// Creates a less-than predicate for a binary column. See [#eq(String,byte[])].
    static FilterPredicate lt(String column, byte[] value) {
        return new BinaryColumnPredicate(column, Operator.LT, value);
    }

    /// Creates a less-than-or-equal predicate for a binary column. See [#eq(String,byte[])].
    static FilterPredicate ltEq(String column, byte[] value) {
        return new BinaryColumnPredicate(column, Operator.LT_EQ, value);
    }

    /// Creates a greater-than predicate for a binary column. See [#eq(String,byte[])].
    static FilterPredicate gt(String column, byte[] value) {
        return new BinaryColumnPredicate(column, Operator.GT, value);
    }

    /// Creates a greater-than-or-equal predicate for a binary column. See [#eq(String,byte[])].
    static FilterPredicate gtEq(String column, byte[] value) {
        return new BinaryColumnPredicate(column, Operator.GT_EQ, value);
    }

    /// Creates a set-membership predicate for a binary column, matching a row whose value is any
    /// of `values`. Each probe is an equality literal and compares as [#eq(String,byte[])]
    /// describes. The arrays are copied.
    static FilterPredicate in(String column, byte[]... values) {
        requireValues(Objects.requireNonNull(values, "values").length);
        return new BinaryInPredicate(column, values);
    }

    static FilterPredicate in(String column, int... values) {
        requireValues(Objects.requireNonNull(values, "values").length);
        return new IntInPredicate(column, values);
    }

    static FilterPredicate in(String column, long... values) {
        requireValues(Objects.requireNonNull(values, "values").length);
        return new LongInPredicate(column, values);
    }

    /// Creates a set-membership predicate for a `DOUBLE` column, matching a row whose value is any
    /// of `values`.
    static FilterPredicate in(String column, double... values) {
        requireValues(Objects.requireNonNull(values, "values").length);
        return new DoubleInPredicate(column, values);
    }

    /// Creates a set-membership predicate for a text column, matching a row whose value is any of
    /// `values`. Each probe compares as [#eq(String,String)] describes.
    ///
    /// @deprecated use [#in(String,String...)], which builds the same predicate
    @Deprecated(since = "1.1.0")
    static FilterPredicate inStrings(String column, String... values) {
        return in(column, values);
    }

    /// Creates a set-membership predicate for a text column, matching a row whose value is any of
    /// `values`. Each probe compares as [#eq(String,String)] describes.
    static FilterPredicate in(String column, String... values) {
        requireValues(Objects.requireNonNull(values, "values").length);
        return new StringInPredicate(column, values);
    }

    // ==================== LocalDate (DATE) Predicates ====================

    /// Creates an equals predicate for a [LocalDate] column (Parquet DATE logical type).
    /// The column must carry the `DATE` logical type; the date is converted to days since the
    /// Unix epoch at reader creation.
    static FilterPredicate eq(String column, LocalDate value) {
        return new DateColumnPredicate(column, Operator.EQ, value);
    }

    /// Creates a not-equals predicate for a [LocalDate] column.
    static FilterPredicate notEq(String column, LocalDate value) {
        return new DateColumnPredicate(column, Operator.NOT_EQ, value);
    }

    /// Creates a less-than predicate for a [LocalDate] column.
    static FilterPredicate lt(String column, LocalDate value) {
        return new DateColumnPredicate(column, Operator.LT, value);
    }

    /// Creates a less-than-or-equal predicate for a [LocalDate] column.
    static FilterPredicate ltEq(String column, LocalDate value) {
        return new DateColumnPredicate(column, Operator.LT_EQ, value);
    }

    /// Creates a greater-than predicate for a [LocalDate] column.
    static FilterPredicate gt(String column, LocalDate value) {
        return new DateColumnPredicate(column, Operator.GT, value);
    }

    /// Creates a greater-than-or-equal predicate for a [LocalDate] column.
    static FilterPredicate gtEq(String column, LocalDate value) {
        return new DateColumnPredicate(column, Operator.GT_EQ, value);
    }

    /// Creates a set-membership predicate for a [LocalDate] column, matching a row whose value is
    /// any of `values`. Each probe is an equality literal, as for [#eq(String,LocalDate)].
    static FilterPredicate in(String column, LocalDate... values) {
        requireValues(Objects.requireNonNull(values, "values").length);
        return new DateInPredicate(column, Arrays.asList(values));
    }

    // ==================== Instant (TIMESTAMP) Predicates ====================

    /// Creates an equals predicate for an [Instant] column (Parquet TIMESTAMP logical type with
    /// `isAdjustedToUTC = true`, or a legacy `INT96` timestamp). The column's time unit is
    /// determined from the schema at reader creation.
    static FilterPredicate eq(String column, Instant value) {
        return new InstantColumnPredicate(column, Operator.EQ, value);
    }

    /// Creates a not-equals predicate for an [Instant] column.
    static FilterPredicate notEq(String column, Instant value) {
        return new InstantColumnPredicate(column, Operator.NOT_EQ, value);
    }

    /// Creates a less-than predicate for an [Instant] column.
    static FilterPredicate lt(String column, Instant value) {
        return new InstantColumnPredicate(column, Operator.LT, value);
    }

    /// Creates a less-than-or-equal predicate for an [Instant] column.
    static FilterPredicate ltEq(String column, Instant value) {
        return new InstantColumnPredicate(column, Operator.LT_EQ, value);
    }

    /// Creates a greater-than predicate for an [Instant] column.
    static FilterPredicate gt(String column, Instant value) {
        return new InstantColumnPredicate(column, Operator.GT, value);
    }

    /// Creates a greater-than-or-equal predicate for an [Instant] column.
    static FilterPredicate gtEq(String column, Instant value) {
        return new InstantColumnPredicate(column, Operator.GT_EQ, value);
    }

    /// Creates a set-membership predicate for an [Instant] column, matching a row whose value is
    /// any of `values`. Each probe is an equality literal, as for [#eq(String,Instant)].
    static FilterPredicate in(String column, Instant... values) {
        requireValues(Objects.requireNonNull(values, "values").length);
        return new InstantInPredicate(column, Arrays.asList(values));
    }

    // ==================== LocalDateTime (local TIMESTAMP) Predicates ====================

    /// Creates an equals predicate for a [LocalDateTime] column (Parquet TIMESTAMP logical type
    /// with `isAdjustedToUTC = false`). The column's time unit is determined from the schema at
    /// reader creation.
    static FilterPredicate eq(String column, LocalDateTime value) {
        return new LocalDateTimeColumnPredicate(column, Operator.EQ, value);
    }

    /// Creates a not-equals predicate for a [LocalDateTime] column.
    static FilterPredicate notEq(String column, LocalDateTime value) {
        return new LocalDateTimeColumnPredicate(column, Operator.NOT_EQ, value);
    }

    /// Creates a less-than predicate for a [LocalDateTime] column.
    static FilterPredicate lt(String column, LocalDateTime value) {
        return new LocalDateTimeColumnPredicate(column, Operator.LT, value);
    }

    /// Creates a less-than-or-equal predicate for a [LocalDateTime] column.
    static FilterPredicate ltEq(String column, LocalDateTime value) {
        return new LocalDateTimeColumnPredicate(column, Operator.LT_EQ, value);
    }

    /// Creates a greater-than predicate for a [LocalDateTime] column.
    static FilterPredicate gt(String column, LocalDateTime value) {
        return new LocalDateTimeColumnPredicate(column, Operator.GT, value);
    }

    /// Creates a greater-than-or-equal predicate for a [LocalDateTime] column.
    static FilterPredicate gtEq(String column, LocalDateTime value) {
        return new LocalDateTimeColumnPredicate(column, Operator.GT_EQ, value);
    }

    /// Creates a set-membership predicate for a [LocalDateTime] column, matching a row whose value
    /// is any of `values`. Each probe is an equality literal, as for [#eq(String,LocalDateTime)].
    static FilterPredicate in(String column, LocalDateTime... values) {
        requireValues(Objects.requireNonNull(values, "values").length);
        return new LocalDateTimeInPredicate(column, Arrays.asList(values));
    }

    // ==================== LocalTime (TIME) Predicates ====================

    /// Creates an equals predicate for a [LocalTime] column (Parquet TIME logical type).
    /// The column's time unit is determined from the schema at reader creation.
    static FilterPredicate eq(String column, LocalTime value) {
        return new TimeColumnPredicate(column, Operator.EQ, value);
    }

    /// Creates a not-equals predicate for a [LocalTime] column.
    static FilterPredicate notEq(String column, LocalTime value) {
        return new TimeColumnPredicate(column, Operator.NOT_EQ, value);
    }

    /// Creates a less-than predicate for a [LocalTime] column.
    static FilterPredicate lt(String column, LocalTime value) {
        return new TimeColumnPredicate(column, Operator.LT, value);
    }

    /// Creates a less-than-or-equal predicate for a [LocalTime] column.
    static FilterPredicate ltEq(String column, LocalTime value) {
        return new TimeColumnPredicate(column, Operator.LT_EQ, value);
    }

    /// Creates a greater-than predicate for a [LocalTime] column.
    static FilterPredicate gt(String column, LocalTime value) {
        return new TimeColumnPredicate(column, Operator.GT, value);
    }

    /// Creates a greater-than-or-equal predicate for a [LocalTime] column.
    static FilterPredicate gtEq(String column, LocalTime value) {
        return new TimeColumnPredicate(column, Operator.GT_EQ, value);
    }

    /// Creates a set-membership predicate for a [LocalTime] column, matching a row whose value is
    /// any of `values`. Each probe is an equality literal, as for [#eq(String,LocalTime)].
    static FilterPredicate in(String column, LocalTime... values) {
        requireValues(Objects.requireNonNull(values, "values").length);
        return new TimeInPredicate(column, Arrays.asList(values));
    }

    // ==================== BigDecimal (DECIMAL) Predicates ====================

    /// Creates an equals predicate for a [BigDecimal] column (Parquet DECIMAL logical type).
    /// The column's scale and physical type are read from the schema at reader creation, and the
    /// value is rescaled to the column's scale, padded where the column holds more. An equality
    /// literal carrying a digit the column's scale cannot hold, or a value past the range of the
    /// column's physical type, throws `IllegalArgumentException` at reader creation; an ordered
    /// predicate on such a literal is answered exactly.
    static FilterPredicate eq(String column, BigDecimal value) {
        return new DecimalColumnPredicate(column, Operator.EQ, value);
    }

    /// Creates a not-equals predicate for a [BigDecimal] column.
    static FilterPredicate notEq(String column, BigDecimal value) {
        return new DecimalColumnPredicate(column, Operator.NOT_EQ, value);
    }

    /// Creates a less-than predicate for a [BigDecimal] column.
    static FilterPredicate lt(String column, BigDecimal value) {
        return new DecimalColumnPredicate(column, Operator.LT, value);
    }

    /// Creates a less-than-or-equal predicate for a [BigDecimal] column.
    static FilterPredicate ltEq(String column, BigDecimal value) {
        return new DecimalColumnPredicate(column, Operator.LT_EQ, value);
    }

    /// Creates a greater-than predicate for a [BigDecimal] column.
    static FilterPredicate gt(String column, BigDecimal value) {
        return new DecimalColumnPredicate(column, Operator.GT, value);
    }

    /// Creates a greater-than-or-equal predicate for a [BigDecimal] column.
    static FilterPredicate gtEq(String column, BigDecimal value) {
        return new DecimalColumnPredicate(column, Operator.GT_EQ, value);
    }

    /// Creates a set-membership predicate for a [BigDecimal] column, matching a row whose value is
    /// any of `values`. Each probe is an equality literal, as for [#eq(String,BigDecimal)].
    static FilterPredicate in(String column, BigDecimal... values) {
        requireValues(Objects.requireNonNull(values, "values").length);
        return new DecimalInPredicate(column, Arrays.asList(values));
    }

    // ==================== UUID Predicates ====================

    /// Creates an equals predicate for a [UUID] column (Parquet UUID logical type).
    /// The UUID is encoded as a 16-byte big-endian `FIXED_LEN_BYTE_ARRAY`.
    static FilterPredicate eq(String column, UUID value) {
        return new UUIDColumnPredicate(column, Operator.EQ, uuidToBytes(value));
    }

    /// Creates a not-equals predicate for a [UUID] column.
    static FilterPredicate notEq(String column, UUID value) {
        return new UUIDColumnPredicate(column, Operator.NOT_EQ, uuidToBytes(value));
    }

    /// Creates a less-than predicate for a [UUID] column.
    static FilterPredicate lt(String column, UUID value) {
        return new UUIDColumnPredicate(column, Operator.LT, uuidToBytes(value));
    }

    /// Creates a less-than-or-equal predicate for a [UUID] column.
    static FilterPredicate ltEq(String column, UUID value) {
        return new UUIDColumnPredicate(column, Operator.LT_EQ, uuidToBytes(value));
    }

    /// Creates a greater-than predicate for a [UUID] column.
    static FilterPredicate gt(String column, UUID value) {
        return new UUIDColumnPredicate(column, Operator.GT, uuidToBytes(value));
    }

    /// Creates a greater-than-or-equal predicate for a [UUID] column.
    static FilterPredicate gtEq(String column, UUID value) {
        return new UUIDColumnPredicate(column, Operator.GT_EQ, uuidToBytes(value));
    }

    /// Creates a set-membership predicate for a [UUID] column, matching a row whose value is any of
    /// `values`.
    static FilterPredicate in(String column, UUID... values) {
        requireValues(Objects.requireNonNull(values, "values").length);
        return new UUIDInPredicate(column, Arrays.asList(values));
    }

    // ==================== INTERVAL Predicates ====================

    /// Creates an equals predicate for an `INTERVAL` column, whose literal is the
    /// [PqInterval] [RowReader#getInterval] returns for it. The column must carry the `INTERVAL`
    /// annotation.
    ///
    /// The format defines no order over intervals — there is no fixed conversion between months,
    /// days and milliseconds — so `lt`, `ltEq`, `gt` and `gtEq` on an `INTERVAL` column throw
    /// `IllegalArgumentException` at reader creation. Each component is stored as an unsigned
    /// 32-bit value, so one outside `[0, 4294967295]` throws there as well.
    static FilterPredicate eq(String column, PqInterval value) {
        return new IntervalColumnPredicate(column, Operator.EQ, value);
    }

    /// Creates a not-equals predicate for an `INTERVAL` column. See
    /// [#eq(String,PqInterval)].
    static FilterPredicate notEq(String column, PqInterval value) {
        return new IntervalColumnPredicate(column, Operator.NOT_EQ, value);
    }

    /// Creates a set-membership predicate for an `INTERVAL` column, matching a row whose value is
    /// any of `values`. Each probe is an equality literal, as for [#eq(String,PqInterval)].
    static FilterPredicate in(String column, PqInterval... values) {
        requireValues(Objects.requireNonNull(values, "values").length);
        return new IntervalInPredicate(column, Arrays.asList(values));
    }

    // ==================== Conversion Helpers ====================

    /// Rejects a null column name, naming the argument as a null literal is named.
    private static String requireColumn(String column) {
        return Objects.requireNonNull(column, "column");
    }

    /// Rejects `and` / `or` over no children, and a null child, naming it by its position.
    private static List<FilterPredicate> requireFilters(List<FilterPredicate> filters, String combinator) {
        if (filters.isEmpty()) {
            throw new IllegalArgumentException(combinator + " predicate requires at least one child");
        }
        for (int i = 0; i < filters.size(); i++) {
            Objects.requireNonNull(filters.get(i), "filters[" + i + "]");
        }
        return List.copyOf(filters);
    }

    /// Rejects a `NaN` bound of an `intersects` box. It compares false with every bound a unit
    /// records, so the box would keep or drop units regardless of what they cover.
    private static void requireBound(String column, String bound, double value) {
        if (Double.isNaN(value)) {
            throw new IllegalArgumentException("Column '" + column
                    + "' is tested by intersects, whose bounds are numbers; " + bound + " is NaN");
        }
    }

    /// Rejects a set form with no probe to test against.
    private static void requireValues(int count) {
        if (count == 0) {
            throw new IllegalArgumentException("IN predicate requires at least one value");
        }
    }

    /// Rejects a null probe list or a null probe in it, naming the probe the way a set form over
    /// an array does.
    private static <T> List<T> requireProbes(List<T> values) {
        Objects.requireNonNull(values, "values");
        for (int i = 0; i < values.size(); i++) {
            Objects.requireNonNull(values.get(i), "values[" + i + "]");
        }
        return values;
    }

    /// Rejects a `String` literal that is not well-formed UTF-16, which is one holding an
    /// unpaired surrogate. Such a literal has no UTF-8 encoding: encoding it replaces the
    /// surrogate with `?`, and the predicate would silently be one on a different value.
    ///
    /// `argument` names the offending literal the way the message reads it back, so a set form
    /// says which probe is at fault.
    private static void requireWellFormed(String column, String value, String argument) {
        if (!StandardCharsets.UTF_8.newEncoder().canEncode(value)) {
            throw new IllegalArgumentException("Column '" + column
                    + "' compares a String literal as its UTF-8 bytes; " + argument + " is not"
                    + " well-formed UTF-16 and has no UTF-8 encoding");
        }
    }

    private static byte[] uuidToBytes(UUID value) {
        Objects.requireNonNull(value, "value");
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(value.getMostSignificantBits());
        buffer.putLong(value.getLeastSignificantBits());
        return buffer.array();
    }

    // ==================== NULL Predicates ====================

    /// Creates a predicate that matches rows where the given column is null.
    static FilterPredicate isNull(String column) {
        return new IsNullPredicate(column);
    }

    /// Creates a predicate that matches rows where the given column is not null.
    static FilterPredicate isNotNull(String column) {
        return new IsNotNullPredicate(column);
    }

    // ==================== GEOSPATIAL Predicates ====================

    /// Creates a predicate that matches column chunks whose bounding box intersects the given bounding box.
    ///
    /// A box with `xmin > xmax` wraps across the antimeridian, covering `x >= xmin` or
    /// `x <= xmax`. A `NaN` bound throws `IllegalArgumentException`.
    ///
    /// The rows a bounding box does not cover are not a bounding box, so the predicate has no
    /// inverse: `not` over it, or over any predicate holding one, throws
    /// `IllegalArgumentException` at reader creation.
    static FilterPredicate intersects(String column, double xmin, double ymin,
                                      double xmax, double ymax) {
        return new IntersectsPredicate(column, xmin, ymin, xmax, ymax);
    }

    // ==================== Logical Combinators ====================

    static FilterPredicate and(FilterPredicate left, FilterPredicate right) {
        return new And(Arrays.asList(left, right));
    }

    /// Creates a conjunction of `filters`. At least one is required; none throws
    /// `IllegalArgumentException`.
    static FilterPredicate and(FilterPredicate... filters) {
        return new And(Arrays.asList(Objects.requireNonNull(filters, "filters")));
    }

    static FilterPredicate or(FilterPredicate left, FilterPredicate right) {
        return new Or(Arrays.asList(left, right));
    }

    /// Creates a disjunction of `filters`. At least one is required; none throws
    /// `IllegalArgumentException`.
    static FilterPredicate or(FilterPredicate... filters) {
        return new Or(Arrays.asList(Objects.requireNonNull(filters, "filters")));
    }

    static FilterPredicate not(FilterPredicate filter) {
        return new Not(Objects.requireNonNull(filter, "filter"));
    }

    // ==================== Leaf Predicate Records ====================

    record IntColumnPredicate(String column, Operator op, int value) implements FilterPredicate {

        public IntColumnPredicate {
            requireColumn(column);
        }
    }

    record LongColumnPredicate(String column, Operator op, long value) implements FilterPredicate {

        public LongColumnPredicate {
            requireColumn(column);
        }
    }

    record FloatColumnPredicate(String column, Operator op, float value) implements FilterPredicate {

        public FloatColumnPredicate {
            requireColumn(column);
        }
    }

    record DoubleColumnPredicate(String column, Operator op, double value) implements FilterPredicate {

        public DoubleColumnPredicate {
            requireColumn(column);
        }
    }

    record BooleanColumnPredicate(String column, Operator op, boolean value) implements FilterPredicate {

        public BooleanColumnPredicate {
            requireColumn(column);
        }
    }

    /// Predicate for a binary column over its stored bytes. Built by the `byte[]` factories and by
    /// `parquet-java-compat`'s filter conversion.
    record BinaryColumnPredicate(String column, Operator op, byte[] value) implements FilterPredicate {

        public BinaryColumnPredicate(String column, Operator op, byte[] value) {
            this.column = requireColumn(column);
            this.op = op;
            this.value = Objects.requireNonNull(value, "value").clone();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BinaryColumnPredicate that)) return false;
            return column.equals(that.column) && op == that.op && Arrays.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            int result = column.hashCode();
            result = 31 * result + op.hashCode();
            result = 31 * result + Arrays.hashCode(value);
            return result;
        }
    }

    /// Predicate for a text column, comparing the literal's UTF-8 bytes. The value is held as
    /// given and encoded at reader creation, where the column is also checked to be one a
    /// `String` reads.
    record StringColumnPredicate(String column, Operator op, String value) implements FilterPredicate {

        public StringColumnPredicate {
            requireColumn(column);
            requireWellFormed(column, Objects.requireNonNull(value, "value"), "the literal");
        }
    }

    record UUIDColumnPredicate(String column, Operator op, byte[] value) implements FilterPredicate {

        public UUIDColumnPredicate {
            requireColumn(column);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof UUIDColumnPredicate that)) return false;
            return column.equals(that.column) && op == that.op && Arrays.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            int result = column.hashCode();
            result = 31 * result + op.hashCode();
            result = 31 * result + Arrays.hashCode(value);
            return result;
        }
    }

    record IntInPredicate(String column, int[] values) implements FilterPredicate {

        public IntInPredicate(String column, int[] values) {
            this.column = requireColumn(column);
            this.values = Objects.requireNonNull(values, "values").clone();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof IntInPredicate that)) return false;
            return column.equals(that.column) && Arrays.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return 31 * column.hashCode() + Arrays.hashCode(values);
        }
    }

    record LongInPredicate(String column, long[] values) implements FilterPredicate {

        public LongInPredicate(String column, long[] values) {
            this.column = requireColumn(column);
            this.values = Objects.requireNonNull(values, "values").clone();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof LongInPredicate that)) return false;
            return column.equals(that.column) && Arrays.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return 31 * column.hashCode() + Arrays.hashCode(values);
        }
    }

    record FloatInPredicate(String column, float[] values) implements FilterPredicate {

        public FloatInPredicate(String column, float[] values) {
            this.column = requireColumn(column);
            this.values = Objects.requireNonNull(values, "values").clone();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof FloatInPredicate that)) return false;
            return column.equals(that.column) && Arrays.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return 31 * column.hashCode() + Arrays.hashCode(values);
        }
    }

    record DoubleInPredicate(String column, double[] values) implements FilterPredicate {

        public DoubleInPredicate(String column, double[] values) {
            this.column = requireColumn(column);
            this.values = Objects.requireNonNull(values, "values").clone();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof DoubleInPredicate that)) return false;
            return column.equals(that.column) && Arrays.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return 31 * column.hashCode() + Arrays.hashCode(values);
        }
    }

    /// Set-membership predicate for a binary column. Each probe compares as a
    /// [BinaryColumnPredicate] literal does.
    record BinaryInPredicate(String column, byte[][] values) implements FilterPredicate {

        public BinaryInPredicate(String column, byte[][] values) {
            this.column = requireColumn(column);
            this.values = new byte[Objects.requireNonNull(values, "values").length][];
            for (int i = 0; i < values.length; i++) {
                this.values[i] = Objects.requireNonNull(values[i], "values[" + i + "]").clone();
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BinaryInPredicate that)) return false;
            return column.equals(that.column) && Arrays.deepEquals(values, that.values);
        }

        @Override
        public int hashCode() {
            return 31 * column.hashCode() + Arrays.deepHashCode(values);
        }
    }

    /// Set-membership predicate for a text column. Each probe compares as a
    /// [StringColumnPredicate] literal does.
    record StringInPredicate(String column, String[] values) implements FilterPredicate {

        public StringInPredicate(String column, String[] values) {
            this.column = requireColumn(column);
            this.values = Objects.requireNonNull(values, "values").clone();
            for (int i = 0; i < this.values.length; i++) {
                String argument = "values[" + i + "]";
                requireWellFormed(column, Objects.requireNonNull(this.values[i], argument), argument);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof StringInPredicate that)) return false;
            return column.equals(that.column) && Arrays.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return 31 * column.hashCode() + Arrays.hashCode(values);
        }
    }

    // ==================== Logical-Type Predicate Records ====================

    /// Predicate for DATE columns. The [LocalDate] value is converted to epoch days at reader creation.
    record DateColumnPredicate(String column, Operator op, LocalDate value) implements FilterPredicate {

        public DateColumnPredicate {
            requireColumn(column);
            Objects.requireNonNull(value, "value");
        }
    }

    /// Predicate for TIMESTAMP columns with `isAdjustedToUTC = true` and for legacy `INT96`
    /// timestamp columns. The [Instant] value is converted to the column's time unit (MILLIS,
    /// MICROS, or NANOS) at reader creation using the schema's `TimestampType`, stored as the
    /// column's `INT64` or `FIXED_LEN_BYTE_ARRAY(12)` count, or to the twelve bytes of an `INT96`.
    record InstantColumnPredicate(String column, Operator op, Instant value) implements FilterPredicate {

        public InstantColumnPredicate {
            requireColumn(column);
            Objects.requireNonNull(value, "value");
        }
    }

    /// Predicate for TIMESTAMP columns with `isAdjustedToUTC = false`. The [LocalDateTime] value
    /// is converted to the column's time unit at reader creation, reading the wall clock as UTC.
    record LocalDateTimeColumnPredicate(String column, Operator op, LocalDateTime value)
            implements FilterPredicate {

        public LocalDateTimeColumnPredicate {
            requireColumn(column);
            Objects.requireNonNull(value, "value");
        }
    }

    /// Predicate for TIME columns. The [LocalTime] value is converted to the column's time unit
    /// at reader creation using the schema's `TimeType`.
    record TimeColumnPredicate(String column, Operator op, LocalTime value) implements FilterPredicate {

        public TimeColumnPredicate {
            requireColumn(column);
            Objects.requireNonNull(value, "value");
        }
    }

    /// Predicate for an `INTERVAL` column, comparing the twelve stored bytes as the three
    /// unsigned 32-bit components they encode.
    record IntervalColumnPredicate(String column, Operator op, PqInterval value)
            implements FilterPredicate {

        public IntervalColumnPredicate {
            requireColumn(column);
            Objects.requireNonNull(value, "value");
        }
    }

    /// Predicate for DECIMAL columns. The [BigDecimal] value is converted to the column's physical
    /// representation at reader creation using the schema's `DecimalType`.
    record DecimalColumnPredicate(String column, Operator op, BigDecimal value) implements FilterPredicate {

        public DecimalColumnPredicate {
            requireColumn(column);
            Objects.requireNonNull(value, "value");
        }
    }

    // ==================== Logical-Type Set-Membership Records ====================

    /// Set-membership predicate for DATE columns. Each probe resolves as a
    /// [DateColumnPredicate] equality literal does.
    record DateInPredicate(String column, List<LocalDate> values) implements FilterPredicate {

        public DateInPredicate {
            requireColumn(column);
            values = List.copyOf(requireProbes(values));
        }
    }

    /// Set-membership predicate for the columns an [InstantColumnPredicate] takes. Each probe
    /// resolves as its equality literal does.
    record InstantInPredicate(String column, List<Instant> values) implements FilterPredicate {

        public InstantInPredicate {
            requireColumn(column);
            values = List.copyOf(requireProbes(values));
        }
    }

    /// Set-membership predicate for TIMESTAMP columns with `isAdjustedToUTC = false`. Each probe
    /// resolves as a [LocalDateTimeColumnPredicate] equality literal does.
    record LocalDateTimeInPredicate(String column, List<LocalDateTime> values) implements FilterPredicate {

        public LocalDateTimeInPredicate {
            requireColumn(column);
            values = List.copyOf(requireProbes(values));
        }
    }

    /// Set-membership predicate for TIME columns. Each probe resolves as a
    /// [TimeColumnPredicate] equality literal does.
    record TimeInPredicate(String column, List<LocalTime> values) implements FilterPredicate {

        public TimeInPredicate {
            requireColumn(column);
            values = List.copyOf(requireProbes(values));
        }
    }

    /// Set-membership predicate for DECIMAL columns. Each probe resolves as a
    /// [DecimalColumnPredicate] equality literal does.
    record DecimalInPredicate(String column, List<BigDecimal> values) implements FilterPredicate {

        public DecimalInPredicate {
            requireColumn(column);
            values = List.copyOf(requireProbes(values));
        }
    }

    /// Set-membership predicate for UUID columns, each probe compared as its 16 big-endian bytes.
    record UUIDInPredicate(String column, List<UUID> values) implements FilterPredicate {

        public UUIDInPredicate {
            requireColumn(column);
            values = List.copyOf(requireProbes(values));
        }
    }

    /// Set-membership predicate for an `INTERVAL` column. Each probe resolves as an
    /// [IntervalColumnPredicate] literal does.
    record IntervalInPredicate(String column, List<PqInterval> values) implements FilterPredicate {

        public IntervalInPredicate {
            requireColumn(column);
            values = List.copyOf(requireProbes(values));
        }
    }

    // ==================== NULL Predicate Records ====================

    /// Predicate that matches rows where the column value is null.
    record IsNullPredicate(String column) implements FilterPredicate {

        public IsNullPredicate {
            requireColumn(column);
        }
    }

    /// Predicate that matches rows where the column value is not null.
    record IsNotNullPredicate(String column) implements FilterPredicate {

        public IsNotNullPredicate {
            requireColumn(column);
        }
    }

    // ==================== Logical Combinator Records ====================

    record And(List<FilterPredicate> filters) implements FilterPredicate {

        public And {
            filters = requireFilters(filters, "AND");
        }
    }

    record Or(List<FilterPredicate> filters) implements FilterPredicate {

        public Or {
            filters = requireFilters(filters, "OR");
        }
    }

    record Not(FilterPredicate delegate) implements FilterPredicate {

        public Not {
            Objects.requireNonNull(delegate, "filter");
        }
    }

    // ==================== Geospatial Predicate Records ====================

    ///  Predicate for spatial bounding box.
    record IntersectsPredicate(String column, double xmin, double ymin,
                               double xmax, double ymax) implements FilterPredicate {

        public IntersectsPredicate {
            requireColumn(column);
            requireBound(column, "xmin", xmin);
            requireBound(column, "ymin", ymin);
            requireBound(column, "xmax", xmax);
            requireBound(column, "ymax", ymax);
        }
    }
}
