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
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/// A predicate for filtering row groups based on column statistics.
///
/// Filter predicates enable predicate push-down: row groups whose statistics
/// prove that no rows can match the predicate are skipped entirely, avoiding
/// unnecessary I/O and decoding.
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
/// try (ColumnReader reader = fileReader.createColumnReader("salary", filter)) {
///     while (reader.nextBatch()) { ... }
/// }
/// ```
///
/// ## Null handling
///
/// All comparison predicates (`eq`, `notEq`, `lt`, `ltEq`, `gt`, `gtEq`, `in`,
/// `inStrings`) follow SQL three-valued logic: comparing a null column value
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
public sealed interface FilterPredicate
        permits FilterPredicate.IntColumnPredicate,
                FilterPredicate.LongColumnPredicate,
                FilterPredicate.FloatColumnPredicate,
                FilterPredicate.DoubleColumnPredicate,
                FilterPredicate.BooleanColumnPredicate,
                FilterPredicate.BinaryColumnPredicate,
                FilterPredicate.SignedBinaryColumnPredicate,
                FilterPredicate.IntInPredicate,
                FilterPredicate.LongInPredicate,
                FilterPredicate.BinaryInPredicate,
                FilterPredicate.DateColumnPredicate,
                FilterPredicate.InstantColumnPredicate,
                FilterPredicate.TimeColumnPredicate,
                FilterPredicate.DecimalColumnPredicate,
                FilterPredicate.IsNullPredicate,
                FilterPredicate.IsNotNullPredicate,
                FilterPredicate.And,
                FilterPredicate.Or,
                FilterPredicate.Not {

    // ==================== Operators ====================

    enum Operator {
        EQ, NOT_EQ, LT, LT_EQ, GT, GT_EQ;

        /// Returns the logical inverse of this operator, used to push NOT through leaf predicates.
        /// For example, `NOT(GT(x, 5))` becomes `LT_EQ(x, 5)`.
        ///
        /// `NOT_EQ.invert()` returns `EQ`, which enables full pushdown (unlike the conservative
        /// fallback for arbitrary NOT predicates).
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

    // ==================== STRING (BYTE_ARRAY) Predicates ====================

    static FilterPredicate eq(String column, String value) {
        return new BinaryColumnPredicate(column, Operator.EQ, value.getBytes(StandardCharsets.UTF_8));
    }

    static FilterPredicate notEq(String column, String value) {
        return new BinaryColumnPredicate(column, Operator.NOT_EQ, value.getBytes(StandardCharsets.UTF_8));
    }

    static FilterPredicate lt(String column, String value) {
        return new BinaryColumnPredicate(column, Operator.LT, value.getBytes(StandardCharsets.UTF_8));
    }

    static FilterPredicate ltEq(String column, String value) {
        return new BinaryColumnPredicate(column, Operator.LT_EQ, value.getBytes(StandardCharsets.UTF_8));
    }

    static FilterPredicate gt(String column, String value) {
        return new BinaryColumnPredicate(column, Operator.GT, value.getBytes(StandardCharsets.UTF_8));
    }

    static FilterPredicate gtEq(String column, String value) {
        return new BinaryColumnPredicate(column, Operator.GT_EQ, value.getBytes(StandardCharsets.UTF_8));
    }

    static FilterPredicate in(String column, int... values) {
        if (values.length == 0) {
            throw new IllegalArgumentException("IN predicate requires at least one value");
        }
        return new IntInPredicate(column, values);
    }

    static FilterPredicate in(String column, long... values) {
        if (values.length == 0) {
            throw new IllegalArgumentException("IN predicate requires at least one value");
        }
        return new LongInPredicate(column, values);
    }

    static FilterPredicate inStrings(String column, String... values) {
        if (values.length == 0) {
            throw new IllegalArgumentException("IN predicate requires at least one value");
        }
        byte[][] encoded = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            encoded[i] = values[i].getBytes(StandardCharsets.UTF_8);
        }
        return new BinaryInPredicate(column, encoded);
    }

    // ==================== LocalDate (DATE) Predicates ====================

    /// Creates an equals predicate for a [LocalDate] column (Parquet DATE logical type).
    /// The date is converted to days since the Unix epoch at evaluation time.
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

    // ==================== Instant (TIMESTAMP) Predicates ====================

    /// Creates an equals predicate for an [Instant] column (Parquet TIMESTAMP logical type).
    /// The column's time unit is determined from the schema at evaluation time.
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

    // ==================== LocalTime (TIME) Predicates ====================

    /// Creates an equals predicate for a [LocalTime] column (Parquet TIME logical type).
    /// The column's time unit is determined from the schema at evaluation time.
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

    // ==================== BigDecimal (DECIMAL) Predicates ====================

    /// Creates an equals predicate for a [BigDecimal] column (Parquet DECIMAL logical type).
    /// The column's scale, precision, and physical type are determined from the schema at evaluation time.
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

    // ==================== UUID Predicates ====================

    /// Creates an equals predicate for a [UUID] column (Parquet UUID logical type).
    /// The UUID is encoded as a 16-byte big-endian `FIXED_LEN_BYTE_ARRAY`.
    static FilterPredicate eq(String column, UUID value) {
        return new BinaryColumnPredicate(column, Operator.EQ, uuidToBytes(value));
    }

    /// Creates a not-equals predicate for a [UUID] column.
    static FilterPredicate notEq(String column, UUID value) {
        return new BinaryColumnPredicate(column, Operator.NOT_EQ, uuidToBytes(value));
    }

    /// Creates a less-than predicate for a [UUID] column.
    static FilterPredicate lt(String column, UUID value) {
        return new BinaryColumnPredicate(column, Operator.LT, uuidToBytes(value));
    }

    /// Creates a less-than-or-equal predicate for a [UUID] column.
    static FilterPredicate ltEq(String column, UUID value) {
        return new BinaryColumnPredicate(column, Operator.LT_EQ, uuidToBytes(value));
    }

    /// Creates a greater-than predicate for a [UUID] column.
    static FilterPredicate gt(String column, UUID value) {
        return new BinaryColumnPredicate(column, Operator.GT, uuidToBytes(value));
    }

    /// Creates a greater-than-or-equal predicate for a [UUID] column.
    static FilterPredicate gtEq(String column, UUID value) {
        return new BinaryColumnPredicate(column, Operator.GT_EQ, uuidToBytes(value));
    }

    // ==================== Conversion Helpers ====================

    private static byte[] uuidToBytes(UUID value) {
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

    // ==================== Logical Combinators ====================

    static FilterPredicate and(FilterPredicate left, FilterPredicate right) {
        return new And(List.of(left, right));
    }

    static FilterPredicate and(FilterPredicate... filters) {
        return new And(List.of(filters));
    }

    static FilterPredicate or(FilterPredicate left, FilterPredicate right) {
        return new Or(List.of(left, right));
    }

    static FilterPredicate or(FilterPredicate... filters) {
        return new Or(List.of(filters));
    }

    static FilterPredicate not(FilterPredicate filter) {
        return new Not(filter);
    }

    // ==================== Leaf Predicate Records ====================

    record IntColumnPredicate(String column, Operator op, int value) implements FilterPredicate {
    }

    record LongColumnPredicate(String column, Operator op, long value) implements FilterPredicate {
    }

    record FloatColumnPredicate(String column, Operator op, float value) implements FilterPredicate {
    }

    record DoubleColumnPredicate(String column, Operator op, double value) implements FilterPredicate {
    }

    record BooleanColumnPredicate(String column, Operator op, boolean value) implements FilterPredicate {
    }

    record BinaryColumnPredicate(String column, Operator op, byte[] value) implements FilterPredicate {

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

    /// Predicate for `FIXED_LEN_BYTE_ARRAY` columns that require signed (two's complement)
    /// comparison, such as decimals. The value must be padded to the column's fixed length.
    record SignedBinaryColumnPredicate(String column, Operator op, byte[] value) implements FilterPredicate {

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SignedBinaryColumnPredicate that)) return false;
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
            this.column = column;
            this.values = values.clone();
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
            this.column = column;
            this.values = values.clone();
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

    record BinaryInPredicate(String column, byte[][] values) implements FilterPredicate {

        public BinaryInPredicate(String column, byte[][] values) {
            this.column = column;
            this.values = values.clone();
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

    // ==================== Logical-Type Predicate Records ====================

    /// Predicate for DATE columns. The [LocalDate] value is converted to epoch days at evaluation time.
    record DateColumnPredicate(String column, Operator op, LocalDate value) implements FilterPredicate {
    }

    /// Predicate for TIMESTAMP columns. The [Instant] value is converted to the column's time unit
    /// (MILLIS, MICROS, or NANOS) at evaluation time using the schema's `TimestampType`.
    record InstantColumnPredicate(String column, Operator op, Instant value) implements FilterPredicate {
    }

    /// Predicate for TIME columns. The [LocalTime] value is converted to the column's time unit
    /// at evaluation time using the schema's `TimeType`.
    record TimeColumnPredicate(String column, Operator op, LocalTime value) implements FilterPredicate {
    }

    /// Predicate for DECIMAL columns. The [BigDecimal] value is converted to the column's physical
    /// representation at evaluation time using the schema's `DecimalType`.
    record DecimalColumnPredicate(String column, Operator op, BigDecimal value) implements FilterPredicate {
    }

    // ==================== NULL Predicate Records ====================

    /// Predicate that matches rows where the column value is null.
    record IsNullPredicate(String column) implements FilterPredicate {
    }

    /// Predicate that matches rows where the column value is not null.
    record IsNotNullPredicate(String column) implements FilterPredicate {
    }

    // ==================== Logical Combinator Records ====================

    record And(List<FilterPredicate> filters) implements FilterPredicate {
    }

    record Or(List<FilterPredicate> filters) implements FilterPredicate {
    }

    record Not(FilterPredicate delegate) implements FilterPredicate {
    }
}
