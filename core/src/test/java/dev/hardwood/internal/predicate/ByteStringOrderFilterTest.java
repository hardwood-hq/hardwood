/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate.Comparison;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A binary literal compares the stored bytes, and either binary physical type reaches that path
/// whatever it is annotated, because a `BYTE_ARRAY` predicate is accepted on a
/// `FIXED_LEN_BYTE_ARRAY` column. Two annotations order by the value their bytes stand for
/// rather than by the bytes, so the literal takes the column's own comparison instead — which is
/// what parquet-java does through `BINARY_AS_SIGNED_INTEGER` and `BINARY_AS_FLOAT16`, and what
/// the compatibility shim's callers rely on.
///
/// A `FLOAT16` is two little-endian bytes, so a byte-string reading sorts the low mantissa byte
/// first: a chunk running `1.0` (`00 3C`) to `1.5` (`00 3E`) records exactly those bounds, and
/// `1.0009765625` (`01 3C`) sits inside them numerically while comparing above the maximum
/// byte-wise.
class ByteStringOrderFilterTest {

    @Test
    void aFloat16ColumnComparesABinaryLiteralNumerically() {
        assertThat(FilterPredicateResolver.resolve(
                FilterPredicate.lt("h", half(1.5f)), float16Schema()))
                .isInstanceOfSatisfying(ResolvedPredicate.Float16Predicate.class,
                        p -> assertThat(p.value()).isEqualTo(1.5f));
    }

    /// The row the byte-string reading dropped: inside the bounds numerically, above the maximum
    /// byte-wise.
    @Test
    void aFloat16ColumnFindsARowAByteStringReadingDropped() throws Exception {
        byte[] oneAndABit = { 0x01, 0x3C };
        byte[] file = writeFloat16(new byte[][] { half(1.0f), oneAndABit, half(1.5f) });

        assertThat(filteredFloat16(file, FilterPredicate.eq("h", oneAndABit)))
                .containsExactly(oneAndABit);
    }

    @Test
    void aFloat16LiteralOfTheWrongWidthIsRejected() {
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("h", new byte[] { 0x01, 0x02, 0x03 }), float16Schema()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'h' is a FLOAT16, whose literal is 2 bytes, not 3");
    }

    /// A `BYTE_ARRAY` `DECIMAL` compares the value its bytes stand for, so a padded encoding of
    /// the same number is found — the comparison parquet-java applies, and the one a
    /// `BigDecimal` literal already resolves to here.
    @Test
    void aByteArrayDecimalComparesABinaryLiteralAsItsValue() {
        assertThat(FilterPredicateResolver.resolve(
                FilterPredicate.gt("amount", new byte[] { 0x7F }), decimalSchema()))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryPredicate.class,
                        p -> assertThat(p.comparison()).isEqualTo(Comparison.VARIABLE_DECIMAL));
    }

    @Test
    void aFixedDecimalComparesABinaryLiteralAsItsValue() {
        FileSchema fixed = FileSchema.builder("schema")
                .addColumn("amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 4,
                        LogicalType.decimal(9, 2))
                .build();

        assertThat(FilterPredicateResolver.resolve(
                FilterPredicate.gt("amount", new byte[] { 0x7F }), fixed))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryPredicate.class,
                        p -> assertThat(p.comparison()).isEqualTo(Comparison.FIXED_DECIMAL));
    }

    /// Ordering a decimal by its value rather than its bytes, end to end: `3.00` is `01 2C`,
    /// which sorts below the single byte `7F` byte-wise and above it as a number.
    @Test
    void aByteArrayDecimalOrdersABinaryLiteralByValue() throws Exception {
        byte[] file = writeDecimals(new BigDecimal[] {
                new BigDecimal("1.27"), new BigDecimal("3.00"), new BigDecimal("-1.00") });

        assertThat(filteredDecimals(file, FilterPredicate.gt("amount", new byte[] { 0x7F })))
                .containsExactly(new BigDecimal("3.00"));
    }

    /// Membership takes the column's comparison, as a scalar literal does. The encoding-dependent
    /// shortcuts are what change: a `BYTE_ARRAY` `DECIMAL` may hold one number under more than one
    /// byte string, so its Bloom filter and dictionary — which test exact bytes — cannot rule a
    /// probe out, while a fixed-width one has exactly one encoding per value and keeps them.
    @Test
    void aDecimalSetTakesTheColumnsComparison() {
        assertThat(FilterPredicateResolver.resolve(
                FilterPredicate.in("amount", new byte[] { 0x61 }), decimalSchema()))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryInPredicate.class, p -> {
                    assertThat(p.comparison()).isEqualTo(Comparison.VARIABLE_DECIMAL);
                    assertThat(p.byteExact()).isFalse();
                });

        FileSchema fixed = FileSchema.builder("schema")
                .addColumn("amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 4,
                        LogicalType.decimal(9, 2))
                .build();
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("amount", new byte[] { 0x61 }), fixed))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryInPredicate.class, p -> {
                    assertThat(p.comparison()).isEqualTo(Comparison.FIXED_DECIMAL);
                    assertThat(p.byteExact()).isTrue();
                });
    }

    /// The statistics half, in the column's order: `7F` is `127`, inside bounds of `-100`
    /// (`9C`) to `300` (`01 2C`) as numbers, but below the minimum byte-wise — the reading that
    /// would drop a row group holding it.
    @Test
    void aDecimalSetPrunesInTheColumnsOrder() {
        byte[][] probe = { { 0x7F } };
        byte[] min = { (byte) 0x9C };
        byte[] max = { 0x01, 0x2C };

        assertThat(StatisticsFilterSupport.canDropBinaryIn(probe, min, max, Comparison.VARIABLE_DECIMAL)).isFalse();
        assertThat(StatisticsFilterSupport.canDropBinaryIn(probe, min, max, Comparison.BYTE_STRING))
                .as("byte-wise the probe sorts below the minimum, which must not be acted on")
                .isTrue();
    }

    /// Membership on a `FLOAT16` decodes each two-byte probe to the half it encodes and compares it
    /// the way `eq` does, so the row a byte-string reading dropped is a member, and `not(in)` is
    /// its exact complement.
    @Test
    void aFloat16SetComparesItsProbesNumerically() throws Exception {
        byte[] oneAndABit = { 0x01, 0x3C };
        byte[] file = writeFloat16(new byte[][] { half(1.0f), oneAndABit, half(1.5f) });

        assertThat(filteredFloat16(file, FilterPredicate.in("h", oneAndABit)))
                .containsExactly(oneAndABit);
        assertThat(filteredFloat16(file, FilterPredicate.not(
                FilterPredicate.in("h", oneAndABit))))
                .containsExactly(half(1.0f), half(1.5f));
    }

    /// `in(double...)` compares against the decoded half as it does against a `FLOAT`'s widened
    /// value: `0.1` is no half, so it is a member of nothing, and `not(in)` keeps every row.
    @Test
    void aFloat16DoubleSetMatchesOnlyExactHalves() throws Exception {
        byte[] oneAndABit = { 0x01, 0x3C };
        byte[] file = writeFloat16(new byte[][] { half(1.0f), oneAndABit, half(1.5f) });

        assertThat(filteredFloat16(file, FilterPredicate.in("h", 1.0009765625, 0.1)))
                .containsExactly(oneAndABit);
        assertThat(filteredFloat16(file, FilterPredicate.in("h", 0.1))).isEmpty();
        assertThat(filteredFloat16(file, FilterPredicate.not(FilterPredicate.in("h", 0.1))))
                .containsExactly(half(1.0f), oneAndABit, half(1.5f));
    }

    @Test
    void aFloat16SetProbeOfTheWrongWidthIsRejected() {
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.in("h", new byte[] { 0x01, 0x02, 0x03 }), float16Schema()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'h' is a FLOAT16, whose literal is 2 bytes, not 3");
    }

    /// The bounds read as halves: `1.0009765625` lies inside a chunk running `1.0` to `1.5`, where
    /// a byte-string reading of `01 3C` against `00 3C` .. `00 3E` places it above the maximum.
    @Test
    void aFloat16SetPrunesInTheColumnsOrder() {
        ResolvedPredicate leaf = FilterPredicateResolver.resolve(
                FilterPredicate.in("h", new byte[] { 0x01, 0x3C }), float16Schema());
        Statistics stats = new Statistics(half(1.0f), half(1.5f), 0L, null, false);

        assertThat(MinMaxStats.of(stats, leaf, BoundsReadability.ALL).canDrop(leaf)).isFalse();
    }

    /// Every annotation whose order is the bytes themselves keeps the byte-string comparison, as
    /// does a binary column carrying none.
    @Test
    void anAnnotationOrderedAsItsBytesKeepsTheByteStringComparison() {
        assertByteString(binary(PhysicalType.BYTE_ARRAY, null));
        assertByteString(fixed(16, null));
        assertByteString(binary(PhysicalType.BYTE_ARRAY, LogicalType.string()));
        assertByteString(binary(PhysicalType.BYTE_ARRAY, LogicalType.enumType()));
        assertByteString(binary(PhysicalType.BYTE_ARRAY, LogicalType.json()));
        assertByteString(binary(PhysicalType.BYTE_ARRAY, LogicalType.bson()));
        assertByteString(fixed(16, LogicalType.uuid()));
        assertByteString(fixed(12, new LogicalType.IntervalType()));
        assertByteString(binary(PhysicalType.BYTE_ARRAY, new LogicalType.GeometryType(null)));
        assertByteString(binary(PhysicalType.BYTE_ARRAY, new LogicalType.GeographyType(null, null)));
    }

    // ==================== Fixtures ====================

    /// The literal takes the column's width where it has one, since a fixed-width column holds
    /// only byte strings of that width and refuses an equality literal of any other.
    private static void assertByteString(FileSchema schema) {
        Integer width = schema.getColumn("c").typeLength();
        byte[] literal = new byte[width == null ? 1 : width];
        Arrays.fill(literal, (byte) 0x61);

        assertThat(FilterPredicateResolver.resolve(FilterPredicate.eq("c", literal), schema))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryPredicate.class,
                        p -> assertThat(p.comparison()).isEqualTo(Comparison.BYTE_STRING));
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("c", literal), schema))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryInPredicate.class,
                        p -> assertThat(p.comparison()).isEqualTo(Comparison.BYTE_STRING));
    }

    private static byte[] half(float value) {
        short bits = Float.floatToFloat16(value);
        return new byte[] { (byte) bits, (byte) (bits >>> 8) };
    }

    private static FileSchema float16Schema() {
        return FileSchema.builder("schema")
                .addColumn("h", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 2,
                        LogicalType.float16())
                .build();
    }

    private static FileSchema decimalSchema() {
        return FileSchema.builder("schema")
                .addColumn("amount", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        LogicalType.decimal(18, 2))
                .build();
    }

    private static FileSchema binary(PhysicalType type, LogicalType logicalType) {
        return FileSchema.builder("schema")
                .addColumn("c", type, RepetitionType.REQUIRED, logicalType)
                .build();
    }

    private static FileSchema fixed(int length, LogicalType logicalType) {
        return FileSchema.builder("schema")
                .addColumn("c", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED,
                        length, logicalType)
                .build();
    }

    private static byte[] writeFloat16(byte[][] values) throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, float16Schema())) {
            writer.columnWriter().writeBatch(batch -> batch.fixed(0, values));
        }
        return out.toByteArray();
    }

    private static byte[] writeDecimals(BigDecimal[] values) throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, decimalSchema())) {
            for (BigDecimal value : values) {
                writer.rowWriter().writeRow(row -> row.setDecimal("amount", value));
            }
        }
        return out.toByteArray();
    }

    private static List<byte[]> filteredFloat16(byte[] file, FilterPredicate predicate)
            throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
                RowReader rows = reader.buildRowReader().filter(predicate).build()) {
            List<byte[]> matched = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                matched.add(rows.getBinary("h"));
            }
            return matched;
        }
    }

    private static List<BigDecimal> filteredDecimals(byte[] file, FilterPredicate predicate)
            throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
                RowReader rows = reader.buildRowReader().filter(predicate).build()) {
            List<BigDecimal> matched = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                matched.add(rows.getDecimal("amount"));
            }
            return matched;
        }
    }
}
