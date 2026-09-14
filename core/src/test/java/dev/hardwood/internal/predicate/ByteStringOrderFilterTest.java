/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

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

/// A binary literal is the stored bytes, and either binary physical type reaches that path
/// whatever it is annotated, because a `BYTE_ARRAY` predicate is accepted on a
/// `FIXED_LEN_BYTE_ARRAY` column. Two annotations order by the value their bytes stand for
/// rather than by the bytes, and their bounds are written in that order, so byte equality prunes
/// as the value the bytes encode.
///
/// A `FLOAT16` is two little-endian bytes, so a byte-string reading sorts the low mantissa byte
/// first: a chunk running `1.0` (`00 3C`) to `1.5` (`00 3E`) records exactly those bounds, and
/// `1.0009765625` (`01 3C`) sits inside them numerically while comparing above the maximum
/// byte-wise.
class ByteStringOrderFilterTest {

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

    /// Membership on a `FLOAT16` prunes each two-byte probe as the half it encodes, as `eq` does,
    /// so the row a byte-string reading dropped is a member, and `not(in)` is its exact complement.
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

    /// `in(float...)` compares against the decoded half.
    @Test
    void aFloat16FloatSetComparesAgainstTheDecodedHalf() throws Exception {
        byte[] oneAndABit = { 0x01, 0x3C };
        byte[] file = writeFloat16(new byte[][] { half(1.0f), oneAndABit, half(1.5f) });

        assertThat(filteredFloat16(file, FilterPredicate.in("h", 1.0009765625f, 2.0f)))
                .containsExactly(oneAndABit);
        assertThat(filteredFloat16(file, FilterPredicate.not(FilterPredicate.in("h", 1.0009765625f))))
                .containsExactly(half(1.0f), half(1.5f));
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
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.in("h", new byte[] { 0x01, 0x3C }), float16Schema());
        Statistics stats = new Statistics(half(1.0f), half(1.5f), 0L, null, false);

        assertThat(resolved).isInstanceOfSatisfying(ResolvedPredicate.And.class, and -> assertThat(and.children())
                .noneMatch(leaf -> MinMaxStats.of(stats, leaf, BoundsReadability.ALL).canDrop(leaf)));
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
}
