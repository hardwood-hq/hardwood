/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.bloomfilter.BloomFilter;
import dev.hardwood.internal.bloomfilter.BloomFilterHeader;
import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate.Comparison;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;

/// A `DECIMAL` stored as `BYTE_ARRAY` holds each unscaled value in the fewest bytes that hold
/// it, so two values of the same column differ in length and length does not track magnitude:
/// `127` is `0x7F` and `128` is `0x00 0x80`. Predicates over such a column compare the
/// represented value, not the byte string.
class BinaryDecimalFilterTest {

    /// A position with nothing to point at: these cases assert decisions, not diagnostics.
    private static final LogContext UNNAMED =
            new LogContext(null, ExceptionContext.UNKNOWN_ROW_GROUP);


    private static final int PADDED_ROWS = 512;

    /// One split-block bloom filter block, the smallest a bitset can be.
    private static final int BLOOM_FILTER_BYTES = 32;

    /// Unscaled values at the boundaries where the encoding length changes, and across zero.
    private static final BigDecimal[] VALUES = {
            new BigDecimal("0.00"), // 0    -> {} / 0x00
            new BigDecimal("1.27"), // 127  -> 0x7F
            new BigDecimal("1.28"), // 128  -> 0x00 0x80  (longer, but greater)
            new BigDecimal("3.00"), // 300  -> 0x01 0x2C
            new BigDecimal("-1.00"), // -100 -> 0x9C
            new BigDecimal("-2.56") // -256 -> 0xFF 0x00  (longer, but smaller)
    };

    @Test
    void greaterThanOrdersByValueRatherThanByBytes() throws Exception {
        assertThat(filtered(FilterPredicate.gt("amount", new BigDecimal("1.27"))))
                .containsExactly(new BigDecimal("1.28"), new BigDecimal("3.00"));
    }

    @Test
    void lessThanSpansTheSignBoundary() throws Exception {
        assertThat(filtered(FilterPredicate.lt("amount", new BigDecimal("0.00"))))
                .containsExactly(new BigDecimal("-1.00"), new BigDecimal("-2.56"));
    }

    @Test
    void aLongerEncodingIsNotAutomaticallyGreater() throws Exception {
        assertThat(filtered(FilterPredicate.ltEq("amount", new BigDecimal("-1.00"))))
                .containsExactly(new BigDecimal("-1.00"), new BigDecimal("-2.56"));
    }

    @Test
    void equalityFindsTheValue() throws Exception {
        assertThat(filtered(FilterPredicate.eq("amount", new BigDecimal("1.28"))))
                .containsExactly(new BigDecimal("1.28"));
    }

    /// The format says a `BYTE_ARRAY` decimal *should* use the fewest bytes, not that it must, so
    /// a writer may pad. A padded value is the same number and must still be found.
    @Test
    void aPaddedEncodingIsStillTheSameValue() throws Exception {
        byte[] file = writeRawBinaryDecimals(paddedRows());
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
                RowReader rows = reader.buildRowReader()
                        .filter(FilterPredicate.eq("amount", new BigDecimal("1.27")))
                        .build()) {
            assertThat(collect(rows)).hasSize(PADDED_ROWS / 2)
                    .allMatch(value -> value.compareTo(new BigDecimal("1.27")) == 0);
        }
    }

    /// Equality on such a column must not be decided by testing the literal's own bytes:
    /// bloom filters hash them and dictionary membership compares them, and a padded value in
    /// the file would come back absent while the value is present. The predicate carries that
    /// as `byteExact = false`, and [RowGroupFilterEvaluator] skips both shortcuts for it —
    /// statistics, which compare in the column's order, carry the pruning instead.
    @Test
    void equalityIsNotMarkedByteExact() {
        FileSchema schema = schema();
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("amount", new BigDecimal("1.27")), schema);

        assertThat(resolved).isInstanceOfSatisfying(ResolvedPredicate.BinaryPredicate.class, p -> {
            assertThat(p.comparison()).isEqualTo(Comparison.VARIABLE_DECIMAL);
            assertThat(p.byteExact()).isFalse();
            // 127 in the fewest bytes that hold it, the form the format asks a writer for.
            assertThat(p.value()).containsExactly(0x7F);
        });
    }

    /// A `FIXED_LEN_BYTE_ARRAY` decimal pads every value to the column width, so the encoding of
    /// a given number is the only one the column can hold and the shortcuts stay available.
    @Test
    void aFixedWidthDecimalStaysByteExact() {
        FileSchema fixed = FileSchema.builder("schema")
                .addColumn("amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 8,
                        LogicalType.decimal(18, 2))
                .build();
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("amount", new BigDecimal("1.27")), fixed);

        assertThat(resolved).isInstanceOfSatisfying(ResolvedPredicate.BinaryPredicate.class,
                p -> assertThat(p.byteExact()).isTrue());
    }

    @Test
    void notEqualExcludesOnlyTheValue() throws Exception {
        assertThat(filtered(FilterPredicate.notEq("amount", new BigDecimal("1.28"))))
                .containsExactly(new BigDecimal("0.00"), new BigDecimal("1.27"),
                        new BigDecimal("3.00"), new BigDecimal("-1.00"), new BigDecimal("-2.56"));
    }

    /// A null is not a value, so it satisfies no comparison — including the negated one, which a
    /// column of the same values without nulls would otherwise answer identically.
    @Test
    void aNullMatchesNoValuePredicate() throws Exception {
        assertThat(filteredWithNulls(FilterPredicate.gtEq("amount", new BigDecimal("-2.56"))))
                .containsExactly(new BigDecimal("1.27"), new BigDecimal("3.00"));
        assertThat(filteredWithNulls(FilterPredicate.notEq("amount", new BigDecimal("1.27"))))
                .containsExactly(new BigDecimal("3.00"));
        assertThat(filteredWithNulls(FilterPredicate.isNull("amount")))
                .containsOnlyNulls().hasSize(2);
    }

    /// Ordering a padded encoding against the resolver's minimal literal: `127` written in four
    /// bytes must not out-rank `127` itself, and the two-byte `300` must still beat it.
    @Test
    void aPaddedEncodingOrdersByValueToo() throws Exception {
        byte[] file = writeRawBinaryDecimals(paddedRows());
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
                RowReader rows = reader.buildRowReader()
                        .filter(FilterPredicate.gt("amount", new BigDecimal("1.27")))
                        .build()) {
            assertThat(collect(rows)).hasSize(PADDED_ROWS / 2)
                    .allMatch(value -> value.compareTo(new BigDecimal("3.00")) == 0);
        }
    }

    /// The bloom-filter and dictionary shortcuts answer "are these exact bytes present?", which
    /// stands in for "is this value present?" only where the value has one encoding. A filter
    /// that proves every probe absent must therefore drop the row group under
    /// [Comparison#FIXED_DECIMAL] and leave it standing under [Comparison#VARIABLE_DECIMAL],
    /// where a padded copy of the same number would have missed.
    @Test
    void onlyAByteExactEqualityConsultsTheBloomFilter() throws Exception {
        byte[] file = write(VALUES);
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            RowGroup rowGroup = reader.getFileMetaData().rowGroups().getFirst();
            // 127 sits inside [-256, 300], so statistics alone keep the row group either way.
            byte[] literal = { 0x7F };

            assertThat(decideWithEmptyBloomFilter(rowGroup, literal, Comparison.VARIABLE_DECIMAL))
                    .isEqualTo(FilterDecision.MIGHT_MATCH);
            assertThat(decideWithEmptyBloomFilter(rowGroup, literal, Comparison.FIXED_DECIMAL))
                    .isEqualTo(FilterDecision.CANNOT_MATCH);
        }
    }

    /// The bytes of `1.27` are in the file, and a byte `eq` for exactly those bytes came
    /// back empty: the row group's statistics are written in the column's signed order
    /// (`min = FF 00`, `max = 01 2C`) and pruning them unsigned sorts the literal `7F` below the
    /// minimum. The literal now takes the column's own comparison, which is the one those bounds
    /// were written in, so the row is found.
    @Test
    void aByteStringPredicateComparesAsTheColumnDoes() throws Exception {
        assertThat(filtered(FilterPredicate.eq("amount", new byte[] { 0x7F })))
                .containsExactly(new BigDecimal("1.27"));
    }

    /// `3.00` is `01 2C`, which sorts below the single byte `7F` byte-wise and above it as a
    /// number, so the ordering is visible in the rows that come back.
    @Test
    void aByteStringRangePredicateOrdersByValue() throws Exception {
        assertThat(filtered(FilterPredicate.gt("amount", new byte[] { 0x7F })))
                .containsExactly(new BigDecimal("1.28"), new BigDecimal("3.00"));
    }

    /// Membership compares each probe in the column's order, as `eq` does, so a probe for `127`
    /// finds every row holding it — the padded `00 00 00 7F` encodings included, which a
    /// byte-identity test would miss. parquet-java compares `In` the same way, through the
    /// column's `PrimitiveComparator` rather than `Binary.equals`.
    @Test
    void aByteStringSetPredicateFindsPaddedMembers() throws Exception {
        byte[] file = writeRawBinaryDecimals(paddedRows());
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
                RowReader rows = reader.buildRowReader()
                        .filter(FilterPredicate.in("amount", new byte[] { 0x7F }))
                        .build()) {
            assertThat(collect(rows)).hasSize(PADDED_ROWS / 2)
                    .allMatch(value -> value.compareTo(new BigDecimal("1.27")) == 0);
        }
    }

    /// A `BYTE_ARRAY` column that is not a `DECIMAL` orders as its bytes, so the same predicates
    /// stay available on it, a `String` literal among them.
    @Test
    void aByteStringPredicateStaysAvailableOnAPlainBinaryColumn() {
        FileSchema plain = FileSchema.builder("schema")
                .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, null)
                .build();

        assertThat(FilterPredicateResolver.resolve(FilterPredicate.eq("name", "a"), plain))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryPredicate.class,
                        p -> assertThat(p.comparison()).isEqualTo(Comparison.BYTE_STRING));
    }

    // ==================== Fixtures ====================

    private static FileSchema schema() {
        return FileSchema.builder("schema")
                .addColumn("amount", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        LogicalType.decimal(18, 2))
                .build();
    }

    private static FileSchema nullableSchema() {
        return FileSchema.builder("schema")
                .addColumn("amount", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                        LogicalType.decimal(18, 2))
                .build();
    }

    private static byte[] write(BigDecimal[] values) throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema())) {
            for (BigDecimal value : values) {
                writer.rowWriter().writeRow(row -> row.setDecimal("amount", value));
            }
        }
        return out.toByteArray();
    }

    private static List<BigDecimal> filtered(FilterPredicate predicate) throws Exception {
        return filtered(write(VALUES), predicate);
    }

    /// `1.27`, a null, `3.00`, a null — so a predicate that would take every value still has to
    /// leave the two nulls behind.
    private static List<BigDecimal> filteredWithNulls(FilterPredicate predicate) throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, nullableSchema())) {
            writer.rowWriter().writeRow(row -> row.setDecimal("amount", new BigDecimal("1.27")));
            writer.rowWriter().writeRow(row -> row.setNull("amount"));
            writer.rowWriter().writeRow(row -> row.setDecimal("amount", new BigDecimal("3.00")));
            writer.rowWriter().writeRow(row -> row.setNull("amount"));
        }
        return filtered(out.toByteArray(), predicate);
    }

    private static List<BigDecimal> filtered(byte[] file, FilterPredicate predicate) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
                RowReader rows = reader.buildRowReader().filter(predicate).build()) {
            return collect(rows);
        }
    }

    /// The row-group decision for an `EQ` against a bloom filter whose bitset is all zeroes, so
    /// every probe misses and the filter proves any value absent.
    private static FilterDecision decideWithEmptyBloomFilter(RowGroup rowGroup, byte[] literal,
            Comparison comparison) throws IOException {
        ResolvedPredicate predicate = new ResolvedPredicate.BinaryPredicate(0,
                FilterPredicate.Operator.EQ, literal, comparison);
        BloomFilterSource empty = columnIndex -> new BloomFilter(
                new BloomFilterHeader(BLOOM_FILTER_BYTES, BloomFilterHeader.Algorithm.BLOCK,
                        BloomFilterHeader.Hash.XXHASH, BloomFilterHeader.Compression.UNCOMPRESSED),
                ByteBuffer.allocate(BLOOM_FILTER_BYTES).order(ByteOrder.LITTLE_ENDIAN).asReadOnlyBuffer());
        return RowGroupFilterEvaluator.decideRowGroup(predicate, rowGroup, empty, null, UNNAMED, BoundsReadability.ALL);
    }

    /// Rows alternating a padded `127` with a minimally encoded `300`.
    private static byte[][] paddedRows() {
        byte[][] rows = new byte[PADDED_ROWS][];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = i % 2 == 0
                    ? new byte[] { 0x00, 0x00, 0x00, 0x7F } // 127, in four bytes rather than one
                    : new byte[] { 0x01, 0x2C }; // 300, minimally encoded
        }
        return rows;
    }

    /// Writes the given unscaled encodings verbatim, bypassing the decimal conversion, so the
    /// column can hold a value in a longer form than the minimal one.
    private static byte[] writeRawBinaryDecimals(byte[][] unscaled) throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema())) {
            writer.columnWriter().writeBatch(batch -> batch.bytes(0, unscaled));
        }
        return out.toByteArray();
    }

    private static List<BigDecimal> collect(RowReader rows) throws IOException {
        List<BigDecimal> matched = new ArrayList<>();
        while (rows.hasNext()) {
            rows.next();
            matched.add(rows.getDecimal("amount"));
        }
        return matched;
    }
}
