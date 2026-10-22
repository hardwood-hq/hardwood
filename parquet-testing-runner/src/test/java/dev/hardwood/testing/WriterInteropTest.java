/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.OutputFile;
import dev.hardwood.internal.writer.EncodingSupport;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.testing.FlatInteropGate.Verified;
import dev.hardwood.testing.FlatInteropGate.WritePath;
import dev.hardwood.testing.InteropCase.Nullability;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static dev.hardwood.testing.FlatInteropGate.COLUMN;
import static dev.hardwood.testing.FlatInteropGate.FEW_ROWS;
import static dev.hardwood.testing.FlatInteropGate.sweep;
import static dev.hardwood.testing.FlatInteropGate.verify;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The flat half of the write-path interop gate (`_designs/WRITER_VALIDATION.md`): Hardwood
/// writes a single-column file, parquet-java reads it back through its Group record model, and
/// the values, the null counts and the column-chunk statistics are asserted against the data
/// that was written.
///
/// The matrix varies one axis at a time — repetition, encoding, layout — against a representative
/// base, and sweeps each axis across all seven writable physical types, since the value encoders
/// are per type and that is where an encoding defect lives. The fixed-length case is narrower: it
/// varies the length of `FIXED_LEN_BYTE_ARRAY` alone. Its floor is a single-entry
/// dictionary per physical type, which `BOOLEAN`, never dictionary-encoded, writes `PLAIN`. That
/// dictionary shape produced #901: an `RLE_DICTIONARY` index stream with no run header, which
/// Hardwood's own reader and DuckDB both accept and parquet-java rejects.
///
/// The codec axis is in [WriterCodecInteropTest], which asserts through the same [FlatInteropGate];
/// the nested shapes are in [WriterNestedInteropTest] and the logical-type annotations in
/// [WriterLogicalTypeInteropTest].
class WriterInteropTest {

    /// Enough rows that a case crosses a page and a row-group boundary at the small targets the
    /// layout axis configures, and small enough that the whole matrix stays quick.
    private static final int MANY_ROWS = 20_000;

    // ==================== Axes ====================

    /// The floor of the matrix: a column chunk whose dictionary holds exactly one entry, in every
    /// repetition shape, for every physical type.
    static Stream<InteropCase> singleEntryDictionary() {
        return sweep(List.of(Nullability.values()), (type, nullability) -> InteropCase.of(
                "single-entry dictionary", type, nullability, 1, 200, WriterConfig.defaults()));
    }

    /// The encoding axis: the dictionary in its ordinary multi-entry form, declined because every
    /// value is distinct, and disabled outright.
    static Stream<InteropCase> encodings() {
        return sweep(List.of(
                new EncodingAxis("multi-entry dictionary", 64, WriterConfig.defaults(), false),
                new EncodingAxis("all-distinct column written PLAIN", FEW_ROWS,
                        WriterConfig.defaults(), true),
                new EncodingAxis("dictionary disabled", 64,
                        WriterConfig.builder().encoding(ColumnEncoding.PLAIN).build(), false)),
                (type, axis) -> new InteropCase(axis.name(), type, Nullability.OPTIONAL_SOME_NULL,
                        axis.distinct(), FEW_ROWS, axis.config(), axis.plainOnly()));
    }

    /// The optional-encoding axis: one case per legal (encoding, physical type) pair, in every
    /// repetition shape, written under a file-wide policy and read back through parquet-java.
    ///
    /// These are the encodings Hardwood could read but not write before stage 19b, so this axis
    /// is the first time its own output for them is put in front of another implementation. The
    /// pairs come from the same legality table [ColumnEncoding] states, restricted to the types
    /// this gate writes. Nullability is swept the way [#singleEntryDictionary] sweeps it, since a
    /// `REQUIRED` column takes a different level path and an `OPTIONAL_ALL_NULL` one asks the
    /// encoder for an empty range.
    static Stream<InteropCase> optionalEncodings() {
        return Stream.of(TypeFixture.values()).flatMap(type -> Stream.of(
                ColumnEncoding.DELTA_BINARY_PACKED, ColumnEncoding.DELTA_LENGTH_BYTE_ARRAY,
                ColumnEncoding.DELTA_BYTE_ARRAY, ColumnEncoding.BYTE_STREAM_SPLIT)
                .filter(encoding -> legal(encoding, type))
                .flatMap(encoding -> Stream.of(Nullability.values())
                        .map(nullability -> InteropCase.of("encoding " + encoding, type,
                                nullability, 64, FEW_ROWS,
                                WriterConfig.builder().encoding(encoding).build()))));
    }

    /// The legality table the writer itself applies, rather than a copy of it: a pair dropped
    /// from `EncodingSupport` leaves this axis in the same commit that drops it.
    private static boolean legal(ColumnEncoding encoding, TypeFixture type) {
        return EncodingSupport.supports(encoding, type.physicalType());
    }

    /// The `FIXED_LEN_BYTE_ARRAY` lengths the writer has a reason to see, against every encoding
    /// legal for the type.
    ///
    /// The length is part of what two of those encoders do rather than a detail above them:
    /// `BYTE_STREAM_SPLIT` scatters a value across one stream per byte position, so the number of
    /// streams *is* the length, and `DELTA_BYTE_ARRAY` splits each value into the prefix it
    /// shares with the one before and the suffix it does not. The flat matrix declares this type
    /// at one length, so neither encoder is otherwise seen at the lengths the annotations fix —
    /// two for `FLOAT16`, twelve for `INTERVAL`, sixteen for `UUID`.
    static Stream<FixedLengthCase> fixedLengths() {
        return Stream.of(2, 8, 12, 16).flatMap(typeLength -> Stream.of(
                new FixedLengthCase(typeLength, ColumnEncoding.AUTO, Encoding.RLE_DICTIONARY),
                new FixedLengthCase(typeLength, ColumnEncoding.PLAIN, Encoding.PLAIN),
                new FixedLengthCase(typeLength, ColumnEncoding.DELTA_BYTE_ARRAY, Encoding.DELTA_BYTE_ARRAY),
                new FixedLengthCase(typeLength, ColumnEncoding.BYTE_STREAM_SPLIT,
                        Encoding.BYTE_STREAM_SPLIT)));
    }

    /// The layout axis: one page, several pages within one row group, and several row groups.
    ///
    /// The single-page value is pinned exactly — 200 rows at the default 1 MiB targets is one
    /// page in one row group for every type — so it establishes the "one page" end of the axis
    /// instead of merely passing a bound any file satisfies. The other two are lower bounds: their
    /// small page and row-group targets are crossed many times over, and how many times is an
    /// artifact of the per-type value width rather than something worth pinning.
    static Stream<LayoutCase> layouts() {
        return sweep(List.of(
                new LayoutAxis("single page", 200, WriterConfig.defaults(), 1, 1, LayoutBound.EXACTLY),
                new LayoutAxis("multiple pages", MANY_ROWS,
                        WriterConfig.builder().pageTargetBytes(1024).build(), 1, 2, LayoutBound.AT_LEAST),
                // The target is well under what a BOOLEAN column retains at this row count — a
                // byte of definition level per entry beside a bit of value, so 22 KB of the two
                // — which is the narrowest of the seven types and so sets the bar for all of
                // them. Room enough, too, that a 64-value dictionary is a fraction of a chunk
                // rather than most of one: a target that cannot hold a dictionary and a chunk
                // worth amortizing it over cuts row groups that are written PLAIN, which is
                // honest about the memory it was given and tests nothing about dictionaries.
                new LayoutAxis("multiple row groups", MANY_ROWS, WriterConfig.builder()
                        .pageTargetBytes(1024).rowGroupBufferTargetBytes(8192).build(), 2, 2, LayoutBound.AT_LEAST)),
                (type, axis) -> new LayoutCase(
                        InteropCase.of(axis.name(), type, Nullability.OPTIONAL_SOME_NULL, 64,
                                axis.rows(), axis.config()),
                        axis.rowGroups(), axis.dataPages(), axis.bound()));
    }

    /// The write-path axis: the same shapes produced through the row-oriented layer rather than
    /// the columnar one. It stages records into batches of its own and submits them through the
    /// columnar core, so what it adds to the gate is the record-shaped entry point — the
    /// single-entry dictionary floor in every repetition shape, and a file whose records cross
    /// page and row-group boundaries.
    static Stream<InteropCase> rowWritten() {
        return Stream.concat(
                sweep(List.of(Nullability.values()), (type, nullability) -> InteropCase.of(
                        "row-written single-entry dictionary", type, nullability, 1, 200,
                        WriterConfig.defaults())),
                sweep(List.of(Nullability.OPTIONAL_SOME_NULL), (type, nullability) -> InteropCase.of(
                        "row-written across pages and row groups", type, nullability, 64, MANY_ROWS,
                        WriterConfig.builder().pageTargetBytes(1024).rowGroupBufferTargetBytes(8192).build())));
    }

    // ==================== Tests ====================

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void singleEntryDictionary(InteropCase testCase, @TempDir Path dir) throws IOException {
        verify(testCase, dir);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void encodings(InteropCase testCase, @TempDir Path dir) throws IOException {
        verify(testCase, dir);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void optionalEncodings(InteropCase testCase, @TempDir Path dir) throws IOException {
        verify(testCase, dir);
    }

    /// A `FIXED_LEN_BYTE_ARRAY` column of one length under one encoding, read back through
    /// parquet-java and asserted value by value, with the encoding the pages declared held to the
    /// one the policy asked for — without which a length that quietly fell back to `PLAIN` would
    /// pass as covered.
    @ParameterizedTest(name = "{0}")
    @MethodSource("fixedLengths")
    void fixedLengths(FixedLengthCase testCase, @TempDir Path dir) throws IOException {
        Path file = dir.resolve("fixed.parquet");
        byte[][] values = testCase.values();

        FileSchema schema = FileSchema.builder("interop")
                .addColumn(COLUMN, PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL,
                        testCase.typeLength())
                .build();
        WriterConfig config = WriterConfig.builder().encoding(testCase.policy()).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.fixed(COLUMN, values));
        }

        List<Group> rows = ParquetJavaReader.readGroups(file);
        assertThat(rows).as("row count").hasSize(values.length);
        for (int r = 0; r < values.length; r++) {
            assertThat(rows.get(r).getBinary(COLUMN, 0).getBytes()).as("row %d", r)
                    .isEqualTo(values[r]);
        }

        for (Set<Encoding> chunk : ParquetJavaReader.readPages(file).chunkValueEncodings()) {
            assertThat(chunk).as("page encodings").containsExactly(testCase.expected());
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void layouts(LayoutCase layoutCase, @TempDir Path dir) throws IOException {
        Verified verified = verify(layoutCase.base(), dir);

        int rowGroups = verified.footer().getBlocks().size();
        int dataPages = verified.pages().dataPageCount();
        if (layoutCase.bound() == LayoutBound.EXACTLY) {
            assertThat(rowGroups).as("row groups").isEqualTo(layoutCase.rowGroups());
            assertThat(dataPages).as("data pages").isEqualTo(layoutCase.dataPages());
        }
        else {
            assertThat(rowGroups).as("row groups").isGreaterThanOrEqualTo(layoutCase.rowGroups());
            assertThat(dataPages).as("data pages").isGreaterThanOrEqualTo(layoutCase.dataPages());
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void rowWritten(InteropCase testCase, @TempDir Path dir) throws IOException {
        verify(testCase, dir, WritePath.ROW);
    }

    /// A page whose values all resolve to dictionary index 0 declares an index bit width of zero
    /// while its chunk's dictionary holds several entries — the shape a per-page bit width
    /// introduces, and one no swept case produces, since a case's values cycle through their
    /// ordinals from the first row. A zero-bit index stream has shipped unreadable before (#901),
    /// so it is worth holding this shape to a strict reader too, not only the single-entry
    /// dictionary whose whole chunk is zero-bit.
    @Test
    void pageOfOneRepeatedValueInAMultiEntryDictionary(@TempDir Path dir) throws IOException {
        int leadingRun = 20_000;
        int rows = leadingRun + 20_000;
        int[] values = new int[rows];
        for (int r = leadingRun; r < rows; r++) {
            values[r] = 1 + (r % 7);
        }

        Path file = dir.resolve("leading-run.parquet");
        FileSchema schema = FileSchema.builder("interop")
                .addColumn(COLUMN, PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        // A small page target cuts several pages inside the leading run, so the file carries pages
        // whose largest index is 0 ahead of pages that need the full width.
        WriterConfig config = WriterConfig.builder().pageTargetBytes(2048).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(COLUMN, values));
        }

        assertThat(ParquetJavaReader.readDistinctCounts(file))
                .as("the chunk's dictionary holds more than the repeated value")
                .containsExactly(8L);
        // The shape is asserted rather than inferred from the values decoding: a page's declared
        // width is invisible to a reader that only checks what came back, so a return to a
        // chunk-wide width would leave every other assertion here green.
        List<Integer> widths = ParquetJavaReader.readIndexBitWidths(file);
        assertThat(widths).as("a page inside the leading run declares a zero-bit index stream")
                .contains(0);
        assertThat(widths).as("a page past the run declares what its own largest index needs")
                .contains(3);

        List<Group> read = ParquetJavaReader.readGroups(file);
        assertThat(read).as("row count").hasSize(rows);
        for (int r = 0; r < rows; r++) {
            assertThat(read.get(r).getInteger(COLUMN, 0)).as("row %d", r).isEqualTo(values[r]);
        }
    }

    /// A chunk whose dictionary was given up at a probe and interned again at flush: distinct
    /// through both probes, repeating the first values after them. Its dictionary page and index
    /// stream are rebuilt from the values the chunk stored, so the gate holds them to a strict
    /// reader, together with the exact count the rebuilt dictionary states.
    @Test
    void dictionaryInternedAgainAtFlush(@TempDir Path dir) throws IOException {
        int rows = 60_000;
        int distinct = 30_500;
        long[] values = new long[rows];
        for (int r = 0; r < rows; r++) {
            values[r] = (r < distinct ? r : r % distinct) * 7919L + 3;
        }

        Path file = dir.resolve("recovered.parquet");
        FileSchema schema = FileSchema.builder("interop")
                .addColumn(COLUMN, PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        WriterConfig config = WriterConfig.builder().rowGroupTargetRows(rows).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.longs(COLUMN, values));
        }

        ColumnChunkMetaData chunk = ParquetJavaReader.readFooter(file).getBlocks().get(0).getColumns().get(0);
        assertThat(chunk.hasDictionaryPage()).as("the chunk carries a dictionary page").isTrue();
        assertThat(chunk.getEncodingStats().getDataEncodings()).as("data page encodings")
                .containsExactly(Encoding.RLE_DICTIONARY);
        assertThat(ParquetJavaReader.readDistinctCounts(file)).containsExactly((long) distinct);

        List<Group> read = ParquetJavaReader.readGroups(file);
        assertThat(read).as("row count").hasSize(rows);
        for (int r = 0; r < rows; r++) {
            assertThat(read.get(r).getLong(COLUMN, 0)).as("row %d", r).isEqualTo(values[r]);
        }
    }

    /// Why `ALP` is absent from the optional-encoding axes, asserted rather than left as a
    /// comment.
    ///
    /// parquet-java's `Encoding` stops at `BYTE_STREAM_SPLIT`: no release up to 1.18.1 names ALP,
    /// let alone decodes its pages, so this gate has no reader for it. The writer's ALP output is
    /// covered by core's round trips (`AlpEncoderTest`, `WriterEncodingPolicyTest`,
    /// `WriterLayoutTest`) through a decoder `AlpReadTest` holds to Arrow C++'s output bit for
    /// bit.
    ///
    /// This test pins the reason, so a parquet-java that gains the encoding fails here and `ALP`
    /// joins the axes and the coverage domain rather than staying out by inertia.
    @Test
    void parquetJavaHasNoAlpEncoding() {
        assertThatThrownBy(() -> Encoding.valueOf("ALP"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No enum constant org.apache.parquet.column.Encoding.ALP");
    }

    // ==================== Matrix plumbing ====================

    private record EncodingAxis(String name, int distinct, WriterConfig config, boolean plainOnly) {
    }

    private record LayoutAxis(String name, int rows, WriterConfig config, int rowGroups, int dataPages,
            LayoutBound bound) {
    }

    /// Whether a layout case's row-group and page counts are the exact numbers the configuration
    /// produces, or the floor it must clear.
    enum LayoutBound {
        EXACTLY,
        AT_LEAST
    }

    /// A flat case plus the layout the writer configuration was sized to produce.
    record LayoutCase(InteropCase base, int rowGroups, int dataPages, LayoutBound bound) {
        @Override
        public String toString() {
            return base.toString();
        }
    }

    /// One `FIXED_LEN_BYTE_ARRAY` length written under one encoding policy, with the page
    /// encoding that policy must produce.
    ///
    /// The values share a prefix and differ in their last byte, which is what
    /// `DELTA_BYTE_ARRAY` exists to exploit, and there are few enough distinct ones that
    /// [ColumnEncoding#AUTO] arrives at a dictionary rather than at `PLAIN`.
    ///
    /// @param typeLength the column's declared length
    /// @param policy the encoding policy the file is written under
    /// @param expected the encoding its data pages must then declare
    record FixedLengthCase(int typeLength, ColumnEncoding policy, Encoding expected) {

        private static final int ROWS = 200;

        private static final int DISTINCT = 8;

        byte[][] values() {
            byte[][] values = new byte[ROWS][];
            for (int r = 0; r < ROWS; r++) {
                byte[] value = new byte[typeLength];
                Arrays.fill(value, (byte) 0x2a);
                value[typeLength - 1] = (byte) (r % DISTINCT);
                values[r] = value;
            }
            return values;
        }

        @Override
        public String toString() {
            return "FIXED_LEN_BYTE_ARRAY(" + typeLength + ") / " + policy;
        }
    }
}
