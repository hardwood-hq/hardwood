/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.OutputFile;
import dev.hardwood.internal.writer.EncodingSupport;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.testing.InteropCase.Nullability;
import dev.hardwood.testing.ParquetJavaReader.Pages;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The flat half of the write-path interop gate (`_designs-legacy/WRITER_INTEROP_GATE.md`): Hardwood
/// writes a single-column file, parquet-java reads it back through its Group record model, and
/// the values, the null counts and the column-chunk statistics are asserted against the data
/// that was written.
///
/// The matrix varies one axis at a time — repetition, encoding, codec, layout — against a
/// representative base, and sweeps every axis across all seven writable physical types, since
/// the value encoders are per type and that is where an encoding defect lives. Its floor is a
/// single-entry dictionary per physical type, the shape that produced #901: an `RLE_DICTIONARY`
/// index stream with no run header, which Hardwood's own reader and DuckDB both accept and
/// parquet-java rejects.
///
/// The nested shapes are in [WriterNestedInteropTest] and the logical-type annotations in
/// [WriterLogicalTypeInteropTest].
class WriterInteropTest {

    private static final String COLUMN = "v";

    /// Enough rows that a case crosses a page and a row-group boundary at the small targets the
    /// layout axis configures, and small enough that the whole matrix stays quick.
    private static final int MANY_ROWS = 20_000;

    private static final int FEW_ROWS = 2_000;

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

    /// The codec axis crossed with the optional encodings, which [#optionalEncodings] leaves at
    /// the default codec and [#codecs] leaves at the default encoding — so between them the four
    /// encodings stage 19b added meet no codec but one.
    ///
    /// The pair is what matters here rather than the type: a codec compresses a page body, and
    /// what these encodings change is the shape of that body. `BYTE_STREAM_SPLIT` is the case
    /// that makes the point — it changes no page's size on its own, and exists entirely for what
    /// the codec after it can then do — so each encoding is swept over the codecs on one type it
    /// is legal for rather than over all of them.
    ///
    /// Nullability is swept as it is in [#optionalEncodings], since the shape of a page body is
    /// what a codec sees: an `OPTIONAL_ALL_NULL` case hands the codec a body with no values in it
    /// at all, and a `REQUIRED` one a body the definition levels are absent from.
    static Stream<InteropCase> optionalEncodingCodecs() {
        return Stream.of(
                new EncodingCodecAxis(ColumnEncoding.DELTA_BINARY_PACKED, TypeFixture.INT64),
                new EncodingCodecAxis(ColumnEncoding.DELTA_LENGTH_BYTE_ARRAY, TypeFixture.BYTE_ARRAY),
                new EncodingCodecAxis(ColumnEncoding.DELTA_BYTE_ARRAY, TypeFixture.BYTE_ARRAY),
                new EncodingCodecAxis(ColumnEncoding.BYTE_STREAM_SPLIT, TypeFixture.DOUBLE))
                .flatMap(axis -> PARQUET_JAVA_CODECS.stream()
                        .flatMap(codec -> Stream.of(Nullability.values())
                                .map(nullability -> InteropCase.of(
                                        axis.encoding() + " under " + codec, axis.type(),
                                        nullability, 64, FEW_ROWS,
                                        WriterConfig.builder()
                                                .encoding(axis.encoding())
                                                .codec(codec)
                                                .build()))));
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

    /// The codec axis, over every codec the writer produces. The two it does not — `LZ4`'s
    /// deprecated Hadoop framing, and `LZO` — fail at writer creation, so they have no file for
    /// this gate to read back; `WriterRoundTripTest` holds them to failing there.
    ///
    /// What the axis is really checking is framing. Each of these codecs has a raw block form and
    /// a wrapped one, and the wrapped form round-trips perfectly against Hardwood's own reader
    /// while being the wrong bytes for everyone else — which is what makes an independent reader,
    /// rather than a round trip, the thing that settles it.
    ///
    /// `BROTLI` is the one codec the writer produces that is missing here, because the pinned
    /// parquet-java cannot read it at all — see [#parquetJavaHasNoBrotliCodec()].
    static Stream<InteropCase> codecs() {
        return sweep(PARQUET_JAVA_CODECS,
                (type, codec) -> InteropCase.of("codec " + codec, type, Nullability.OPTIONAL_SOME_NULL,
                        64, FEW_ROWS, WriterConfig.builder().codec(codec).build()));
    }

    /// Every codec the writer produces that the pinned parquet-java can also read, which is all
    /// of them but `BROTLI` — see [#parquetJavaHasNoBrotliCodec()].
    private static final List<CompressionCodec> PARQUET_JAVA_CODECS = List.of(
            CompressionCodec.UNCOMPRESSED, CompressionCodec.GZIP, CompressionCodec.SNAPPY,
            CompressionCodec.ZSTD, CompressionCodec.LZ4_RAW);

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

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void codecs(InteropCase testCase, @TempDir Path dir) throws IOException {
        verify(testCase, dir);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void optionalEncodingCodecs(InteropCase testCase, @TempDir Path dir) throws IOException {
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

    // ==================== The gate ====================

    /// Writes the case's file with Hardwood and asserts everything parquet-java can see about it:
    /// that it reads at all, that every value and null matches what was written, and that the
    /// footer's row count, statistics and encodings do too.
    private Verified verify(InteropCase testCase, Path dir) throws IOException {
        return verify(testCase, dir, WritePath.BATCH);
    }

    private Verified verify(InteropCase testCase, Path dir, WritePath writePath) throws IOException {
        Path file = dir.resolve("written.parquet");
        write(testCase, file, writePath);

        assertValues(testCase, ParquetJavaReader.readGroups(file));

        ParquetMetadata footer = ParquetJavaReader.readFooter(file);
        Pages pages = ParquetJavaReader.readPages(file);
        ParquetJavaReader.assertParseableCreatedBy(footer);
        assertRowCount(testCase, footer);
        assertStatistics(testCase, footer);
        assertDistinctCounts(testCase, file);
        assertEncodings(testCase, footer, pages);
        return new Verified(footer, pages);
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

    /// Why `BROTLI` is absent from the codec axis, asserted rather than left as a comment.
    ///
    /// parquet-java resolves the codec by name through Hadoop's `CompressionCodec` registry, and
    /// for `BROTLI` the name is `org.apache.hadoop.io.compress.BrotliCodec` — a class that ships
    /// in neither parquet-java 1.18.1 nor Hadoop, but in `com.github.rdblue:brotli-codec`, an
    /// unmaintained third-party artifact whose bundled native binaries cover a few platforms
    /// only. Putting it on this module's classpath would make the gate's result depend on the
    /// architecture it runs on, which is the opposite of what a gate is for.
    ///
    /// So the codec is not unverified, only unverified *against this reader*:
    /// `WriterDifferentialTest` reads Hardwood's `BROTLI` files back through DuckDB, which
    /// decompresses them natively. The read direction has the same hole for the same reason —
    /// `large_string_map.brotli.parquet` is the corpus's only `BROTLI` file and sits in
    /// [Utils#SKIPPED_FILES] — so this is one gap in parquet-java, not a new one the write path
    /// introduced.
    ///
    /// This test pins the reason, so a parquet-java that gains the codec fails here and `BROTLI`
    /// rejoins the axis rather than staying out by inertia.
    @Test
    void parquetJavaHasNoBrotliCodec() {
        assertThatThrownBy(() -> Class.forName("org.apache.hadoop.io.compress.BrotliCodec"))
                .isInstanceOf(ClassNotFoundException.class);
    }

    /// Which of the two write APIs produces the file under test.
    private enum WritePath { BATCH, ROW }

    private void write(InteropCase testCase, Path file, WritePath writePath) throws IOException {
        FileSchema schema = testCase.type()
                .declare(FileSchema.builder("interop"), COLUMN, testCase.nullability().repetitionType())
                .build();

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, testCase.config())) {
            if (writePath == WritePath.BATCH) {
                writer.columnWriter().writeBatch(batch -> testCase.type().set(batch, COLUMN, testCase));
                return;
            }
            RowWriter rows = writer.rowWriter();
            for (int r = 0; r < testCase.rows(); r++) {
                int row = r;
                if (testCase.isNull(row)) {
                    rows.writeRow(record -> record.setNull(COLUMN));
                }
                else {
                    rows.writeRow(record -> testCase.type().set(record, COLUMN, testCase.ordinal(row)));
                }
            }
        }
    }

    /// Every row parquet-java produces carries the value that was written, and a null row is
    /// absent from its group rather than carrying a stand-in value.
    private void assertValues(InteropCase testCase, List<Group> rows) {
        assertThat(rows).as("row count").hasSize(testCase.rows());
        for (int r = 0; r < rows.size(); r++) {
            Group row = rows.get(r);
            int field = row.getType().getFieldIndex(COLUMN);
            if (testCase.isNull(r)) {
                assertThat(row.getFieldRepetitionCount(field)).as("row %d is null", r).isZero();
            }
            else {
                assertThat(row.getFieldRepetitionCount(field)).as("row %d is present", r).isOne();
                assertThat(testCase.type().read(row, field)).as("row %d value", r)
                        .isEqualTo(testCase.type().value(testCase.ordinal(r)));
            }
        }
    }

    private void assertRowCount(InteropCase testCase, ParquetMetadata footer) {
        long rows = footer.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
        assertThat(rows).as("footer row count").isEqualTo(testCase.rows());
    }

    /// The column-chunk statistics parquet-java exposes agree with the written data. It builds
    /// them with its own comparator, derived from the column's type and the footer's
    /// `column_orders`, so agreement here is a cross-implementation check on the sort order the
    /// bounds were computed in — not merely on the bytes they were stored as.
    ///
    /// Bounds are folded over the row groups, so the assertion holds whatever layout the case
    /// produced.
    private void assertStatistics(InteropCase testCase, ParquetMetadata footer) {
        long nulls = 0;
        Comparable<Object> min = null;
        Comparable<Object> max = null;
        for (ColumnChunkMetaData chunk : chunks(footer)) {
            Statistics<?> statistics = chunk.getStatistics();
            nulls += statistics.getNumNulls();
            if (!statistics.hasNonNullValue()) {
                continue;
            }
            min = extreme(min, statistics.genericGetMin(), true);
            max = extreme(max, statistics.genericGetMax(), false);
        }

        assertThat(nulls).as("null count").isEqualTo(testCase.nullCount());
        assertThat(min).as("min").isEqualTo(testCase.type().expectedMin(testCase));
        assertThat(max).as("max").isEqualTo(testCase.type().expectedMax(testCase));
    }

    /// `distinct_count` is written for a chunk that knows its cardinality exactly and left out
    /// otherwise, read here from the footer's own Thrift rather than through parquet-java's
    /// `Statistics`, which does not surface the field — so a wrong field id or type fails here
    /// rather than round-tripping cleanly through the writer and reader that agree on it.
    ///
    /// A chunk knows the count while it still holds what it counted with, which is any chunk of a
    /// dictionary-capable type with dictionary encoding enabled, whichever encoding the comparison
    /// then chose, plus every `BOOLEAN` chunk. The exact value is only asserted for a single-chunk
    /// file, since a chunk of a multi-row-group file counts its own values rather than the file's.
    private void assertDistinctCounts(InteropCase testCase, Path file) throws IOException {
        List<Long> counts = ParquetJavaReader.readDistinctCounts(file);
        boolean known = testCase.type() == TypeFixture.BOOLEAN
                || (testCase.config().defaultEncoding() == ColumnEncoding.AUTO
                        && testCase.type().dictionaryCapable());
        if (!known) {
            assertThat(counts).as("distinct_count where the chunk cannot state it")
                    .containsOnlyNulls();
            return;
        }
        assertThat(counts).as("distinct_count where the chunk knows it").doesNotContainNull();
        if (counts.size() == 1) {
            assertThat(counts.get(0)).as("distinct_count").isEqualTo(expectedDistinctCount(testCase));
        }
    }

    /// How many distinct values the case's present rows carry, counted the way the value
    /// assertions count them so a type that maps many ordinals onto few values — `BOOLEAN` onto
    /// two — is expected to report what it actually wrote.
    private static long expectedDistinctCount(InteropCase testCase) {
        Set<Object> distinct = new HashSet<>();
        for (int row = 0; row < testCase.rows(); row++) {
            if (!testCase.isNull(row)) {
                Object value = testCase.type().value(testCase.ordinal(row));
                // A binary fixture hands back byte[], which compares by identity — wrap it so
                // this counts the values the writer counts rather than the arrays holding them.
                distinct.add(value instanceof byte[] bytes ? ByteBuffer.wrap(bytes) : value);
            }
        }
        return distinct.size();
    }

    /// The case actually produced the encoding it exists to cover, so a change in the writer's
    /// encoding choice becomes a failure rather than a silent loss of coverage.
    ///
    /// Two things are asserted, because a chunk's encoding is chosen from the values that chunk
    /// holds. **Within a chunk** the encoding never varies — that is the guarantee the row-group
    /// wide decision provides, and the discriminating assertion is on the *page* value encodings,
    /// not the chunk's `encodings` list, which always contains `PLAIN` because a dictionary page
    /// body is itself `PLAIN`. **Across chunks** the case's intent is pinned on the first chunk,
    /// which is a full one; a case whose values argue against a dictionary must not produce one
    /// anywhere, while a case whose values argue for one may still write a short trailing chunk
    /// `PLAIN`, where too few values are left for a dictionary to pay for itself.
    private void assertEncodings(InteropCase testCase, ParquetMetadata footer, Pages pages) {
        for (Set<Encoding> chunkEncodings : pages.chunkValueEncodings()) {
            assertThat(chunkEncodings).as("page value encodings within one chunk").hasSize(1);
        }
        Encoding expected = testCase.expectsDictionary() ? Encoding.RLE_DICTIONARY
                : valueEncodingOf(testCase.config().defaultEncoding());
        assertThat(pages.chunkValueEncodings().get(0)).as("first chunk's page value encoding")
                .containsExactly(expected);
        if (!testCase.expectsDictionary()) {
            for (ColumnChunkMetaData chunk : chunks(footer)) {
                assertThat(chunk.getEncodings()).as("declared chunk encodings")
                        .doesNotContain(Encoding.RLE_DICTIONARY);
            }
        }
        else {
            assertThat(chunks(footer).get(0).getEncodings()).as("first chunk's declared encodings")
                    .contains(Encoding.RLE_DICTIONARY);
        }
    }

    /// parquet-java's name for the page encoding a file-wide policy produces. `AUTO` reaches
    /// here only for a case whose values argued against a dictionary, which is `PLAIN`.
    private static Encoding valueEncodingOf(ColumnEncoding encoding) {
        return switch (encoding) {
            case AUTO, PLAIN -> Encoding.PLAIN;
            case DELTA_BINARY_PACKED -> Encoding.DELTA_BINARY_PACKED;
            case DELTA_LENGTH_BYTE_ARRAY -> Encoding.DELTA_LENGTH_BYTE_ARRAY;
            case DELTA_BYTE_ARRAY -> Encoding.DELTA_BYTE_ARRAY;
            case BYTE_STREAM_SPLIT -> Encoding.BYTE_STREAM_SPLIT;
        };
    }

    private static List<ColumnChunkMetaData> chunks(ParquetMetadata footer) {
        List<ColumnChunkMetaData> chunks = new ArrayList<>();
        for (BlockMetaData block : footer.getBlocks()) {
            chunks.addAll(block.getColumns());
        }
        return chunks;
    }

    /// Folds one row group's bound into the running one, in parquet-java's own comparison order
    /// for the type.
    @SuppressWarnings("unchecked")
    private static Comparable<Object> extreme(Comparable<Object> current, Comparable<?> candidate, boolean min) {
        Comparable<Object> other = (Comparable<Object>) candidate;
        if (current == null) {
            return other;
        }
        int order = other.compareTo(current);
        return (min ? order < 0 : order > 0) ? other : current;
    }

    // ==================== Matrix plumbing ====================

    /// The cross product of every physical type with one axis's values.
    private static <A, C> Stream<C> sweep(List<A> axis, BiFunction<TypeFixture, A, C> factory) {
        return Stream.of(TypeFixture.values())
                .flatMap(type -> axis.stream().map(value -> factory.apply(type, value)));
    }

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

    private record EncodingCodecAxis(ColumnEncoding encoding, TypeFixture type) {
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

    private record Verified(ParquetMetadata footer, Pages pages) {
    }
}
