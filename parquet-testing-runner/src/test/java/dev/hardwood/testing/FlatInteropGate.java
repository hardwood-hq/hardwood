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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveComparator;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import dev.hardwood.OutputFile;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.testing.ParquetJavaReader.PageIndex;
import dev.hardwood.testing.ParquetJavaReader.Pages;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;

import static org.assertj.core.api.Assertions.assertThat;

/// The flat write-path interop gate (`_designs/WRITER_VALIDATION.md`): writes a single-column file
/// with Hardwood and asserts everything parquet-java can see about it, for [WriterInteropTest] and
/// [WriterCodecInteropTest].
final class FlatInteropGate {

    static final String COLUMN = "v";

    /// The rows of a case that needs no more than a page.
    static final int FEW_ROWS = 2_000;

    private FlatInteropGate() {
    }

    /// Writes the case's file with Hardwood and asserts everything parquet-java can see about it:
    /// that it reads at all, that every value and null matches what was written, and that the
    /// footer's row count, statistics and encodings do too.
    static Verified verify(InteropCase testCase, Path dir) throws IOException {
        return verify(testCase, dir, WritePath.BATCH);
    }

    static Verified verify(InteropCase testCase, Path dir, WritePath writePath) throws IOException {
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
        assertEncodingStats(footer, pages);
        assertPageIndex(testCase, file, footer, pages, ParquetJavaReader.readPageIndex(file));
        return new Verified(footer, pages);
    }

    /// Which of the two write APIs produces the file under test.
    enum WritePath { BATCH, ROW }

    private static void write(InteropCase testCase, Path file, WritePath writePath) throws IOException {
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
    private static void assertValues(InteropCase testCase, List<Group> rows) {
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

    private static void assertRowCount(InteropCase testCase, ParquetMetadata footer) {
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
    private static void assertStatistics(InteropCase testCase, ParquetMetadata footer) {
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
    private static void assertDistinctCounts(InteropCase testCase, Path file) throws IOException {
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
    private static void assertEncodings(InteropCase testCase, ParquetMetadata footer, Pages pages) {
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

    /// parquet-java reads every chunk's page index, and the page bounds agree with the written
    /// data under parquet-java's own comparator: folded over every page they are the case's true
    /// extremes, the same ones [#assertStatistics] holds the chunk statistics to, and a chunk whose
    /// index claims an ascending or descending boundary order has page bounds that are.
    ///
    /// A chunk carries no `ColumnIndex` only where the format forbids one: a column without an
    /// order, whose chunk statistics carry no bounds either, and a floating-point chunk with a page
    /// of nothing but `NaN`, which its raw `nan_count` must show it holds.
    @SuppressWarnings("unchecked")
    private static void assertPageIndex(InteropCase testCase, Path file, ParquetMetadata footer, Pages pages,
                                 List<PageIndex> indexes) throws IOException {
        ParquetJavaReader.assertPageIndexStructure(footer, pages, indexes);
        ParquetJavaReader.assertSizeStatistics(file, pages, indexes);
        List<ColumnChunkMetaData> chunks = chunks(footer);
        List<Long> nanCounts = ParquetJavaReader.readNanCounts(file);
        Comparable<Object> min = null;
        Comparable<Object> max = null;
        boolean everyChunkIndexed = true;
        for (int i = 0; i < chunks.size(); i++) {
            ColumnChunkMetaData chunk = chunks.get(i);
            PrimitiveType type = chunk.getPrimitiveType();
            ColumnIndex columnIndex = indexes.get(i).columnIndex();
            if (columnIndex == null) {
                boolean unordered = !chunk.getStatistics().hasNonNullValue()
                        && chunk.getStatistics().getNumNulls() < chunk.getValueCount();
                boolean allNaNPagePossible = isFloatingPoint(type) && nanCounts.get(i) > 0;
                assertThat(unordered || allNaNPagePossible)
                        .as("chunk %d has no column index although its column has an order", i).isTrue();
                everyChunkIndexed = false;
                continue;
            }
            PrimitiveComparator<Object> comparator = (PrimitiveComparator<Object>) type.comparator();
            Object previousMin = null;
            Object previousMax = null;
            boolean ascending = true;
            boolean descending = true;
            for (int p = 0; p < columnIndex.getNullPages().size(); p++) {
                if (columnIndex.getNullPages().get(p)) {
                    continue;
                }
                Statistics<?> bounds = Statistics.getBuilderForReading(type)
                        .withMin(bytes(columnIndex.getMinValues().get(p)))
                        .withMax(bytes(columnIndex.getMaxValues().get(p)))
                        .withNumNulls(0)
                        .build();
                Object pageMin = bounds.genericGetMin();
                Object pageMax = bounds.genericGetMax();
                assertThat(comparator.compare(pageMin, pageMax)).as("page %d of chunk %d has min <= max", p, i)
                        .isLessThanOrEqualTo(0);
                if (previousMin != null) {
                    ascending &= comparator.compare(previousMin, pageMin) <= 0
                            && comparator.compare(previousMax, pageMax) <= 0;
                    descending &= comparator.compare(previousMin, pageMin) >= 0
                            && comparator.compare(previousMax, pageMax) >= 0;
                }
                previousMin = pageMin;
                previousMax = pageMax;
                min = extreme(min, bounds.genericGetMin(), true);
                max = extreme(max, bounds.genericGetMax(), false);
            }
            switch (columnIndex.getBoundaryOrder()) {
                case ASCENDING -> assertThat(ascending).as("chunk %d's pages ascend", i).isTrue();
                case DESCENDING -> assertThat(descending).as("chunk %d's pages descend", i).isTrue();
                case UNORDERED -> {
                }
            }
        }
        if (everyChunkIndexed) {
            assertThat(min).as("page index min").isEqualTo(testCase.type().expectedMin(testCase));
            assertThat(max).as("page index max").isEqualTo(testCase.type().expectedMax(testCase));
        }
    }

    private static boolean isFloatingPoint(PrimitiveType type) {
        return type.getPrimitiveTypeName() == PrimitiveTypeName.FLOAT
                || type.getPrimitiveTypeName() == PrimitiveTypeName.DOUBLE
                || type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation;
    }

    private static byte[] bytes(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return bytes;
    }

    /// parquet-java's decoding of each chunk's `encoding_stats` agrees with the pages it walked:
    /// the same data-page encodings, a dictionary entry exactly where the chunk has a dictionary
    /// page, and data-page counts that add up to the pages in the file. These complete stats provide
    /// the metadata needed for dictionary-based row-group pruning when a reader applies that filter.
    private static void assertEncodingStats(ParquetMetadata footer, Pages pages) {
        List<ColumnChunkMetaData> chunks = chunks(footer);
        int dataPages = 0;
        for (int i = 0; i < chunks.size(); i++) {
            ColumnChunkMetaData chunk = chunks.get(i);
            EncodingStats stats = chunk.getEncodingStats();
            assertThat(stats).as("encoding_stats of chunk %d", i).isNotNull();
            assertThat(stats.getDataEncodings()).as("data-page encodings of chunk %d", i)
                    .containsExactlyInAnyOrderElementsOf(pages.chunkValueEncodings().get(i));
            assertThat(stats.hasDictionaryPages()).as("dictionary page entry of chunk %d", i)
                    .isEqualTo(chunk.hasDictionaryPage());
            if (stats.hasDictionaryPages()) {
                assertThat(stats.getDictionaryEncodings()).as("dictionary encodings of chunk %d", i)
                        .containsExactly(Encoding.PLAIN);
                assertThat(stats.getNumDictionaryPagesEncodedAs(Encoding.PLAIN)).isOne();
                assertThat(stats.hasNonDictionaryEncodedPages())
                        .as("chunk %d has a data page outside its dictionary", i).isFalse();
            }
            for (Encoding encoding : stats.getDataEncodings()) {
                dataPages += stats.getNumDataPagesEncodedAs(encoding);
            }
        }
        assertThat(dataPages).as("data pages counted by encoding_stats").isEqualTo(pages.dataPageCount());
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
            case ALP -> throw new IllegalArgumentException(
                    "parquet-java has no ALP encoding; see parquetJavaHasNoAlpEncoding");
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

    /// The cross product of every physical type with one axis's values.
    static <A, C> Stream<C> sweep(List<A> axis, BiFunction<TypeFixture, A, C> factory) {
        return Stream.of(TypeFixture.values())
                .flatMap(type -> axis.stream().map(value -> factory.apply(type, value)));
    }

    record Verified(ParquetMetadata footer, Pages pages) {
    }
}
