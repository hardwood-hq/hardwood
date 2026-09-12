/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.internal.thrift.FooterRewriter;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Pruning compares a literal against recorded bounds, which answers only in the order those
/// bounds were written in. Three shapes arrive where that order is not knowable, none of them
/// produced by this writer, so each reaches the reader only on a file written elsewhere.
class UnreadableSortOrderTest {

    @TempDir
    Path tempDir;

    @RegisterExtension
    final CapturedWarnings warnings = new CapturedWarnings();

    /// parquet-format defines no ordering for `GEOMETRY`, and `StatisticsOrder` keeps this
    /// writer from recording any. Another writer's bounds are in an order this reader has no
    /// way to name, so they prune nothing, and say why.
    @Test
    void boundsOnAnUnorderedAnnotationDoNotPrune() {
        FileSchema geometry = geometrySchema();
        ResolvedPredicate leaf = FilterPredicateResolver.resolve(
                FilterPredicate.eq("g", bytes("M")), geometry);
        Statistics foreign = new Statistics(bytes("N"), bytes("Z"), 0L, null, false);

        MinMaxStats stats = MinMaxStats.of(foreign, leaf, readability(geometry));

        assertThat(stats.canDrop(leaf)).isFalse();
        assertThat(stats.discardReason()).isEqualTo(MinMaxStats.UNKNOWN_SORT_ORDER);
        assertThat(MinMaxStats.of(foreign, leaf, BoundsReadability.ALL).canDrop(leaf))
                .as("byte-wise the probe sorts below the minimum, which is what must not be acted on")
                .isTrue();
    }

    /// A conforming writer records no bounds for an unordered annotation, so there is nothing
    /// to discard and nothing to report.
    @Test
    void anUnorderedAnnotationWithoutBoundsReportsNothing() {
        FileSchema geometry = geometrySchema();
        ResolvedPredicate leaf = FilterPredicateResolver.resolve(
                FilterPredicate.eq("g", bytes("M")), geometry);
        Statistics boundless = new Statistics(null, null, 0L, null, false);

        MinMaxStats stats = MinMaxStats.of(boundless, leaf, readability(geometry));

        assertThat(stats).isInstanceOf(MinMaxStats.NullCountOnlyStats.class);
        assertThat(stats.discardReason()).isNull();
    }

    /// An annotation that does name an order keeps its pruning.
    @Test
    void boundsOnAnOrderedAnnotationStillPrune() {
        FileSchema string = FileSchema.builder("s")
                .addColumn("g", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        LogicalType.string())
                .build();
        ResolvedPredicate leaf = FilterPredicateResolver.resolve(
                FilterPredicate.eq("g", "M"), string);
        Statistics stats = new Statistics(bytes("N"), bytes("Z"), 0L, null, false);

        assertThat(MinMaxStats.of(stats, leaf, readability(string)).canDrop(leaf)).isTrue();
    }

    /// An `INT96` compares as the instant it encodes, which no order a writer records its bounds
    /// in follows on every value, so its bounds prune nothing even under the type-defined order.
    /// These are the bounds parquet-java writes for a chunk holding 00:00, 00:00:00.000000255 on
    /// the 13th and 00:00:00.000000016 on the 14th: it orders the stored bytes as a big-endian
    /// integer, which the low byte of the nanoseconds leads, so the 14th falls between the two.
    @Test
    void int96BoundsDoNotPrune() {
        FileSchema int96 = FileSchema.builder("s")
                .addColumn("ts", PhysicalType.INT96, RepetitionType.REQUIRED)
                .build();
        ResolvedPredicate leaf = FilterPredicateResolver.resolve(
                FilterPredicate.gt("ts", Instant.parse("2023-11-13T12:00:00Z")), int96);
        Statistics byteOrdered = new Statistics(int96Bytes(0, 2_460_262), int96Bytes(255, 2_460_262),
                0L, null, false);

        assertThat(MinMaxStats.of(byteOrdered, leaf,
                BoundsReadability.of(int96, List.of(ColumnOrder.TYPE_DEFINED_ORDER))).canDrop(leaf)).isFalse();
        assertThat(MinMaxStats.of(byteOrdered, leaf, BoundsReadability.ALL).canDrop(leaf))
                .as("as instants the bounds end before the literal, which is what must not be acted on")
                .isTrue();
    }

    /// `parquet.thrift` on the `ColumnOrder` union: "If the reader does not support the value of
    /// this union, min and max stats for this column should be ignored." Every physical type is
    /// affected, not only the binary ones.
    @Test
    void boundsUnderAnUnrecognizedColumnOrderDoNotPrune() {
        FileSchema ints = intSchema();
        ResolvedPredicate leaf = FilterPredicateResolver.resolve(
                FilterPredicate.gt("v", 100), ints, List.of(ColumnOrder.UNKNOWN));
        Statistics foreign = new Statistics(intBytes(0), intBytes(50), 0L, null, false);

        assertThat(MinMaxStats.of(foreign, leaf,
                BoundsReadability.of(ints, List.of(ColumnOrder.UNKNOWN))).canDrop(leaf)).isFalse();
    }

    @Test
    void boundsUnderTheTypeDefinedOrderStillPrune() {
        FileSchema ints = intSchema();
        ResolvedPredicate leaf = FilterPredicateResolver.resolve(
                FilterPredicate.gt("v", 100), ints, List.of(ColumnOrder.TYPE_DEFINED_ORDER));
        Statistics stats = new Statistics(intBytes(0), intBytes(50), 0L, null, false);

        assertThat(MinMaxStats.of(stats, leaf,
                BoundsReadability.of(ints, List.of(ColumnOrder.TYPE_DEFINED_ORDER))).canDrop(leaf))
                .isTrue();
    }

    /// A file that omits `column_orders` means the type-defined order throughout, so pruning
    /// stands.
    @Test
    void anAbsentColumnOrderListMeansTheTypeDefinedOrder() {
        FileSchema ints = intSchema();
        ResolvedPredicate leaf = FilterPredicateResolver.resolve(FilterPredicate.gt("v", 100), ints);
        Statistics stats = new Statistics(intBytes(0), intBytes(50), 0L, null, false);

        assertThat(MinMaxStats.of(stats, leaf, BoundsReadability.of(ints, List.of()))
                .canDrop(leaf)).isTrue();
    }

    /// Page bounds are bounds, so the page index reads the same answer.
    @Test
    void pageBoundsUnderAnUnrecognizedColumnOrderDoNotPrune() {
        FileSchema ints = intSchema();
        ResolvedPredicate leaf = FilterPredicateResolver.resolve(
                FilterPredicate.gt("v", 100), ints, List.of(ColumnOrder.UNKNOWN));
        ColumnIndex columnIndex = new ColumnIndex(new boolean[] { false },
                List.of(intBytes(0)), List.of(intBytes(50)),
                ColumnIndex.BoundaryOrder.UNORDERED, new long[] { 0L }, null, null, null);

        assertThat(MinMaxStats.ofPage(columnIndex, 0, leaf,
                BoundsReadability.of(ints, List.of(ColumnOrder.UNKNOWN))).canDrop(leaf)).isFalse();
    }

    /// The null count needs no ordering, so it survives where the bounds do not.
    @Test
    void theNullCountSurvivesUnreadableBounds() {
        FileSchema ints = intSchema();
        ResolvedPredicate leaf = FilterPredicateResolver.resolve(
                FilterPredicate.gt("v", 100), ints, List.of(ColumnOrder.UNKNOWN));
        Statistics foreign = new Statistics(intBytes(0), intBytes(50), 7L, null, false);

        assertThat(MinMaxStats.of(foreign, leaf,
                BoundsReadability.of(ints, List.of(ColumnOrder.UNKNOWN))).nullCount()).isEqualTo(7L);
    }

    /// Readability is asked in one file's ordinals; an ordinal outside that file is a wiring
    /// error, not a column without bounds.
    @Test
    void anOrdinalOutsideTheFileIsRefused() {
        BoundsReadability readability = BoundsReadability.of(intSchema(), List.of());

        assertThatThrownBy(() -> readability.readable(1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Column 1 is not one of the 1 leaf columns of this file");
    }

    // ==================== Through the reader ====================

    /// Each file of a multi-file read is judged in its own ordinals. The two files hold the
    /// same columns in opposite leaf order, each with `GEOMETRY` bounds of `[N, Z]` around a
    /// stored `M`. Judged in the first file's ordinals, the second file's `g` would be taken
    /// for `v`, and its row group dropped on bounds this reader cannot read.
    @Test
    void eachFileOfAMultiFileReadIsJudgedInItsOwnOrdinals() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.openAll(List.of(
                InputFile.of(geometryThenValue()), InputFile.of(valueThenGeometry())));
                RowReader rows = reader.buildRowReader().filter(FilterPredicate.eq("g", bytes("M"))).build()) {
            assertThat(values(rows)).containsExactly(1, 2);
        }
    }

    /// The ordered column of the same pair is readable in both files, so nothing is reported
    /// about it — in the first file's ordinals, the second file's `v` would be taken for `g`.
    @Test
    void anOrderedColumnIsReadableInEveryFile() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.openAll(List.of(
                InputFile.of(geometryThenValue()), InputFile.of(valueThenGeometry())));
                RowReader rows = reader.buildRowReader().filter(FilterPredicate.gt("v", 100)).build()) {
            assertThat(values(rows)).isEmpty();
        }
        assertThat(warnings.messages()).isEmpty();
    }

    // ==================== Fixtures ====================

    private static FileSchema geometrySchema() {
        return FileSchema.builder("s")
                .addColumn("g", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        new LogicalType.GeometryType(null))
                .build();
    }

    private static FileSchema intSchema() {
        return FileSchema.builder("s")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
    }

    private static BoundsReadability readability(FileSchema schema) {
        return BoundsReadability.of(schema, List.of());
    }

    /// Leaves `g`, `v`, holding the single row `g = M, v = 1`.
    private Path geometryThenValue() throws IOException {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, FileSchema.builder("s")
                .addColumn("g", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, LogicalType.string())
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build())) {
            writer.columnWriter().writeBatch(batch -> batch
                    .bytes(0, new byte[][] { bytes("M") })
                    .ints(1, new int[] { 1 }));
        }
        return asForeignGeometry(out.toByteArray(), "g-then-v.parquet");
    }

    /// Leaves `v`, `g`, holding the single row `v = 2, g = M`.
    private Path valueThenGeometry() throws IOException {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, FileSchema.builder("s")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("g", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, LogicalType.string())
                .build())) {
            writer.columnWriter().writeBatch(batch -> batch
                    .ints(0, new int[] { 2 })
                    .bytes(1, new byte[][] { bytes("M") }));
        }
        return asForeignGeometry(out.toByteArray(), "v-then-g.parquet");
    }

    /// Rewrites the footer so that the string column `g` is annotated `GEOMETRY` and carries
    /// the bounds `[N, Z]`, as a writer other than this one might record them. Compared
    /// byte-wise, those bounds exclude the stored `M`.
    private Path asForeignGeometry(byte[] file, String fileName) throws IOException {
        byte[] rewritten = FooterRewriter.rewrite(file, metaData -> new FileMetaData(
                metaData.version(),
                metaData.schema().stream().map(UnreadableSortOrderTest::asGeometry).toList(),
                metaData.numRows(),
                metaData.rowGroups().stream().map(UnreadableSortOrderTest::withForeignBounds).toList(),
                metaData.keyValueMetadata(), metaData.createdBy(), metaData.columnOrders()));
        Path path = tempDir.resolve(fileName);
        Files.write(path, rewritten);
        return path;
    }

    private static SchemaElement asGeometry(SchemaElement element) {
        return element.name().equals("g")
                ? new SchemaElement(element.name(), element.type(), element.typeLength(),
                        element.repetitionType(), element.numChildren(), null, element.scale(),
                        element.precision(), element.fieldId(), new LogicalType.GeometryType(null))
                : element;
    }

    /// `g` is the only `BYTE_ARRAY` chunk in either fixture.
    private static RowGroup withForeignBounds(RowGroup rowGroup) {
        List<ColumnChunk> columns = new ArrayList<>(rowGroup.columns().size());
        for (ColumnChunk chunk : rowGroup.columns()) {
            ColumnMetaData m = chunk.metaData();
            if (m.type() != PhysicalType.BYTE_ARRAY) {
                columns.add(chunk);
                continue;
            }
            Statistics foreign = new Statistics(bytes("N"), bytes("Z"), m.statistics().nullCount(),
                    null, false);
            columns.add(new ColumnChunk(new ColumnMetaData(m.type(), m.encodings(), m.pathInSchema(),
                    m.codec(), m.numValues(), m.totalUncompressedSize(), m.totalCompressedSize(),
                    m.keyValueMetadata(), m.dataPageOffset(), m.dictionaryPageOffset(), foreign,
                    m.geospatialStatistics(), m.bloomFilterOffset(), m.bloomFilterLength(),
                    m.encodingStats(), m.sizeStatistics()),
                    chunk.offsetIndexOffset(), chunk.offsetIndexLength(), chunk.columnIndexOffset(),
                    chunk.columnIndexLength(), chunk.filePath()));
        }
        return new RowGroup(columns, rowGroup.totalByteSize(), rowGroup.numRows());
    }

    private static List<Integer> values(RowReader rows) throws IOException {
        List<Integer> values = new ArrayList<>();
        while (rows.hasNext()) {
            rows.next();
            values.add(rows.getInt("v"));
        }
        return values;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] int96Bytes(long nanosOfDay, int julianDay) {
        return ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).putLong(nanosOfDay).putInt(julianDay).array();
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }
}
