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
import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.internal.thrift.FileMetaDataReader;
import dev.hardwood.internal.thrift.FileMetaDataReader.ReadFooter;
import dev.hardwood.internal.thrift.FooterRewriter;
import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.internal.thrift.ThriftStructBuilder;
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
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Pruning compares a literal against recorded bounds, which answers only in the order those
/// bounds were written in. Three shapes arrive where that order is not knowable, none of them
/// produced by this writer, so each reaches the reader only on a file written elsewhere. An `INT96`
/// is the one type whose bounds are never read at all, which is not reported as a discard.
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

        assertThat(stats).isInstanceOf(MinMaxStats.NoBounds.class);
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

    /// An `INT96` compares as the instant it encodes. `parquet.thrift` leaves its type-defined
    /// order undefined, and no order bounds are recorded in follows the instant on every value,
    /// so its bounds prune nothing under any column order. Read as instants, this pair ends
    /// before the literal, which is what must not be acted on. The file is not at fault, so
    /// nothing is reported either.
    @Test
    void int96BoundsAreNotReadAndReportNothing() {
        FileSchema int96 = FileSchema.builder("s")
                .addColumn("ts", PhysicalType.INT96, RepetitionType.REQUIRED)
                .build();
        ResolvedPredicate leaf = FilterPredicateResolver.resolve(
                FilterPredicate.gt("ts", Instant.parse("2023-11-13T12:00:00Z")), int96);
        Statistics beforeTheLiteral = new Statistics(int96Bytes(0, 2_460_262), int96Bytes(255, 2_460_262),
                0L, null, false);

        MinMaxStats stats = MinMaxStats.of(beforeTheLiteral, leaf,
                readability(int96, List.of(ColumnOrder.TYPE_DEFINED_ORDER)));

        assertThat(stats.canDrop(leaf)).isFalse();
        assertThat(stats.discardReason()).isNull();
        assertThat(MinMaxStats.of(beforeTheLiteral, leaf, BoundsReadability.ALL).canDrop(leaf)).isFalse();
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
                readability(ints, List.of(ColumnOrder.UNKNOWN))).canDrop(leaf)).isFalse();
    }

    @Test
    void boundsUnderTheTypeDefinedOrderStillPrune() {
        FileSchema ints = intSchema();
        ResolvedPredicate leaf = FilterPredicateResolver.resolve(
                FilterPredicate.gt("v", 100), ints, List.of(ColumnOrder.TYPE_DEFINED_ORDER));
        Statistics stats = new Statistics(intBytes(0), intBytes(50), 0L, null, false);

        assertThat(MinMaxStats.of(stats, leaf,
                readability(ints, List.of(ColumnOrder.TYPE_DEFINED_ORDER))).canDrop(leaf))
                .isTrue();
    }

    /// A file that omits `column_orders` means the type-defined order throughout, so pruning
    /// stands.
    @Test
    void anAbsentColumnOrderListMeansTheTypeDefinedOrder() {
        FileSchema ints = intSchema();
        ResolvedPredicate leaf = FilterPredicateResolver.resolve(FilterPredicate.gt("v", 100), ints);
        Statistics stats = new Statistics(intBytes(0), intBytes(50), 0L, null, false);

        assertThat(MinMaxStats.of(stats, leaf, readability(ints, List.of()))
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
                readability(ints, List.of(ColumnOrder.UNKNOWN))).canDrop(leaf)).isFalse();
    }

    /// Readability is asked in one file's ordinals; an ordinal outside that file is a wiring
    /// error, not a column without bounds.
    @Test
    void anOrdinalOutsideTheFileIsRefused() {
        BoundsReadability readability = readability(intSchema());

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

    /// A `TIMESTAMP` held in sixteen bytes is no carrier the format defines, so the reader drops
    /// the annotation and reads the column as plain bytes. The writer recorded the bounds in the
    /// order of the annotation, as little-endian counts: `[256, 513]`, stored `00 01 …` and
    /// `01 02 …`. Read byte-wise those bounds hold together, yet the stored `300`, `2C 01 …`,
    /// sorts above the maximum, so pruning on them would drop the row that matches.
    @Test
    void boundsUnderADroppedAnnotationDoNotPrune() throws Exception {
        Path file = droppedTimestampWithValueOrderBounds();
        FilterPredicate aboveTwo = FilterPredicate.gt("ts", littleEndian(2));

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                RowReader rows = reader.buildRowReader().filter(aboveTwo).build()) {
            assertThat(reader.getFileSchema().getColumn("ts").logicalType()).isNull();
            List<byte[]> values = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                values.add(rows.getBinary("ts"));
            }
            assertThat(values).containsExactly(littleEndian(300));
        }
        assertThat(warnings.messages()).containsExactly(
                "Ignoring 1 logical type annotation(s) the column's physical type cannot carry; those "
                        + "columns are read as their physical type: ts (TIMESTAMP over a FIXED_LEN_BYTE_ARRAY "
                        + "is 12 bytes, but the column declares 16)",
                "[dropped-timestamp.parquet: row group 0, column 'ts'] Ignoring the min/max statistics "
                        + "for pruning: the order they were written in is one this reader cannot read. "
                        + "Rows they could have skipped are read and filtered instead.");
    }

    /// The column reader plans its row groups through the same bounds.
    @Test
    void boundsUnderADroppedAnnotationDoNotPruneTheColumnReader() throws Exception {
        Path file = droppedTimestampWithValueOrderBounds();

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                ColumnReader column = reader.buildColumnReader("ts")
                        .filter(FilterPredicate.gt("ts", littleEndian(2))).build()) {
            List<byte[]> values = new ArrayList<>();
            while (column.nextBatch()) {
                values.addAll(List.of(column.getBinaries()));
            }
            assertThat(values).containsExactly(littleEndian(300));
        }
    }

    /// A logical type this build does not recognize is dropped where the footer is parsed, so the
    /// column reads as unannotated. Its bounds are in the unknown type's order, which the reader
    /// cannot name either. The file's reader reaches the same bounds through its own footer read.
    @Test
    void boundsUnderAnUnrecognizedLogicalTypeDoNotPrune() throws Exception {
        Path file = sixteenByteColumnWithValueOrderBounds(LogicalType.uuid(), "unrecognized-type.parquet",
                UnreadableSortOrderTest::withUnrecognizedLogicalType);

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                RowReader rows = reader.buildRowReader()
                        .filter(FilterPredicate.gt("ts", littleEndian(2))).build()) {
            assertThat(reader.getFileSchema().getColumn("ts").logicalType()).isNull();
            List<byte[]> values = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                values.add(rows.getBinary("ts"));
            }
            assertThat(values).containsExactly(littleEndian(300));
        }
        assertThat(warnings.messages()).containsExactly(
                "Ignoring unrecognized LogicalType union field 20; the column will be read as its "
                        + "physical type. The file may have been written against a newer version of the "
                        + "format.",
                "[unrecognized-type.parquet: row group 0, column 'ts'] Ignoring the min/max statistics "
                        + "for pruning: the order they were written in is one this reader cannot read. "
                        + "Rows they could have skipped are read and filtered instead.");
    }

    /// The unannotated column beside a leaf of unrecognized logical type keeps its bounds.
    @Test
    void boundsUnderAnUnrecognizedLogicalTypeAreNotRead() {
        byte[] unknownMember = new ThriftStructBuilder()
                .field(20, FieldType.STRUCT).nested(new ThriftStructBuilder().stop().build())
                .stop().build();
        byte[] unknown = new ThriftStructBuilder()
                .field(1, FieldType.I32).i32(7) // FIXED_LEN_BYTE_ARRAY
                .field(2, FieldType.I32).i32(16)
                .field(3, FieldType.I32).i32(0) // REQUIRED
                .field(4, FieldType.BINARY).binary(bytes("ts"))
                .field(10, FieldType.STRUCT).nested(unknownMember) // logicalType
                .stop().build();
        byte[] plain = new ThriftStructBuilder()
                .field(1, FieldType.I32).i32(1) // INT32
                .field(3, FieldType.I32).i32(0) // REQUIRED
                .field(4, FieldType.BINARY).binary(bytes("v"))
                .stop().build();

        ReadFooter footer = readFooter(unknown, plain);
        FileSchema schema = FileSchema.fromSchemaElements(footer.metaData().schema());
        BoundsReadability readability = BoundsReadability.of(schema, footer);

        assertThat(schema.getColumn("ts").logicalType()).isNull();
        assertThat(readability.readable(0)).isFalse();
        assertThat(readability.readable(1)).isTrue();
    }

    /// A legacy `converted_type` the physical type cannot carry is dropped as a `LogicalType` is,
    /// and so are its bounds. `MAP_KEY_VALUE` annotates a group, so on a leaf it stands for no
    /// annotation at all: nothing is dropped and the leaf keeps its bounds.
    @Test
    void boundsUnderADroppedConvertedTypeAreNotRead() {
        byte[] timestamp = new ThriftStructBuilder()
                .field(1, FieldType.I32).i32(7) // FIXED_LEN_BYTE_ARRAY
                .field(2, FieldType.I32).i32(16)
                .field(3, FieldType.I32).i32(0) // REQUIRED
                .field(4, FieldType.BINARY).binary(bytes("ts"))
                .field(6, FieldType.I32).i32(10) // converted_type TIMESTAMP_MICROS
                .stop().build();
        byte[] mapKeyValue = new ThriftStructBuilder()
                .field(1, FieldType.I32).i32(1) // INT32
                .field(3, FieldType.I32).i32(0) // REQUIRED
                .field(4, FieldType.BINARY).binary(bytes("v"))
                .field(6, FieldType.I32).i32(2) // converted_type MAP_KEY_VALUE
                .stop().build();

        ReadFooter footer = readFooter(timestamp, mapKeyValue);
        FileSchema schema = FileSchema.fromSchemaElements(footer.metaData().schema());
        BoundsReadability readability = BoundsReadability.of(schema, footer);

        assertThat(schema.getColumn("ts").logicalType()).isNull();
        assertThat(schema.getColumn("v").logicalType()).isNull();
        assertThat(readability.readable(0)).isFalse();
        assertThat(readability.readable(1)).isTrue();
    }

    // ==================== Fixtures ====================

    /// A sixteen-byte column holding the little-endian counts `256`, `300` and `513`, annotated
    /// `TIMESTAMP(MICROS)` with row-group bounds `[256, 513]`.
    private Path droppedTimestampWithValueOrderBounds() throws IOException {
        return sixteenByteColumnWithValueOrderBounds(LogicalType.timestamp(true, LogicalType.TimeUnit.MICROS),
                "dropped-timestamp.parquet", UnaryOperator.identity());
    }

    /// A sixteen-byte column `ts` holding the little-endian counts `256`, `300` and `513`,
    /// annotated `annotation` with row-group bounds `[256, 513]`, its bytes then passed through
    /// `patch`.
    private Path sixteenByteColumnWithValueOrderBounds(LogicalType annotation, String fileName,
            UnaryOperator<byte[]> patch) throws IOException {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, FileSchema.builder("s")
                .addColumn("ts", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 16)
                .build())) {
            writer.columnWriter().writeBatch(batch -> batch.fixed(0, new byte[][] {
                    littleEndian(256), littleEndian(300), littleEndian(513) }));
        }
        byte[] rewritten = FooterRewriter.rewrite(out.toByteArray(), metaData -> new FileMetaData(
                metaData.version(),
                metaData.schema().stream().map(element -> annotated(element, annotation)).toList(),
                metaData.numRows(),
                metaData.rowGroups().stream()
                        .map(rowGroup -> withBounds(rowGroup, littleEndian(256), littleEndian(513)))
                        .toList(),
                metaData.keyValueMetadata(), metaData.createdBy(), metaData.columnOrders()));
        Path path = tempDir.resolve(fileName);
        Files.write(path, patch.apply(rewritten));
        return path;
    }

    private static SchemaElement annotated(SchemaElement element, LogicalType annotation) {
        return element.name().equals("ts")
                ? new SchemaElement(element.name(), element.type(), element.typeLength(),
                        element.repetitionType(), element.numChildren(), null, element.scale(),
                        element.precision(), element.fieldId(), annotation)
                : element;
    }

    /// Renumbers the footer's one `UUID` union member, field 14, to field 20, which no version
    /// of parquet-format defines. Its compact header grows from the short form `EC` to the long
    /// form `0C 28`, so the footer length is rewritten with it.
    private static byte[] withUnrecognizedLogicalType(byte[] file) {
        byte[] uuidMember = { (byte) 0xEC, 0x00, 0x00 };
        byte[] unknownMember = { 0x0C, 0x28, 0x00, 0x00 };
        int footerLength = ByteBuffer.wrap(file, file.length - 8, Integer.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN).getInt();
        int footerStart = file.length - 8 - footerLength;
        int at = -1;
        for (int i = footerStart; i <= file.length - 8 - uuidMember.length; i++) {
            if (Arrays.equals(file, i, i + uuidMember.length, uuidMember, 0, uuidMember.length)) {
                assertThat(at).as("a single UUID member in the footer").isEqualTo(-1);
                at = i;
            }
        }
        assertThat(at).as("a UUID member in the footer").isNotEqualTo(-1);
        ByteBuffer patched = ByteBuffer.allocate(file.length + unknownMember.length - uuidMember.length)
                .order(ByteOrder.LITTLE_ENDIAN);
        patched.put(file, 0, at);
        patched.put(unknownMember);
        patched.put(file, at + uuidMember.length, file.length - 8 - at - uuidMember.length);
        patched.putInt(footerLength + unknownMember.length - uuidMember.length);
        patched.put(file, file.length - 4, 4);
        return patched.array();
    }

    /// Reads a footer whose root group holds `leaves`, each an encoded `SchemaElement`.
    private static ReadFooter readFooter(byte[]... leaves) {
        byte[] root = new ThriftStructBuilder()
                .field(4, FieldType.BINARY).binary(bytes("s"))
                .field(5, FieldType.I32).i32(leaves.length)
                .stop().build();
        byte[][] elements = new byte[leaves.length + 1][];
        elements[0] = root;
        System.arraycopy(leaves, 0, elements, 1, leaves.length);
        byte[] footerBytes = new ThriftStructBuilder()
                .field(1, FieldType.I32).i32(2)
                .field(2, FieldType.LIST).structList(elements)
                .field(3, FieldType.I64).i64(0)
                .field(4, FieldType.LIST).structList()
                .stop().build();
        return FileMetaDataReader.readFooter(new ThriftCompactReader(ByteBuffer.wrap(footerBytes)));
    }

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
        return readability(schema, List.of());
    }

    /// A schema declared here drops no annotation.
    private static BoundsReadability readability(FileSchema schema, List<ColumnOrder> columnOrders) {
        return BoundsReadability.of(schema, columnOrders, ordinal -> false);
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
        return withBounds(rowGroup, bytes("N"), bytes("Z"));
    }

    /// Replaces the bounds of the fixture's one binary chunk.
    private static RowGroup withBounds(RowGroup rowGroup, byte[] min, byte[] max) {
        List<ColumnChunk> columns = new ArrayList<>(rowGroup.columns().size());
        for (ColumnChunk chunk : rowGroup.columns()) {
            ColumnMetaData m = chunk.metaData();
            if (m.type() != PhysicalType.BYTE_ARRAY && m.type() != PhysicalType.FIXED_LEN_BYTE_ARRAY) {
                columns.add(chunk);
                continue;
            }
            Statistics foreign = new Statistics(min, max, m.statistics().nullCount(),
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

    private static byte[] littleEndian(int value) {
        return ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }
}
