/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.LayerKind;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// A [ScheduledNestedColumn] publishes the records the [ReferencePageDecoder] decodes from the
/// column's pages, whatever its batch capacity: read at capacities that put batch edges inside
/// pages and pages inside batches, and at one whose batches end only at row groups, record by
/// record its batches hold the reference's levels and values, every array allocated at the size
/// its batch holds. A batch that keeps only the real-items view holds the view the reference's
/// levels give: its layer offsets, layer validity, leaf validity, real values and where each real
/// value lies among the record's values.
///
/// The fixtures cover lists, maps and structs, pages beginning inside a record, pages holding no
/// record start, dictionary and plain binary leaves, fixed-size lists, and nested leaves in every
/// encoding read in place, read in every index mode and under a row cap.
class ScheduledNestedColumnTest {

    private static final String RESOURCES = "src/test/resources/";

    /// The fixture this class writes itself: nested `ALP` leaves, which PyArrow does not write.
    private static final String ALP_FIXTURE = "nested_alp.parquet";

    /// A batch capacity no fixture's row group reaches, so that its batches end at row groups.
    private static final int WHOLE_ROW_GROUPS = 1 << 20;

    @TempDir
    static Path written;

    static Stream<Arguments> reads() {
        List<Arguments> reads = new ArrayList<>();
        for (String fixture : List.of("nested_v1_mid_record_pages.parquet", "nested_v1_long_record_pages.parquet",
                "nested_v1_no_index.parquet", "list_of_list_of_struct_test.parquet", "map_with_list_value_test.parquet",
                "nested_binary_test.parquet", "nested_dict_batch_boundary.parquet", "dict_nested_repeats.parquet",
                "deep_nested_struct_test.parquet", "fixed_size_list_k4_v1.parquet", "fixed_size_list_k4_leadfast_v2.parquet",
                "misaligned_pages_nested_v2.parquet", "nested_in_place_encodings_v1.parquet",
                "nested_in_place_encodings_v2.parquet", ALP_FIXTURE)) {
            for (NestedIndexMode mode : NestedIndexMode.values()) {
                for (int batchSize : new int[]{ 1, 3, 64 }) {
                    reads.add(Arguments.of(fixture, mode, batchSize, 0));
                }
                reads.add(Arguments.of(fixture, mode, 3, 7));
            }
        }
        return reads.stream();
    }

    @BeforeAll
    static void writeAlpFixture() throws Exception {
        int records = 300;
        int[] ids = new int[records];
        int[] offsets = new int[records + 1];
        boolean[] listNulls = new boolean[records];
        boolean[] structNulls = new boolean[records];
        boolean[] xNulls = new boolean[records];
        double[] xs = new double[records];
        List<Integer> elementIndexes = new ArrayList<>();
        for (int r = 0; r < records; r++) {
            ids[r] = r;
            listNulls[r] = r % 7 == 3;
            if (!listNulls[r]) {
                for (int k = 0; k < r % 4; k++) {
                    elementIndexes.add(elementIndexes.size());
                }
            }
            offsets[r + 1] = elementIndexes.size();
            structNulls[r] = r % 6 == 5;
            xNulls[r] = r % 4 == 2;
            xs[r] = r % 31 == 0 ? Double.NaN : (r % 80) * 0.125 + r;
        }
        int elements = elementIndexes.size();
        float[] floats = new float[elements];
        double[] doubles = new double[elements];
        boolean[] elementNulls = new boolean[elements];
        for (int i = 0; i < elements; i++) {
            floats[i] = i % 29 == 0 ? Float.NaN : (i % 50) * 0.25f;
            doubles[i] = (i % 80) * 0.125 + i;
            elementNulls[i] = i % 5 == 2;
        }
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .list("f", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.FLOAT, RepetitionType.OPTIONAL))
                .list("d", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.DOUBLE, RepetitionType.OPTIONAL))
                .struct("s", RepetitionType.OPTIONAL,
                        struct -> struct.addColumn("x", PhysicalType.DOUBLE, RepetitionType.OPTIONAL))
                .build();
        WriterConfig config = WriterConfig.builder()
                .codec(CompressionCodec.UNCOMPRESSED)
                .encoding(ColumnEncoding.ALP)
                .encoding("id", ColumnEncoding.PLAIN)
                .pageTargetRows(5)
                .rowGroupTargetRows(100)
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(written.resolve(ALP_FIXTURE)), schema,
                config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints("id", ids)
                    .list("f", offsets, Validity.ofNulls(listNulls)).floats("f.list.element", floats, elementNulls)
                    .list("d", offsets, Validity.ofNulls(listNulls)).doubles("d.list.element", doubles, elementNulls)
                    .struct("s", Validity.ofNulls(structNulls)).doubles("s.x", xs, xNulls));
        }
    }

    @ParameterizedTest(name = "{0} {1} batch {2} cap {3}")
    @MethodSource("reads")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void publishesTheReferenceRecordsAtEveryBatchCapacity(String fixture, NestedIndexMode mode, int batchSize,
                                                          int maxRows) throws Exception {
        Path file = fixture.equals(ALP_FIXTURE) ? written.resolve(fixture) : Path.of(RESOURCES + fixture);
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            schema = reader.getFileSchema();
        }
        for (int c = 0; c < schema.getColumnCount(); c++) {
            ColumnSchema column = schema.getColumn(c);
            NestedLevelComputer.Layers layers = NestedLevelComputer.computeLayers(schema.getRootNode(), c);
            List<String> expected = reference(file, c, mode, layers);
            if (maxRows > 0 && expected.size() > maxRows) {
                expected = expected.subList(0, maxRows);
            }
            for (int capacity : new int[]{ batchSize, WHOLE_ROW_GROUPS }) {
                List<NestedBatch> batches = read(file, c, mode, capacity, maxRows);
                for (int b = 0; b < batches.size(); b++) {
                    assertWellFormed(batches.get(b), column.maxDefinitionLevel(),
                            "column " + c + " capacity " + capacity + " batch " + b);
                }
                assertThat(records(batches, column.maxDefinitionLevel(), layers))
                        .as("column %d (%s) records at capacity %d", c, column.name(), capacity)
                        .isEqualTo(expected);
            }
        }
    }

    /// The long-record fixture holds pages without a record start: a record spans three or more
    /// of its pages.
    @Test
    void longRecordFixtureHasPagesWithoutARecordStart() throws Exception {
        InputFile inputFile = InputFile.of(Path.of(RESOURCES + "nested_v1_long_record_pages.parquet"));
        inputFile.open();
        int pagesWithoutRecordStart = 0;
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            FileMetaData meta = ParquetMetadataReader.readMetadata(inputFile);
            FileSchema schema = FileSchema.fromSchemaElements(meta.schema());
            ColumnSchema tags = schema.getColumn("tags.list.element");
            PageIterator pages = SequentialFetchPlan.build(inputFile, tags,
                    meta.rowGroups().get(0).columns().get(tags.columnIndex()), context, 0, inputFile.name(), 0).pages();
            while (pages.hasNext()) {
                PageInfo page = pages.next();
                int[] repetitionLevels = new ReferencePageDecoder(tags, page.columnMetaData().codec(),
                        context.decompressorFactory()).decode(page.pageData(), page.dictionary()).repetitionLevels();
                if (Arrays.stream(repetitionLevels).noneMatch(level -> level == 0)) {
                    pagesWithoutRecordStart++;
                }
            }
        }
        finally {
            inputFile.close();
        }
        assertThat(pagesWithoutRecordStart).isPositive();
    }

    /// Reads one column of `file` through a scheduled column with the fixed-size-list fast path on,
    /// ending at `maxRows` rows unless it is `0`.
    private static List<NestedBatch> read(Path file, int columnIndex, NestedIndexMode mode, int batchSize,
                                          long maxRows) throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            FileSchema schema = reader.getFileSchema();
            ColumnSchema column = schema.getColumn(columnIndex);
            InputFile inputFile = InputFile.of(file);
            inputFile.open();
            RowGroupIterator iterator = new RowGroupIterator(List.of(inputFile), context, 0);
            iterator.setFirstFile(schema, reader.getFileMetaData().rowGroups());
            iterator.initialize(ProjectedSchema.create(schema, ColumnProjection.all()), null);
            iterator.planBatches(batchSize, maxRows);
            NestedLevelComputer.Layers layers = NestedLevelComputer.computeLayers(schema.getRootNode(), columnIndex);
            BatchExchange<NestedBatch> exchange = BatchExchange.detachingNested(column.name(), batchSize);
            PageSource pageSource = new PageSource(iterator, columnIndex);
            ReadScheduler scheduler = new ReadScheduler(context, batchSize, batchSize);
            ScheduledNestedColumn scheduled = new ScheduledNestedColumn(scheduler, pageSource, exchange, column,
                    batchSize, context.decompressorFactory(), layers, mode, true);
            scheduler.start();
            List<NestedBatch> batches = new ArrayList<>();
            NestedBatch batch;
            while ((batch = exchange.poll()) != null && batch.recordCount > 0) {
                batches.add(batch);
            }
            exchange.checkError();
            scheduled.close();
            iterator.close();
            inputFile.close();
            return batches;
        }
    }

    // ==================== Reference ====================

    /// The column's records as [#records] renders them, from the column's pages decoded whole by
    /// the reference decoder and joined per record, row group by row group.
    private static List<String> reference(Path file, int columnIndex, NestedIndexMode mode,
                                          NestedLevelComputer.Layers layers) throws Exception {
        List<String> records = new ArrayList<>();
        InputFile inputFile = InputFile.of(file);
        inputFile.open();
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            FileMetaData meta = ParquetMetadataReader.readMetadata(inputFile);
            FileSchema schema = FileSchema.fromSchemaElements(meta.schema());
            ColumnSchema column = schema.getColumn(columnIndex);
            for (int g = 0; g < meta.rowGroups().size(); g++) {
                RowGroup rowGroup = meta.rowGroups().get(g);
                PageIterator pages = SequentialFetchPlan.build(inputFile, column, rowGroup.columns().get(columnIndex),
                        context, g, inputFile.name(), 0).pages();
                ReferenceChunk chunk = new ReferenceChunk(column);
                while (pages.hasNext()) {
                    PageInfo page = pages.next();
                    chunk.add(new ReferencePageDecoder(column, page.columnMetaData().codec(),
                            context.decompressorFactory()).decode(page.pageData(), page.dictionary()));
                }
                records.addAll(mode == NestedIndexMode.REAL_VIEW ? chunk.realViewRecords(layers) : chunk.levelRecords());
            }
        }
        finally {
            inputFile.close();
        }
        return records;
    }

    /// A column chunk's decoded pages, concatenated: every level entry's levels and its value
    /// rendered, `null` at an entry below the maximum definition level.
    private static final class ReferenceChunk {

        private final int maxDefinitionLevel;
        private final boolean repeated;
        private final List<Integer> definitionLevels = new ArrayList<>();
        private final List<Integer> repetitionLevels = new ArrayList<>();
        private final List<String> values = new ArrayList<>();

        ReferenceChunk(ColumnSchema column) {
            this.maxDefinitionLevel = column.maxDefinitionLevel();
            this.repeated = column.maxRepetitionLevel() > 0;
        }

        void add(ReferencePageDecoder.DecodedPage page) {
            for (int i = 0; i < page.numValues(); i++) {
                int def = page.definitionLevels() != null ? page.definitionLevels()[i] : maxDefinitionLevel;
                definitionLevels.add(def);
                repetitionLevels.add(page.repetitionLevels() != null ? page.repetitionLevels()[i] : 0);
                values.add(def == maxDefinitionLevel ? value(page.values(), i) : null);
            }
        }

        /// Where each record starts among the entries, the entry count after the last.
        private int[] recordStarts() {
            List<Integer> starts = new ArrayList<>();
            for (int i = 0; i < repetitionLevels.size(); i++) {
                if (!repeated || repetitionLevels.get(i) == 0) {
                    starts.add(i);
                }
            }
            starts.add(repetitionLevels.size());
            return starts.stream().mapToInt(Integer::intValue).toArray();
        }

        List<String> levelRecords() {
            int[] starts = recordStarts();
            List<String> records = new ArrayList<>();
            for (int r = 0; r + 1 < starts.length; r++) {
                StringBuilder record = new StringBuilder();
                for (int v = starts[r]; v < starts[r + 1]; v++) {
                    levelEntry(record, definitionLevels.get(v), repetitionLevels.get(v), values.get(v));
                }
                records.add(record.toString());
            }
            return records;
        }

        List<String> realViewRecords(NestedLevelComputer.Layers layers) {
            int[] starts = recordStarts();
            int count = definitionLevels.size();
            int[] def = definitionLevels.stream().mapToInt(Integer::intValue).toArray();
            int[] rep = repetitionLevels.stream().mapToInt(Integer::intValue).toArray();
            NestedLevelComputer.RealView view = NestedLevelComputer.computeRealView(def, repeated ? rep : null, count,
                    starts.length - 1, maxDefinitionLevel, layers);
            int[] recordOffsets = Arrays.copyOf(starts, starts.length - 1);
            int[] map = view.realToRawLeaf();
            return ScheduledNestedColumnTest.realRecords(view, layers, recordOffsets, starts.length - 1,
                    leaf -> values.get(map != null ? map[leaf] : leaf));
        }
    }

    // ==================== Rendering ====================

    /// The batches' records, one string each: the record's levels and values where the batch keeps
    /// its levels, its real-items view where it keeps only that. A fixed-width batch of
    /// fixed-size lists keeps no levels; its records show the levels they imply: every value
    /// present, `0` at a record's start and `1` after.
    private static List<String> records(List<NestedBatch> batches, int maxDefinitionLevel,
                                        NestedLevelComputer.Layers layers) {
        List<String> records = new ArrayList<>();
        for (NestedBatch batch : batches) {
            if (batch.realView != null) {
                Object realValues = batch.realValues;
                records.addAll(realRecords(batch.realView, layers, batch.recordOffsets, batch.recordCount,
                        leaf -> value(realValues, leaf)));
                continue;
            }
            for (int r = 0; r < batch.recordCount; r++) {
                int from = batch.recordOffsets[r];
                int to = r + 1 < batch.recordCount ? batch.recordOffsets[r + 1] : batch.valueCount;
                StringBuilder record = new StringBuilder();
                for (int v = from; v < to; v++) {
                    int def = batch.definitionLevels != null ? batch.definitionLevels[v] : maxDefinitionLevel;
                    int rep = batch.definitionLevels != null ? batch.repetitionLevels[v] : v == from ? 0 : 1;
                    levelEntry(record, def, rep, def == maxDefinitionLevel ? value(batch.values, v) : null);
                }
                records.add(record.toString());
            }
        }
        return records;
    }

    private static void levelEntry(StringBuilder record, int def, int rep, String value) {
        record.append(" [").append(def).append('/').append(rep).append(' ').append(value).append(']');
    }

    /// The records of a real-items view, each as its nested structure: a struct as `{…}`, a list
    /// as `[…]`, an absent item as `null`, and each present leaf as its value and, after `@`,
    /// where its value lies among the record's values.
    private static List<String> realRecords(NestedLevelComputer.RealView view, NestedLevelComputer.Layers layers,
                                            int[] recordOffsets, int recordCount, IntFunction<String> leafValue) {
        List<String> records = new ArrayList<>();
        for (int r = 0; r < recordCount; r++) {
            StringBuilder record = new StringBuilder();
            realItem(record, view, layers, 0, r, recordOffsets[r], leafValue);
            records.add(record.toString());
        }
        return records;
    }

    private static void realItem(StringBuilder out, NestedLevelComputer.RealView view,
                                 NestedLevelComputer.Layers layers, int layer, int slot, int recordOffset,
                                 IntFunction<String> leafValue) {
        if (layer == layers.count()) {
            if (!present(view.leafValidity(), slot)) {
                out.append("null");
                return;
            }
            int raw = view.realToRawLeaf() != null ? view.realToRawLeaf()[slot] : slot;
            out.append(leafValue.apply(slot)).append('@').append(raw - recordOffset);
            return;
        }
        if (!present(view.layerValidity()[layer], slot)) {
            out.append("null");
            return;
        }
        if (layers.kinds()[layer] == LayerKind.STRUCT) {
            out.append('{');
            realItem(out, view, layers, layer + 1, slot, recordOffset, leafValue);
            out.append('}');
            return;
        }
        int[] offsets = view.layerOffsets()[layer];
        out.append('[');
        for (int child = offsets[slot]; child < offsets[slot + 1]; child++) {
            if (child > offsets[slot]) {
                out.append(", ");
            }
            realItem(out, view, layers, layer + 1, child, recordOffset, leafValue);
        }
        out.append(']');
    }

    private static boolean present(long[] validity, int slot) {
        return validity == null || (validity[slot >>> 6] & (1L << slot)) != 0;
    }

    private static String value(Object values, int i) {
        return switch (values) {
            case int[] a -> Integer.toString(a[i]);
            case long[] a -> Long.toString(a[i]);
            case float[] a -> Float.toString(a[i]);
            case double[] a -> Double.toString(a[i]);
            case boolean[] a -> Boolean.toString(a[i]);
            case byte[][] a -> Arrays.toString(a[i]);
            case BinaryBatchValues b -> Arrays.toString(b.byteArrayAt(i));
            default -> throw new IllegalStateException("Unexpected values " + values);
        };
    }

    /// Checks what a batch's arrays must satisfy whatever its capacity: they hold exactly its
    /// values and records, and an all-present batch holds no null.
    private static void assertWellFormed(NestedBatch actual, int maxDefinitionLevel, String what) {
        if (actual.allPresent && actual.definitionLevels != null) {
            assertThat(actual.definitionLevels).as(what + " all present levels").containsOnly(maxDefinitionLevel);
        }
        if (actual.definitionLevels != null) {
            assertThat(actual.definitionLevels).as(what + " definition level array").hasSize(actual.valueCount);
            assertThat(actual.repetitionLevels).as(what + " repetition level array").hasSize(actual.valueCount);
        }
        assertThat(actual.recordOffsets).as(what + " record offset array").hasSize(actual.recordCount);
    }
}
