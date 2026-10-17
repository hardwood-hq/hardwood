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
import java.util.stream.Stream;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.InputFile;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// A [ScheduledNestedColumn] publishes the batches a [NestedColumnWorker] publishes for the same
/// column, batch plan and index mode: the same records, values, levels, offsets and index, with
/// every array allocated at the size its batch holds.
///
/// The fixtures cover lists, maps and structs, pages beginning inside a record, dictionary and
/// plain binary leaves, and fixed-size lists, read at batch capacities that put batch edges
/// inside pages and pages inside batches, and under a row cap.
class ScheduledNestedColumnTest {

    private static final String RESOURCES = "src/test/resources/";

    static Stream<Arguments> reads() {
        List<Arguments> reads = new ArrayList<>();
        for (String fixture : List.of("nested_v1_mid_record_pages.parquet", "nested_v1_no_index.parquet",
                "list_of_list_of_struct_test.parquet", "map_with_list_value_test.parquet",
                "nested_binary_test.parquet", "nested_dict_batch_boundary.parquet", "dict_nested_repeats.parquet",
                "deep_nested_struct_test.parquet", "fixed_size_list_k4_v1.parquet", "fixed_size_list_k4_leadfast_v2.parquet",
                "misaligned_pages_nested_v2.parquet")) {
            for (NestedColumnWorker.IndexMode mode : NestedColumnWorker.IndexMode.values()) {
                for (int batchSize : new int[]{ 1, 3, 64 }) {
                    reads.add(Arguments.of(fixture, mode, batchSize, 0));
                }
                reads.add(Arguments.of(fixture, mode, 3, 7));
            }
        }
        return reads.stream();
    }

    @ParameterizedTest(name = "{0} {1} batch {2} cap {3}")
    @MethodSource("reads")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void publishesTheBatchesOfTheColumnWorker(String fixture, NestedColumnWorker.IndexMode mode, int batchSize,
                                              int maxRows) throws Exception {
        Path file = Path.of(RESOURCES + fixture);
        int columns;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            columns = reader.getFileSchema().getColumnCount();
        }
        for (int c = 0; c < columns; c++) {
            List<NestedBatch> expected = read(file, c, mode, batchSize, maxRows, false);
            List<NestedBatch> actual = read(file, c, mode, batchSize, maxRows, true);

            assertThat(actual).as("column %d batches", c).hasSameSizeAs(expected);
            for (int b = 0; b < expected.size(); b++) {
                assertSameBatch(actual.get(b), expected.get(b), "column " + c + " batch " + b);
            }
        }
    }

    /// Reads one column of `file` through a scheduled column, or through a column worker, with
    /// the fixed-size-list fast path on, ending at `maxRows` rows unless it is `0`.
    private static List<NestedBatch> read(Path file, int columnIndex, NestedColumnWorker.IndexMode mode,
                                          int batchSize, long maxRows, boolean scheduled) throws Exception {
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
            ColumnPipeline pipeline;
            if (scheduled) {
                ReadScheduler scheduler = new ReadScheduler(context, batchSize, batchSize);
                pipeline = new ScheduledNestedColumn(scheduler, pageSource, exchange, column, batchSize,
                        context.decompressorFactory(), layers, mode, true);
                scheduler.start();
            }
            else {
                NestedColumnWorker worker = new NestedColumnWorker(pageSource, exchange, column, batchSize,
                        context.decompressorFactory(), context.executor(), layers, mode, true);
                worker.start();
                pipeline = worker;
            }
            List<NestedBatch> batches = new ArrayList<>();
            NestedBatch batch;
            while ((batch = exchange.poll()) != null && batch.recordCount > 0) {
                batches.add(batch);
            }
            exchange.checkError();
            pipeline.close();
            iterator.close();
            inputFile.close();
            return batches;
        }
    }

    private static void assertSameBatch(NestedBatch actual, NestedBatch expected, String what) {
        assertThat(actual.recordCount).as(what + " records").isEqualTo(expected.recordCount);
        assertThat(actual.valueCount).as(what + " values").isEqualTo(expected.valueCount);
        assertThat(actual.fixedListK).as(what + " k").isEqualTo(expected.fixedListK);
        // The worker counts the page whose first record start closes a batch towards that batch's
        // presence, even when none of its values is in the batch; a cell counts its own pages only.
        if (expected.allPresent) {
            assertThat(actual.allPresent).as(what + " all present").isTrue();
        }
        if (actual.allPresent && actual.definitionLevels != null) {
            int maxDefinitionLevel = Arrays.stream(actual.definitionLevels).max().orElse(0);
            assertThat(actual.definitionLevels).as(what + " all present levels").containsOnly(maxDefinitionLevel);
        }
        assertThat(actual.fileName).as(what + " file").isEqualTo(expected.fileName);
        assertThat(actual.filterAlwaysMatches).as(what + " always matches").isEqualTo(expected.filterAlwaysMatches);
        assertThat(actual.definitionLevels).as(what + " definition levels").isEqualTo(expected.definitionLevels);
        assertThat(actual.repetitionLevels).as(what + " repetition levels").isEqualTo(expected.repetitionLevels);
        assertThat(actual.recordOffsets).as(what + " record offsets").isEqualTo(expected.recordOffsets);
        assertThat(actual.elementValidity).as(what + " element validity").isEqualTo(expected.elementValidity);
        assertThat(actual.multiLevelOffsets).as(what + " layer offsets").isEqualTo(expected.multiLevelOffsets);
        // A leaf compaction consumes a binary batch's values, leaving only its real values to read
        if (expected.realView == null || expected.realView.realToRawLeaf() == null
                || !(expected.values instanceof BinaryBatchValues)) {
            assertSameValues(actual.values, expected.values, actual.valueCount, what + " values");
        }
        if (expected.realView == null) {
            assertThat(actual.realView).as(what + " real view").isNull();
        }
        else {
            NestedLevelComputer.RealView actualView = actual.realView;
            NestedLevelComputer.RealView expectedView = expected.realView;
            assertThat(actualView.layerOffsets()).as(what + " real offsets").isEqualTo(expectedView.layerOffsets());
            assertThat(actualView.layerValidity()).as(what + " real validity").isEqualTo(expectedView.layerValidity());
            assertThat(actualView.leafValidity()).as(what + " leaf validity").isEqualTo(expectedView.leafValidity());
            assertThat(actualView.realToRawLeaf()).as(what + " leaf map").isEqualTo(expectedView.realToRawLeaf());
            assertSameValues(actual.realValues, expected.realValues, expectedView.valueCount(), what + " real values");
        }
        if (actual.definitionLevels != null) {
            assertThat(actual.definitionLevels).as(what + " definition level array").hasSize(actual.valueCount);
            assertThat(actual.repetitionLevels).as(what + " repetition level array").hasSize(actual.valueCount);
        }
        assertThat(actual.recordOffsets).as(what + " record offset array").hasSize(actual.recordCount);
    }

    /// Compares the first `count` values; the scheduled column's value array holds exactly the
    /// batch's values.
    private static void assertSameValues(Object actual, Object expected, int count, String what) {
        if (expected instanceof BinaryBatchValues expectedBinary) {
            BinaryBatchValues actualBinary = (BinaryBatchValues) actual;
            assertThat(actualBinary.capacity()).as(what + " capacity").isGreaterThanOrEqualTo(count);
            for (int i = 0; i < count; i++) {
                assertThat(actualBinary.byteArrayAt(i)).as(what + " value %d", i).isEqualTo(expectedBinary.byteArrayAt(i));
            }
            if (expectedBinary.dictionary != null) {
                assertThat(actualBinary.dictionary.size()).as(what + " dictionary").isEqualTo(expectedBinary.dictionary.size());
                for (int i = 0; i < count; i++) {
                    assertThat(actualBinary.dictIndices[i]).as(what + " id %d", i).isEqualTo(expectedBinary.dictIndices[i]);
                }
            }
            return;
        }
        assertThat(actual).as(what).isEqualTo(expected);
    }
}
