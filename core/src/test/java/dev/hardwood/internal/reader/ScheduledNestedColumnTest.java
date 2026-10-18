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

/// A [ScheduledNestedColumn] publishes the same records whatever its batch capacity: read at a
/// capacity that puts batch edges inside pages and pages inside batches, record by record its
/// batches hold the levels and values of a read whose batches end only at row groups, every array
/// allocated at the size its batch holds.
///
/// The fixtures cover lists, maps and structs, pages beginning inside a record, dictionary and
/// plain binary leaves, and fixed-size lists, read in every index mode and under a row cap.
class ScheduledNestedColumnTest {

    private static final String RESOURCES = "src/test/resources/";

    /// A batch capacity no fixture's row group reaches, so that its batches end at row groups.
    private static final int WHOLE_ROW_GROUPS = 1 << 20;

    static Stream<Arguments> reads() {
        List<Arguments> reads = new ArrayList<>();
        for (String fixture : List.of("nested_v1_mid_record_pages.parquet", "nested_v1_no_index.parquet",
                "list_of_list_of_struct_test.parquet", "map_with_list_value_test.parquet",
                "nested_binary_test.parquet", "nested_dict_batch_boundary.parquet", "dict_nested_repeats.parquet",
                "deep_nested_struct_test.parquet", "fixed_size_list_k4_v1.parquet", "fixed_size_list_k4_leadfast_v2.parquet",
                "misaligned_pages_nested_v2.parquet")) {
            for (NestedIndexMode mode : NestedIndexMode.values()) {
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
    void publishesTheSameRecordsAtEveryBatchCapacity(String fixture, NestedIndexMode mode, int batchSize,
                                                     int maxRows) throws Exception {
        Path file = Path.of(RESOURCES + fixture);
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            schema = reader.getFileSchema();
        }
        for (int c = 0; c < schema.getColumnCount(); c++) {
            int maxDefinitionLevel = schema.getColumn(c).maxDefinitionLevel();
            List<NestedBatch> batches = read(file, c, mode, batchSize, maxRows);
            List<NestedBatch> whole = read(file, c, mode, WHOLE_ROW_GROUPS, maxRows);
            for (int b = 0; b < batches.size(); b++) {
                assertWellFormed(batches.get(b), "column " + c + " batch " + b);
            }
            assertThat(records(batches, maxDefinitionLevel)).as("column %d records", c)
                    .isEqualTo(records(whole, maxDefinitionLevel));
            assertThat(realValues(batches)).as("column %d real values", c).isEqualTo(realValues(whole));
        }
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

    /// The batches' records, one string each: the record's levels and values where the batch
    /// keeps them, its record alone where it keeps only the real-items view. A fixed-width batch
    /// of fixed-size lists keeps no levels; its records show the levels they imply: every value
    /// present, `0` at a record's start and `1` after.
    private static List<String> records(List<NestedBatch> batches, int maxDefinitionLevel) {
        List<String> records = new ArrayList<>();
        for (NestedBatch batch : batches) {
            for (int r = 0; r < batch.recordCount; r++) {
                if (batch.realView != null) {
                    records.add("record");
                    continue;
                }
                int from = batch.recordOffsets[r];
                int to = r + 1 < batch.recordCount ? batch.recordOffsets[r + 1] : batch.valueCount;
                StringBuilder record = new StringBuilder();
                for (int v = from; v < to; v++) {
                    record.append(" [");
                    if (batch.definitionLevels != null) {
                        record.append(batch.definitionLevels[v]).append('/').append(batch.repetitionLevels[v]);
                    }
                    else if (batch.fixedListK > 0) {
                        record.append(maxDefinitionLevel).append('/').append(v == from ? 0 : 1);
                    }
                    record.append(' ').append(value(batch.values, v)).append(']');
                }
                records.add(record.toString());
            }
        }
        return records;
    }

    /// The real values of the batches that keep the real-items view, in order.
    private static List<String> realValues(List<NestedBatch> batches) {
        List<String> values = new ArrayList<>();
        for (NestedBatch batch : batches) {
            if (batch.realView != null) {
                for (int v = 0; v < batch.realView.valueCount(); v++) {
                    values.add(value(batch.realValues != null ? batch.realValues : batch.values, v));
                }
            }
        }
        return values;
    }

    private static String value(Object values, int i) {
        return switch (values) {
            case int[] a -> Integer.toString(a[i]);
            case long[] a -> Long.toString(a[i]);
            case float[] a -> Float.toString(a[i]);
            case double[] a -> Double.toString(a[i]);
            case boolean[] a -> Boolean.toString(a[i]);
            case BinaryBatchValues b -> Arrays.toString(b.byteArrayAt(i));
            default -> throw new IllegalStateException("Unexpected values " + values);
        };
    }

    /// Checks what a batch's arrays must satisfy whatever its capacity: they hold exactly its
    /// values and records, and an all-present batch holds no null.
    private static void assertWellFormed(NestedBatch actual, String what) {
        if (actual.allPresent && actual.definitionLevels != null) {
            int maxDefinitionLevel = Arrays.stream(actual.definitionLevels).max().orElse(0);
            assertThat(actual.definitionLevels).as(what + " all present levels").containsOnly(maxDefinitionLevel);
        }
        if (actual.definitionLevels != null) {
            assertThat(actual.definitionLevels).as(what + " definition level array").hasSize(actual.valueCount);
            assertThat(actual.repetitionLevels).as(what + " repetition level array").hasSize(actual.valueCount);
        }
        assertThat(actual.recordOffsets).as(what + " record offset array").hasSize(actual.recordCount);
    }
}
