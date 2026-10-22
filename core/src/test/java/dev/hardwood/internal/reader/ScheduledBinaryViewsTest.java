/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InputFile;
import dev.hardwood.internal.predicate.ColumnBatchMatcher;
import dev.hardwood.internal.predicate.DictionaryBinaryBatchMatcher;
import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate.Comparison;
import dev.hardwood.internal.predicate.matcher.binaries.BinaryEqBatchMatcher;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// The views of a scheduled binary column's dictionary values are built when a reader first needs
/// them: a reader of entry ids alone never gets them, nor their arrays; a filter that decides by
/// entry ids leaves them deferred, and one that reads every value's view has them built as each
/// cell completes. The first reader that builds deferred views switches the column to building
/// them as its cells complete.
///
/// The fixture has two row groups of 100 dictionary-encoded strings, read in batches of 10.
class ScheduledBinaryViewsTest {

    private static final Path FILE = Path.of("src/test/resources/dict_cross_chunk.parquet");

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aReaderOfIdsOnlyNeverGetsViews() throws Exception {
        List<BinaryBatchValues> batches = read(false, null);

        assertThat(batches).hasSize(20);
        assertThat(batches).allSatisfy(values -> {
            assertThat(values.viewArraysAllocated()).isFalse();
            assertThat(values.dictionary).isNotNull();
        });
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aDictionaryAwareFilterLeavesViewsDeferred() throws Exception {
        byte[] literal = "x".getBytes(StandardCharsets.UTF_8);

        List<BinaryBatchValues> dictionaryAware = read(false, new DictionaryBinaryBatchMatcher(
                new BinaryEqBatchMatcher(literal, Comparison.BYTE_STRING)));
        List<BinaryBatchValues> viewReading = read(false, new BinaryEqBatchMatcher(literal, Comparison.BYTE_STRING));

        assertThat(dictionaryAware).hasSize(20);
        assertThat(dictionaryAware).allSatisfy(values -> assertThat(values.viewArraysAllocated()).isFalse());
        assertThat(viewReading).hasSize(20);
        assertThat(viewReading).allSatisfy(values -> assertThat(values.hasPendingViews()).isFalse());
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aReaderOfViewsSwitchesTheColumnToBuildingThem() throws Exception {
        BinaryBatchValues.ViewDemand demand = new BinaryBatchValues.ViewDemand();
        List<BinaryBatchValues> batches = read(true, null, false, demand);

        assertThat(batches).hasSize(20);
        assertThat(demand.requested).as("the reader's views raised the column's demand").isTrue();
        // Deferred, built and eager batches alike carry each value's bytes.
        for (BinaryBatchValues values : batches) {
            int[] starts = values.starts();
            int[] ends = values.ends();
            for (int i = 0; i < 10; i++) {
                assertThat(new String(values.buffer(i), starts[i], ends[i] - starts[i], StandardCharsets.UTF_8))
                        .isEqualTo(values.stringAt(i));
            }
        }
    }

    /// A column whose views a reader asked for builds them as each cell completes. The demand is
    /// raised before the read starts: which cells complete before a reader's first demand mid-read
    /// is up to the decode threads.
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aColumnAskedForViewsBuildsThemAsItsCellsComplete() throws Exception {
        List<BinaryBatchValues> batches = read(false, null, true, new BinaryBatchValues.ViewDemand());

        assertThat(batches).hasSize(20);
        assertThat(batches).allSatisfy(values -> assertThat(values.hasPendingViews()).isFalse());
    }

    private static List<BinaryBatchValues> read(boolean readViews, ColumnBatchMatcher filter) throws Exception {
        return read(readViews, filter, false, new BinaryBatchValues.ViewDemand());
    }

    /// Reads the fixture's column through a [ScheduledColumn] in batches of 10, building the views
    /// of the first batch if `readViews`, with the column's view demand raised before the read
    /// if `requestViews`. `demand` receives the column's demand as the read ends.
    private static List<BinaryBatchValues> read(boolean readViews, ColumnBatchMatcher filter, boolean requestViews,
                                                BinaryBatchValues.ViewDemand demand) throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            FileSchema schema = reader.getFileSchema();
            ColumnSchema column = schema.getColumn(0);
            InputFile inputFile = InputFile.of(FILE);
            inputFile.open();
            RowGroupIterator iterator = new RowGroupIterator(List.of(inputFile), context, 0);
            iterator.setFirstFile(schema, reader.getFileMetaData().rowGroups());
            iterator.initialize(ProjectedSchema.create(schema, ColumnProjection.all()), null);
            iterator.planBatches(10, 0);

            BatchExchange<BatchExchange.Batch> exchange = BatchExchange.detaching(
                    column.name(), () -> {
                        BatchExchange.Batch b = new BatchExchange.Batch();
                        b.matches = new long[1];
                        return b;
                    }, batch -> batch.capacity, 10);
            ReadScheduler scheduler = new ReadScheduler(iterator.readExecutor(), 10, 10);
            ScheduledFlatColumn scheduled = new ScheduledFlatColumn(scheduler, new PageSource(iterator, 0), exchange, column,
                    10, context.decompressorFactory(), filter);
            if (requestViews) {
                scheduled.viewDemand().requested = true;
            }
            scheduler.start();

            List<BinaryBatchValues> batches = new ArrayList<>();
            BatchExchange.Batch batch;
            while ((batch = exchange.poll()) != null) {
                BinaryBatchValues values = (BinaryBatchValues) batch.values;
                if (readViews && batches.isEmpty()) {
                    values.ensureViews();
                }
                batches.add(values);
            }
            exchange.checkError();
            demand.requested = scheduled.viewDemand().requested;
            scheduled.close();
            iterator.close();
            inputFile.close();
            return batches;
        }
    }
}
