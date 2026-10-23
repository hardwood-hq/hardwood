/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.InputFile;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A [ScheduledNestedColumn] fills its cells in tasks of the decode pool only, never in its fetch
/// steps, which run on the pool too: also the cell a fetch step seals at the end of the stream, and
/// the open cell it seals at its failure between two work items, once the page tasks have placed
/// every page. It queues a [SealedFillTask] only for cells its fetch steps seal, none for the cells
/// its page tasks seal.
@Tag("pool")
class NestedFillThreadTest {

    private static final Path FILE = Path.of("src/test/resources/nested_v1_no_index.parquet");
    /// `tags.list.element`, a column with repetition in a file without an offset index: its pages
    /// give no record count, so its last cell is open until the stream ends.
    private static final int COLUMN = 1;
    private static final int RECORDS = 2000;

    /// Threads that filled a cell off the decode pool or inside a fetch step.
    private final ConcurrentLinkedQueue<String> offPool = new ConcurrentLinkedQueue<>();
    private final AtomicInteger fills = new AtomicInteger();
    private final AtomicInteger sealedFillTasks = new AtomicInteger();

    @BeforeEach
    void observeFills() {
        ScheduledNestedColumn.fillObserver = (column, cell) -> {
            fills.incrementAndGet();
            Thread thread = Thread.currentThread();
            if (thread.isVirtual() || !thread.getName().startsWith("hardwood-") || inFetchStep()) {
                offPool.add(thread.toString());
            }
        };
        ReadScheduler.submissionObserver = (column, page, inFlight, lowestRow, readPublishedRow) -> {
            if (page instanceof SealedFillTask) {
                sealedFillTasks.incrementAndGet();
            }
        };
    }

    @AfterEach
    void stopObserving() {
        ScheduledNestedColumn.fillObserver = null;
        ReadScheduler.submissionObserver = null;
    }

    @ParameterizedTest(name = "batch {0}")
    @ValueSource(ints = { 1, 7, 64, 1 << 20 })
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void fillsEveryCellOnThePool(int batchSize) throws Exception {
        assertThat(read(batchSize, End.STREAM)).isEqualTo(RECORDS);
        assertThat(fills).hasPositiveValue();
        assertThat(offPool).isEmpty();
        // The file has one row group, so the fetch steps seal cells only at the end of the stream
        assertThat(sealedFillTasks).as("sealed fill tasks").hasValueLessThanOrEqualTo(1);
    }

    /// The fetch step reaches the end of the stream only once every page is placed, so its
    /// placement seals the open cell and no page task is left to fill it.
    @ParameterizedTest(name = "batch {0}")
    @ValueSource(ints = { 7, 1 << 20 })
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void fillsTheCellSealedAtTheEndOfTheStreamOnThePool(int batchSize) throws Exception {
        assertThat(read(batchSize, End.STREAM_AFTER_TASKS)).isEqualTo(RECORDS);
        assertThat(fills).hasPositiveValue();
        assertThat(offPool).isEmpty();
        assertThat(sealedFillTasks).as("sealed fill tasks").hasValue(1);
    }

    /// The fetch step fails between two work items only once every page is placed, so its
    /// placement seals the open cell, which is published before the failure is raised.
    @ParameterizedTest(name = "batch {0}")
    @ValueSource(ints = { 7, 1 << 20 })
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void fillsTheCellSealedAtAFetchFailureOnThePool(int batchSize) {
        assertThatThrownBy(() -> read(batchSize, End.FAILURE_AFTER_TASKS))
                .isInstanceOf(IOException.class)
                .hasMessage("simulated transport failure");
        assertThat(fills).hasPositiveValue();
        assertThat(offPool).isEmpty();
        assertThat(sealedFillTasks).as("sealed fill tasks").hasValue(1);
    }

    private enum End {
        /// The stream ends as the file's pages do.
        STREAM,
        /// The stream ends once every page is placed.
        STREAM_AFTER_TASKS,
        /// The fetch step fails, between two work items, once every page is placed.
        FAILURE_AFTER_TASKS
    }

    /// Reads the column through a scheduled column on a two-thread context: the fetch step that
    /// waits for the page tasks to place every page holds one thread, and they run on the other.
    ///
    /// @return the records published before the end of the stream
    private static int read(int batchSize, End end) throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2);
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            FileSchema schema = reader.getFileSchema();
            ColumnSchema column = schema.getColumn(COLUMN);
            InputFile inputFile = InputFile.of(FILE);
            inputFile.open();
            RowGroupIterator iterator = RowGroupIterators.standalone(List.of(inputFile), context);
            iterator.setFirstFile(schema, reader.getFileMetaData().rowGroups());
            iterator.initialize(ReadProjection.of(ProjectedSchema.create(schema, ColumnProjection.all())), null, true);
            iterator.planBatches(batchSize, 0);
            NestedLevelComputer.Layers layers = NestedLevelComputer.computeLayers(schema)[COLUMN];
            BatchExchange<NestedBatch> exchange = BatchExchange.detachingNested(column.name(), batchSize);
            AtomicReference<ScheduledNestedColumn> scheduledColumn = new AtomicReference<>();
            PageSource pageSource = new PageSource(iterator, COLUMN) {

                @Override
                public PageInfo next() throws IOException {
                    PageInfo page = super.next();
                    if (page == null && end != End.STREAM) {
                        awaitPlaced(scheduledColumn.get());
                        if (end == End.FAILURE_AFTER_TASKS) {
                            throw new IOException("simulated transport failure");
                        }
                    }
                    return page;
                }
            };
            ReadScheduler scheduler = new ReadScheduler(iterator.readExecutor(), batchSize, batchSize);
            ScheduledNestedColumn scheduled = new ScheduledNestedColumn(scheduler, pageSource, exchange, column,
                    batchSize, context.decompressorFactory(), layers, NestedIndexMode.ALL_ITEMS, true);
            scheduledColumn.set(scheduled);
            scheduler.start();
            int records = 0;
            try {
                NestedBatch batch;
                while ((batch = exchange.poll()) != null && batch.recordCount > 0) {
                    records += batch.recordCount;
                }
                if (end == End.FAILURE_AFTER_TASKS) {
                    assertThat(records).as("records published before the failure").isEqualTo(RECORDS);
                }
                exchange.checkError();
            }
            finally {
                scheduled.close();
                iterator.close();
                inputFile.close();
            }
            return records;
        }
    }

    /// Whether the calling thread is running a column's fetch step.
    private static boolean inFetchStep() {
        return StackWalker.getInstance().walk(frames -> frames.anyMatch(
                frame -> frame.getMethodName().equals("runFetchStep")));
    }

    /// Waits until the column's page tasks have placed every page the fetch steps queued, or the
    /// column is done, so that a failed page task fails the read instead of hanging it.
    private static void awaitPlaced(ScheduledNestedColumn column) {
        while (true) {
            column.lock.lock();
            try {
                if (column.isDone() || column.unplacedRow() == Long.MAX_VALUE) {
                    return;
                }
            }
            finally {
                column.lock.unlock();
            }
            Thread.onSpinWait();
        }
    }
}
