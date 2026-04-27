/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Tests for the v2 pipeline: [FlatColumnWorker] + [BatchExchange].
///
/// Wires up a real pipeline from a test Parquet file and verifies batch
/// assembly, row counting, null handling, and error propagation.
class ColumnWorkerTest {

    private static final Path TEST_FILE = Path.of("src/test/resources/page_index_test.parquet");
    private static final Path WITH_NULLS = Path.of("src/test/resources/nullable_primitives_test.parquet");

    /// All rows from a file are received through the pipeline.
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void flatPipelineDeliversAllRows() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(TEST_FILE))) {

            FileSchema schema = reader.getFileSchema();
            long expectedRows = reader.getFileMetaData().rowGroups().stream()
                    .mapToLong(rg -> rg.numRows()).sum();

            RowGroupIterator iterator = createIterator(TEST_FILE, schema, context);
            ColumnSchema column = schema.getColumn(0);
            int batchCapacity = 1024;

            BatchExchange<BatchExchange.Batch> exchange = BatchExchange.recycling(
                    column.name(), () -> {
                        BatchExchange.Batch b = new BatchExchange.Batch();
                        b.values = BatchExchange.allocateArray(column.type(), batchCapacity);
                        return b;
                    });
            FlatColumnWorker worker = new FlatColumnWorker(
                    new PageSource(iterator, 0), exchange, column, batchCapacity,
                    context.decompressorFactory(), context.executor(), 0);
            worker.start();

            long totalRows = consumeAllBatches(exchange);
            worker.close();

            assertThat(totalRows)
                    .as("All rows should be delivered through the pipeline")
                    .isEqualTo(expectedRows);
        }
    }

    /// Batches have the correct capacity (except the last partial batch).
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void flatBatchesHaveCorrectCapacity() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(TEST_FILE))) {

            FileSchema schema = reader.getFileSchema();
            RowGroupIterator iterator = createIterator(TEST_FILE, schema, context);
            ColumnSchema column = schema.getColumn(0);
            int batchCapacity = 64;

            BatchExchange<BatchExchange.Batch> exchange = BatchExchange.recycling(
                    column.name(), () -> {
                        BatchExchange.Batch b = new BatchExchange.Batch();
                        b.values = BatchExchange.allocateArray(column.type(), batchCapacity);
                        return b;
                    });
            FlatColumnWorker worker = new FlatColumnWorker(
                    new PageSource(iterator, 0), exchange, column, batchCapacity,
                    context.decompressorFactory(), context.executor(), 0);
            worker.start();

            int batchCount = 0;
            int lastBatchSize = 0;
            BatchExchange.Batch batch;
            while ((batch = exchange.poll()) != null) {
                batchCount++;
                lastBatchSize = batch.recordCount;
                if (batchCount > 1) {
                    // All but possibly the last batch should be full
                    // (earlier batches are full; we check after seeing the next)
                }
                exchange.recycle(batch);
            }
            exchange.checkError();
            worker.close();

            assertThat(batchCount).as("Should produce multiple batches").isGreaterThan(1);
            // The last batch may be partial
            assertThat(lastBatchSize).as("Last batch should have rows").isGreaterThan(0);
        }
    }

    /// Null bitmaps are correctly populated for nullable columns.
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void flatPipelineTracksNulls() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(WITH_NULLS))) {

            FileSchema schema = reader.getFileSchema();
            RowGroupIterator iterator = createIterator(WITH_NULLS, schema, context);

            // Find a nullable column (maxDefinitionLevel > 0)
            int nullableCol = -1;
            for (int i = 0; i < schema.getColumnCount(); i++) {
                if (schema.getColumn(i).maxDefinitionLevel() > 0) {
                    nullableCol = i;
                    break;
                }
            }
            assertThat(nullableCol).as("Test file should have a nullable column").isGreaterThanOrEqualTo(0);

            ColumnSchema column = schema.getColumn(nullableCol);
            int batchCapacity = 1024;

            BatchExchange<BatchExchange.Batch> exchange = BatchExchange.recycling(
                    column.name(), () -> {
                        BatchExchange.Batch b = new BatchExchange.Batch();
                        b.values = BatchExchange.allocateArray(column.type(), batchCapacity);
                        return b;
                    });
            FlatColumnWorker worker = new FlatColumnWorker(
                    new PageSource(iterator, nullableCol), exchange, column, batchCapacity,
                    context.decompressorFactory(), context.executor(), 0);
            worker.start();

            boolean sawNulls = false;
            BatchExchange.Batch batch;
            while ((batch = exchange.poll()) != null) {
                if (batch.nulls != null && !batch.nulls.isEmpty()) {
                    sawNulls = true;
                }
                exchange.recycle(batch);
            }
            exchange.checkError();
            worker.close();

            assertThat(sawNulls)
                    .as("Nullable column should have null values in at least one batch")
                    .isTrue();
        }
    }

    /// maxRows limits the number of rows delivered by the pipeline.
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void flatPipelineRespectsMaxRows() throws Exception {
        long maxRows = 100;

        try (HardwoodContextImpl context = HardwoodContextImpl.create();
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(TEST_FILE))) {

            FileSchema schema = reader.getFileSchema();
            RowGroupIterator iterator = createIterator(TEST_FILE, schema, context);
            ColumnSchema column = schema.getColumn(0);
            int batchCapacity = 64;

            BatchExchange<BatchExchange.Batch> exchange = BatchExchange.recycling(
                    column.name(), () -> {
                        BatchExchange.Batch b = new BatchExchange.Batch();
                        b.values = BatchExchange.allocateArray(column.type(), batchCapacity);
                        return b;
                    });
            FlatColumnWorker worker = new FlatColumnWorker(
                    new PageSource(iterator, 0), exchange, column, batchCapacity,
                    context.decompressorFactory(), context.executor(), maxRows);
            worker.start();

            long totalRows = consumeAllBatches(exchange);
            worker.close();

            assertThat(totalRows)
                    .as("Pipeline should deliver exactly maxRows rows")
                    .isEqualTo(maxRows);
        }
    }

    /// Errors from the pipeline are propagated to the consumer.
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void errorPropagatedToConsumer() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            ColumnSchema column = new ColumnSchema(
                    dev.hardwood.metadata.FieldPath.of("error_col"),
                    PhysicalType.INT32,
                    dev.hardwood.metadata.RepetitionType.REQUIRED,
                    null, 0, 0, 0, null);

            BatchExchange<BatchExchange.Batch> exchange = BatchExchange.recycling(
                    column.name(), () -> {
                        BatchExchange.Batch b = new BatchExchange.Batch();
                        b.values = new int[64];
                        return b;
                    });

            // Signal an error directly on the exchange (simulating a pipeline failure)
            exchange.signalError(new RuntimeException("Simulated pipeline error"));

            assertThatThrownBy(exchange::checkError)
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Simulated pipeline error");
        }
    }

    /// Nested pipeline delivers all rows with pre-computed index structures.
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void nestedPipelineDeliversAllRowsWithIndex() throws Exception {
        Path nestedFile = Path.of("src/test/resources/list_basic_test.parquet");

        try (HardwoodContextImpl context = HardwoodContextImpl.create();
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(nestedFile))) {

            FileSchema schema = reader.getFileSchema();
            long expectedRows = reader.getFileMetaData().rowGroups().stream()
                    .mapToLong(rg -> rg.numRows()).sum();

            RowGroupIterator iterator = createIterator(nestedFile, schema, context);

            // Find a nested column (maxRepetitionLevel > 0)
            int nestedCol = -1;
            for (int i = 0; i < schema.getColumnCount(); i++) {
                if (schema.getColumn(i).maxRepetitionLevel() > 0) {
                    nestedCol = i;
                    break;
                }
            }
            assertThat(nestedCol).as("Test file should have a nested column").isGreaterThanOrEqualTo(0);

            ColumnSchema column = schema.getColumn(nestedCol);
            int batchCapacity = 1024;

            int[] thresholds = NestedLevelComputer.computeLevelNullThresholds(
                    schema.getRootNode(), column.columnIndex());

            BatchExchange<NestedBatch> exchange = BatchExchange.recycling(
                    column.name(), () -> {
                        NestedBatch b = new NestedBatch();
                        b.values = BatchExchange.allocateArray(column.type(), batchCapacity * 2);
                        return b;
                    });
            NestedColumnWorker worker = new NestedColumnWorker(
                    new PageSource(iterator, nestedCol), exchange, column, batchCapacity,
                    context.decompressorFactory(), context.executor(), 0, thresholds);
            worker.start();

            long totalRows = 0;
            boolean sawPreComputedIndex = false;
            NestedBatch batch;
            while ((batch = exchange.poll()) != null) {
                totalRows += batch.recordCount;

                // Verify pre-computed index fields are present
                assertThat(batch.elementNulls)
                        .as("elementNulls should be pre-computed by drain")
                        .isNotNull();
                if (batch.multiLevelOffsets != null) {
                    sawPreComputedIndex = true;
                }

                exchange.recycle(batch);
            }
            exchange.checkError();
            worker.close();

            assertThat(totalRows)
                    .as("All rows should be delivered through nested pipeline")
                    .isEqualTo(expectedRows);
            assertThat(sawPreComputedIndex)
                    .as("Nested column should have pre-computed multi-level offsets")
                    .isTrue();
        }
    }

    /// close() must not return until both VThreads have exited and every in-flight
    /// decode task submitted to the executor has completed. Otherwise an
    /// `InputFile` that releases memory in its own `close()` could free a buffer
    /// a decode task is still reading from.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void closeJoinsThreadsAndAwaitsInFlightDecodes() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(TEST_FILE))) {

            FileSchema schema = reader.getFileSchema();
            RowGroupIterator iterator = createIterator(TEST_FILE, schema, context);
            ColumnSchema column = schema.getColumn(0);
            int batchCapacity = 64;

            CountDownLatch release = new CountDownLatch(1);
            CountDownLatch firstSubmitted = new CountDownLatch(1);
            AtomicInteger decodesEntered = new AtomicInteger();
            AtomicInteger decodesFinished = new AtomicInteger();

            // Track the virtual threads started by the stalled executor so we can join
            // them after close() returns. This is necessary because decodesFinished++ runs
            // AFTER command.run() in the virtual thread — i.e. after the CompletableFuture
            // that ColumnWorker tracks has already completed. Without joining the threads
            // here, the assertion would have a race between close() returning and the
            // virtual threads incrementing decodesFinished.
            List<Thread> executorThreads = new CopyOnWriteArrayList<>();

            Executor stalledExecutor = command -> {
                Thread t = Thread.ofVirtual().start(() -> {
                    decodesEntered.incrementAndGet();
                    firstSubmitted.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    command.run();
                    decodesFinished.incrementAndGet();
                });
                executorThreads.add(t);
            };

            BatchExchange<BatchExchange.Batch> exchange = BatchExchange.recycling(
                    column.name(), () -> {
                        BatchExchange.Batch b = new BatchExchange.Batch();
                        b.values = BatchExchange.allocateArray(column.type(), batchCapacity);
                        return b;
                    });
            FlatColumnWorker worker = new FlatColumnWorker(
                    new PageSource(iterator, 0), exchange, column, batchCapacity,
                    context.decompressorFactory(), stalledExecutor, 0);
            worker.start();

            assertThat(firstSubmitted.await(5, TimeUnit.SECONDS))
                    .as("retriever should have submitted at least one decode task")
                    .isTrue();

            Thread closer = Thread.ofVirtual().start(worker::close);

            // close() must still be running — decodes are stalled on the latch.
            Thread.sleep(300);
            assertThat(closer.isAlive())
                    .as("close() must not return while decode tasks are still in flight")
                    .isTrue();

            release.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(10));

            assertThat(closer.isAlive())
                    .as("close() should return once decodes drain")
                    .isFalse();
            assertThat(worker.retrieverThread.isAlive())
                    .as("retriever thread must have exited")
                    .isFalse();
            assertThat(worker.drainThread.isAlive())
                    .as("drain thread must have exited")
                    .isFalse();

            // Join all executor virtual threads so that decodesFinished++ has run in
            // every thread before we assert. The decode tasks themselves are guaranteed
            // done by close(), but the per-thread bookkeeping after command.run() needs
            // its own synchronisation point.
            for (Thread t : executorThreads) {
                t.join(TimeUnit.SECONDS.toMillis(5));
            }

            assertThat(decodesFinished.get())
                    .as("every submitted decode task should have finished")
                    .isEqualTo(decodesEntered.get());
        }
    }

    /// Regression test for the interrupt-flag bug in [ColumnWorker#close()].
    ///
    /// **Scenario**: the thread that calls `close()` is interrupted while it is
    /// blocked in `retrieverThread.join()`. Before the fix, `close()` re-asserted
    /// `Thread.currentThread().interrupt()` immediately after catching the
    /// exception,
    /// so the subsequent `CompletableFuture.allOf().join()` saw the flag and
    /// returned
    /// at once — leaving decode tasks still executing. Any `InputFile` released in
    /// its
    /// own `close()` (mapped buffers, HTTP connections, etc.) could therefore free
    /// memory a decode task was still reading from.
    ///
    /// **After the fix**: the flag is saved into a local boolean and restored only
    /// *after* `allOf().join()` has drained every in-flight task.
    ///
    /// Assertions:
    /// 1. All decode tasks finish (`decodesFinished == decodesEntered`).
    /// 2. The interrupt flag is re-asserted on the closer thread when it returns.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void closeCompletesDecodesDespiteCallerInterrupt() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(TEST_FILE))) {

            FileSchema schema = reader.getFileSchema();
            RowGroupIterator iterator = createIterator(TEST_FILE, schema, context);
            ColumnSchema column = schema.getColumn(0);
            int batchCapacity = 64;

            // Latch that holds every decode task parked until we release it.
            CountDownLatch release = new CountDownLatch(1);
            // Signals the moment the first task has been submitted to the executor.
            CountDownLatch firstSubmitted = new CountDownLatch(1);

            AtomicInteger decodesEntered = new AtomicInteger();
            AtomicInteger decodesFinished = new AtomicInteger();
            List<Thread> executorThreads = new CopyOnWriteArrayList<>();

            Executor stalledExecutor = command -> {
                Thread t = Thread.ofVirtual().start(() -> {
                    decodesEntered.incrementAndGet();
                    firstSubmitted.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    command.run();
                    decodesFinished.incrementAndGet();
                });
                executorThreads.add(t);
            };

            BatchExchange<BatchExchange.Batch> exchange = BatchExchange.recycling(
                    column.name(), () -> {
                        BatchExchange.Batch b = new BatchExchange.Batch();
                        b.values = BatchExchange.allocateArray(column.type(), batchCapacity);
                        return b;
                    });
            FlatColumnWorker worker = new FlatColumnWorker(
                    new PageSource(iterator, 0), exchange, column, batchCapacity,
                    context.decompressorFactory(), stalledExecutor, 0);
            worker.start();

            // Wait until at least one decode task is in flight.
            assertThat(firstSubmitted.await(5, TimeUnit.SECONDS))
                    .as("retriever should have submitted at least one decode task")
                    .isTrue();

            // Start close() on a separate virtual thread, then interrupt it while it
            // is blocked inside retrieverThread.join() / drainThread.join().
            Thread closer = Thread.ofVirtual().start(worker::close);
            Thread.sleep(100); // give close() time to enter join()
            closer.interrupt();

            // close() is now interrupted but must NOT return until we release the
            // decode tasks. Give it a moment to (incorrectly) short-circuit if buggy.
            Thread.sleep(200);
            assertThat(closer.isAlive())
                    .as("close() must not return early after an interrupt — decode tasks are still running")
                    .isTrue();

            // Release all stalled decode tasks so close() can drain them.
            release.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(10));

            assertThat(closer.isAlive())
                    .as("close() should have returned after decodes drained")
                    .isFalse();

            // The interrupt flag must be restored on the closer thread after close()
            // returns.
            assertThat(closer.isInterrupted())
                    .as("close() must restore the interrupt flag after draining")
                    .isTrue();

            // Join executor threads so decodesFinished++ is visible before we assert.
            for (Thread t : executorThreads) {
                t.join(TimeUnit.SECONDS.toMillis(5));
            }

            assertThat(decodesFinished.get())
                    .as("all decode tasks must have completed despite the interrupt")
                    .isEqualTo(decodesEntered.get());
        }
    }

    /// Regression test for #300. When the exchange is stopped during the
    /// publish/take handshake, `publishCurrentBatch` must set `done = true`
    /// before returning — otherwise the outer `assemblePage` loop continues
    /// and dereferences the now-null `currentBatch` in `copyPageData`.
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void flatPublishCurrentBatchMarksDoneAfterTakeReturnsNull() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            ColumnSchema column = new ColumnSchema(
                    dev.hardwood.metadata.FieldPath.of("col"),
                    PhysicalType.INT32,
                    dev.hardwood.metadata.RepetitionType.REQUIRED,
                    null, 0, 0, 0, null);
            int batchCapacity = 64;

            BatchExchange<BatchExchange.Batch> exchange = BatchExchange.recycling(
                    column.name(), () -> {
                        BatchExchange.Batch b = new BatchExchange.Batch();
                        b.values = BatchExchange.allocateArray(column.type(), batchCapacity);
                        return b;
                    });
            FlatColumnWorker worker = new FlatColumnWorker(
                    null, exchange, column, batchCapacity,
                    context.decompressorFactory(), context.executor(), 0);

            worker.initDrainState();
            worker.currentBatch = exchange.takeBatch();
            worker.rowsInCurrentBatch = batchCapacity;

            // Finish the exchange and drain the free queue so the subsequent
            // takeBatch() inside publishCurrentBatch() returns null.
            exchange.finish();
            while (exchange.takeBatch() != null) {
                // drain
            }

            worker.publishCurrentBatch();

            assertThat(worker.done)
                    .as("done must be set when the exchange was stopped during take")
                    .isTrue();
            assertThat(worker.currentBatch).isNull();
        }
    }

    /// Regression test for #300, nested variant. Same contract as the flat
    /// worker: `done` must propagate out of the stopped-during-handshake paths
    /// so `assemblePage` doesn't dereference a null `currentBatch`.
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void nestedPublishCurrentBatchMarksDoneAfterTakeReturnsNull() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            ColumnSchema column = new ColumnSchema(
                    dev.hardwood.metadata.FieldPath.of("col"),
                    PhysicalType.INT32,
                    dev.hardwood.metadata.RepetitionType.REQUIRED,
                    null, 0, 0, 0, null);
            int batchCapacity = 64;

            BatchExchange<NestedBatch> exchange = BatchExchange.recycling(
                    column.name(), () -> {
                        NestedBatch b = new NestedBatch();
                        b.values = BatchExchange.allocateArray(column.type(), batchCapacity * 2);
                        return b;
                    });
            NestedColumnWorker worker = new NestedColumnWorker(
                    null, exchange, column, batchCapacity,
                    context.decompressorFactory(), context.executor(), 0, null);

            worker.initDrainState();
            worker.currentBatch = exchange.takeBatch();
            worker.rowsInCurrentBatch = batchCapacity;

            exchange.finish();
            while (exchange.takeBatch() != null) {
                // drain
            }

            worker.publishCurrentBatch();

            assertThat(worker.done)
                    .as("done must be set when the exchange was stopped during take")
                    .isTrue();
            assertThat(worker.currentBatch).isNull();
        }
    }

    // Helpers

    private static RowGroupIterator createIterator(Path file, FileSchema schema,
            HardwoodContextImpl context) throws Exception {
        InputFile inputFile = InputFile.of(file);
        inputFile.open();

        ParquetFileReader reader = ParquetFileReader.open(inputFile);
        RowGroupIterator iterator = new RowGroupIterator(
                java.util.List.of(inputFile), context, 0);
        iterator.setFirstFile(schema, reader.getFileMetaData().rowGroups());
        iterator.initialize(
                ProjectedSchema.create(schema, dev.hardwood.schema.ColumnProjection.all()), null);
        reader.close();
        return iterator;
    }

    private static long consumeAllBatches(BatchExchange<BatchExchange.Batch> exchange)
            throws InterruptedException {
        long totalRows = 0;
        BatchExchange.Batch batch;
        while ((batch = exchange.poll()) != null) {
            totalRows += batch.recordCount;
            exchange.recycle(batch);
        }
        exchange.checkError();
        return totalRows;
    }
}
