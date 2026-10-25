/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.plan.RowGroupIterators;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// An `Error` a request read ahead of the read fails with fails the read only where the read
/// reaches the request: a read that never reaches it reads on, and its close throws the `Error`; a
/// read that reaches it fails there, after the batches before it. An `Error` a footer read ahead
/// fails with is thrown by the close of a read that never lists its file.
///
/// Uses diff_order_multi.parquet: four row groups of 50 rows, whose data are requests of their own.
class SpeculativeErrorTest {

    private static final Path FILE = Path.of("src/test/resources/differential/diff_order_multi.parquet");

    /// A read-ahead of a few rows keeps the column's walk in the first row groups, while the
    /// request of the last one is read ahead.
    @ParameterizedTest(name = "threads: {0}")
    @ValueSource(ints = {0, 2})
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void anErrorReadAheadThatTheReadNeverReachesIsThrownByTheClose(int threads) throws Exception {
        LastRowGroupErrorFile file = new LastRowGroupErrorFile(InputFile.of(FILE));
        ColumnProjection projection = ColumnProjection.columns("__row__");
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            schema = reader.getFileSchema();
        }
        ProjectedSchema decoded = ProjectedSchema.create(schema, projection);
        List<Integer> batches = new ArrayList<>();
        try (HardwoodContextImpl context = HardwoodContextImpl.create(threads)) {
            RowGroupIterator iterator = RowGroupIterators.initialized(List.of(file), context, projection, null, 0);
            BatchExchange<BatchExchange.Batch> exchange = BatchExchange.detachingFlat("__row__", 10);
            List<NestedLevelComputer.Layers> flat = new ArrayList<>();
            flat.add(null);
            iterator.start(decoded, flat, NestedIndexMode.ALL_ITEMS, false, 10, 1, 0, List.of(exchange), null);
            batches.add(takeBatch(exchange));
            iterator.awaitQuiescent();
            assertThat(file.failed.getCount()).as("the last row group's request failed").isZero();
            batches.add(takeBatch(exchange));

            assertThatThrownBy(iterator::close).isSameAs(file.error);
        }
        assertThat(batches).containsExactly(10, 10);
    }

    /// The footer of a file ahead is read ahead of the read; an `Error` its read fails with is
    /// thrown by the close of a read that never lists the file.
    @ParameterizedTest(name = "threads: {0}")
    @ValueSource(ints = {0, 2})
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void anErrorOfAFooterReadAheadThatTheReadNeverListsIsThrownByTheClose(int threads) throws Exception {
        FooterErrorFile third = new FooterErrorFile(InputFile.of(FILE));
        ColumnProjection projection = ColumnProjection.columns("__row__");
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            schema = reader.getFileSchema();
        }
        ProjectedSchema decoded = ProjectedSchema.create(schema, projection);
        try (HardwoodContextImpl context = HardwoodContextImpl.create(threads)) {
            RowGroupIterator iterator = RowGroupIterators.initialized(
                    List.of(InputFile.of(FILE), InputFile.of(FILE), third), context, projection, null, 0);
            BatchExchange<BatchExchange.Batch> exchange = BatchExchange.detachingFlat("__row__", 10);
            List<NestedLevelComputer.Layers> flat = new ArrayList<>();
            flat.add(null);
            iterator.start(decoded, flat, NestedIndexMode.ALL_ITEMS, false, 10, 1, 0, List.of(exchange), null);
            assertThat(takeBatch(exchange)).isEqualTo(10);
            iterator.awaitQuiescent();
            assertThat(third.failed.getCount()).as("the third file's footer read failed").isZero();

            assertThatThrownBy(iterator::close).isSameAs(third.error);
        }
    }

    private static int takeBatch(BatchExchange<BatchExchange.Batch> exchange) throws Exception {
        BatchExchange.Batch batch = exchange.poll();
        if (batch == null) {
            exchange.checkError();
            return 0;
        }
        return batch.recordCount;
    }

    @ParameterizedTest(name = "threads: {0}")
    @ValueSource(ints = {0, 2})
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void anErrorReadAheadFailsTheReadWhereItReachesTheRequest(int threads) throws Exception {
        LastRowGroupErrorFile file = new LastRowGroupErrorFile(InputFile.of(FILE));
        List<Integer> batches = new ArrayList<>();
        try (HardwoodContextImpl context = HardwoodContextImpl.create(threads);
             ParquetFileReader reader = ParquetFileReader.open(file, context);
             ColumnReader column = reader.buildColumnReader("__row__").batchSize(50).build()) {
            assertThatThrownBy(() -> {
                while (column.nextBatch()) {
                    batches.add(column.getRecordCount());
                }
            }).isSameAs(file.error);
        }
        assertThat(batches).containsExactly(50, 50, 50);
    }

    /// Fails every read of the last row group's data with an `Error`. Not a file whose bytes are
    /// in memory, so the data are requests read on virtual threads.
    private static final class LastRowGroupErrorFile implements InputFile {

        private final InputFile delegate;
        final Error error = new InjectedError();
        final CountDownLatch failed = new CountDownLatch(1);

        LastRowGroupErrorFile(InputFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            String reason = FetchReason.current();
            if (reason.startsWith("rg=3 ") && reason.contains(" col=")) {
                failed.countDown();
                throw error;
            }
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /// Fails every read with an `Error`, its footer's first. Not a file whose bytes are in memory,
    /// so the read reads its footer ahead.
    private static final class FooterErrorFile implements InputFile {

        private final InputFile delegate;
        final Error error = new InjectedError();
        final CountDownLatch failed = new CountDownLatch(1);

        FooterErrorFile(InputFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) {
            failed.countDown();
            throw error;
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class InjectedError extends Error {

        InjectedError() {
            super("injected");
        }
    }
}
