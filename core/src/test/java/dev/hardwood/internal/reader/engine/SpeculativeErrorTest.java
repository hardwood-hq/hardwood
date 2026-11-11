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
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// An `Error` a request or a footer read ahead of the read fails with is the JVM's and no row's: the
/// read throws it at once, before the row it was met at, and its close throws nothing.
///
/// Uses diff_order_multi.parquet: four row groups of 50 rows, whose data are requests of their own.
class SpeculativeErrorTest {

    private static final Path FILE = Path.of("src/test/resources/differential/diff_order_multi.parquet");
    private static final ColumnProjection ROW = ColumnProjection.columns("__row__");

    @ParameterizedTest(name = "threads: {0}")
    @ValueSource(ints = {0, 2})
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void anErrorReadAheadFailsTheReadAtOnce(int threads) throws Exception {
        LastRowGroupErrorFile file = new LastRowGroupErrorFile(InputFile.of(FILE));
        try (HardwoodContextImpl context = HardwoodContextImpl.create(threads)) {
            RowGroupIterator iterator = RowGroupIterators.initialized(List.of(file), context, ROW, null, 0);
            BatchExchange<BatchExchange.Batch> exchange = start(iterator);
            iterator.awaitQuiescent();
            assertThat(file.failed.getCount()).as("the last row group's request failed").isZero();

            // fewer than the 150 rows before the last row group
            assertThat(rowsUntilThrown(exchange, file.error)).isLessThan(150);
            iterator.close();
        }
    }

    @ParameterizedTest(name = "threads: {0}")
    @ValueSource(ints = {0, 2})
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void anErrorOfAFooterReadAheadFailsTheReadAtOnce(int threads) throws Exception {
        FooterErrorFile third = new FooterErrorFile(InputFile.of(FILE));
        try (HardwoodContextImpl context = HardwoodContextImpl.create(threads)) {
            RowGroupIterator iterator = RowGroupIterators.initialized(
                    List.of(InputFile.of(FILE), InputFile.of(FILE), third), context, ROW, null, 0);
            BatchExchange<BatchExchange.Batch> exchange = start(iterator);
            iterator.awaitQuiescent();
            assertThat(third.failed.getCount()).as("the third file's footer read failed").isZero();

            // fewer than the 400 rows of the files before the third
            assertThat(rowsUntilThrown(exchange, third.error)).isLessThan(400);
            iterator.close();
        }
    }

    /// Starts a read of the row number in batches of 10 rows, a read-ahead of one row.
    private static BatchExchange<BatchExchange.Batch> start(RowGroupIterator iterator) throws IOException {
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            schema = reader.getFileSchema();
        }
        BatchExchange<BatchExchange.Batch> exchange = BatchExchange.detachingFlat("__row__", 10);
        List<NestedLevelComputer.Layers> flat = new ArrayList<>();
        flat.add(null);
        iterator.start(ProjectedSchema.create(schema, ROW), flat, NestedIndexMode.ALL_ITEMS, false, 10, 1,
                List.of(exchange), null);
        return exchange;
    }

    /// The rows taken from `exchange` before it throws `error`.
    private static long rowsUntilThrown(BatchExchange<BatchExchange.Batch> exchange, Error error) {
        long[] rows = new long[1];
        assertThatThrownBy(() -> {
            while (true) {
                rows[0] += takeBatch(exchange);
            }
        }).isSameAs(error);
        return rows[0];
    }

    private static int takeBatch(BatchExchange<BatchExchange.Batch> exchange) throws Exception {
        BatchExchange.Batch batch = exchange.poll();
        if (batch == null) {
            exchange.checkError();
            return 0;
        }
        return batch.recordCount;
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
