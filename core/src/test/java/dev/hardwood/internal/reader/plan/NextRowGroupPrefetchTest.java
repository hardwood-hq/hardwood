/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.reader.fetch.FetchSequence;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Every row group prefetches the next one, including a row group whose plans the previous
/// prefetch already computed, and no further than the next one (#1332).
class NextRowGroupPrefetchTest {

    /// Four row groups of 50 rows, with a page index, so every plan is an
    /// [dev.hardwood.internal.reader.fetch.IndexedFetchPlan].
    private static final Path FILE = Path.of("src/test/resources/differential/diff_order_multi.parquet");
    private static final int ROW_GROUPS = 4;

    /// With no room for speculative data (F of zero), entering each row group still starts the
    /// first section request of the next one, the read's next unit, and nothing past it.
    @Test
    void theNextUnitStartsWithoutFetchAheadRoom() throws Exception {
        System.setProperty(FetchSequence.FETCH_AHEAD_BYTES_PROPERTY, "0");
        try {
            readRowGroupByRowGroup();
        }
        finally {
            System.clearProperty(FetchSequence.FETCH_AHEAD_BYTES_PROPERTY);
        }
    }

    private static void readRowGroupByRowGroup() throws Exception {
        FileMetaData metaData;
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            metaData = reader.getFileMetaData();
            schema = reader.getFileSchema();
        }
        assertThat(metaData.rowGroups()).hasSize(ROW_GROUPS);

        PrefetchSignals signals = new PrefetchSignals(InputFile.of(FILE));
        CountingInputFile file = new CountingInputFile(signals);
        file.open();
        QuiescentExecutor speculative = new QuiescentExecutor();
        try (HardwoodContextImpl context = HardwoodContextImpl.withIoExecutor(Runtime.getRuntime().availableProcessors(), speculative)) {
            RowGroupIterator iterator = RowGroupIterators.standalone(List.of(file), context);
            try {
                iterator.setFirstFile(schema, metaData.rowGroups());
                iterator.initialize(ReadProjection.of(
                        ProjectedSchema.create(schema, ColumnProjection.columns("__row__"))), null, true);

                // The demand accesses come one row group at a time, each only once the next unit
                // of the previous one has issued its read, so a row group is always planned ahead
                // before the demand path reaches it. No column walks a plan, so every data read
                // is a next unit's.
                for (int rg = 0; rg < ROW_GROUPS - 1; rg++) {
                    iterator.getColumnPlan(iterator.workItemAt(rg), 0);
                    assertThat(signals.awaitPrefetch(rg + 1))
                            .as("next unit, row group %d, after the demand access to row group %d", rg + 1, rg)
                            .isTrue();
                    assertNoDataReadPast(file, speculative, rg + 1);
                }
            }
            finally {
                iterator.close();
            }
        }

        List<CountingInputFile.Read> reads = file.reads();
        for (int rg = 1; rg < ROW_GROUPS; rg++) {
            int rowGroup = rg;
            assertThat(reads).filteredOn(read -> isDataRead(read.reason(), rowGroup))
                    .as("data reads of row group %d", rg)
                    .singleElement()
                    .satisfies(read -> assertThat(read.reason()).isEqualTo("rg=" + rowGroup + " col=__row__"));
        }
    }

    /// Asserts, once the speculative tasks have run, that no row group past `rowGroup` has had a
    /// data read: with no room for speculative data, the read starts nothing past its next unit.
    private static void assertNoDataReadPast(CountingInputFile file, QuiescentExecutor speculative, int rowGroup)
            throws InterruptedException {
        assertThat(speculative.awaitQuiescence(5, TimeUnit.SECONDS)).isTrue();
        for (int later = rowGroup + 1; later < ROW_GROUPS; later++) {
            int laterRowGroup = later;
            assertThat(file.reads())
                    .as("data reads of row group %d once row group %d is prefetched", laterRowGroup, rowGroup)
                    .noneMatch(read -> isDataRead(read.reason(), laterRowGroup));
        }
    }

    /// Whether `reason` is a data read of `rowGroup`.
    private static boolean isDataRead(String reason, int rowGroup) {
        return reason.startsWith("rg=" + rowGroup + " col=");
    }

    /// Signals, per row group, its first data read.
    private static final class PrefetchSignals implements InputFile {

        private final InputFile delegate;
        private final CountDownLatch[] prefetched = new CountDownLatch[ROW_GROUPS];

        PrefetchSignals(InputFile delegate) {
            this.delegate = delegate;
            for (int rg = 0; rg < ROW_GROUPS; rg++) {
                prefetched[rg] = new CountDownLatch(1);
            }
        }

        /// Waits at most five seconds for a data read of `rowGroup`.
        boolean awaitPrefetch(int rowGroup) throws InterruptedException {
            return prefetched[rowGroup].await(5, TimeUnit.SECONDS);
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            String reason = FetchReason.current();
            for (int rg = 0; rg < ROW_GROUPS; rg++) {
                if (isDataRead(reason, rg)) {
                    prefetched[rg].countDown();
                }
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
}
