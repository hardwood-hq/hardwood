/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Every row group prefetches the next one, including a row group whose plans the previous
/// prefetch already computed, and no further than the next one (#1332).
class NextRowGroupPrefetchTest {

    /// Four row groups of 50 rows, with a page index, so every plan is an [IndexedFetchPlan].
    private static final Path FILE = Path.of("src/test/resources/differential/diff_order_multi.parquet");
    private static final int ROW_GROUPS = 4;

    @Test
    void everyRowGroupAfterTheFirstIsPrefetched() throws Exception {
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
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            RowGroupIterator iterator = new RowGroupIterator(List.of(file), context, 0);
            try {
                iterator.setFirstFile(schema, metaData.rowGroups());
                iterator.initialize(ProjectedSchema.create(schema, ColumnProjection.columns("__row__")), null);

                // The demand accesses come one row group at a time, each only once the
                // previous one's prefetch has issued its read, so a row group is always
                // planned by the prefetch before the demand path reaches it.
                for (int rg = 0; rg < ROW_GROUPS - 1; rg++) {
                    iterator.getColumnPlan(iterator.workItemAt(rg), 0);
                    assertThat(signals.awaitPrefetch(rg + 1))
                            .as("prefetch of row group %d after the demand access to row group %d", rg + 1, rg)
                            .isTrue();
                    assertNoDataReadPast(file, rg + 1);
                }
            }
            finally {
                iterator.close();
            }
        }

        List<CountingInputFile.Read> reads = file.reads();
        for (int rg = 1; rg < ROW_GROUPS; rg++) {
            assertThat(firstDataRead(reads, rg).reason())
                    .as("first data read of row group %d", rg)
                    .startsWith("prefetch rg=" + rg + " | rg=" + rg + " ");
        }
    }

    /// Asserts, once the prefetch tasks have run, that no row group past `rowGroup` has had a
    /// data read: the prefetch of a row group does not cascade into the one after it.
    private static void assertNoDataReadPast(CountingInputFile file, int rowGroup) {
        assertThat(ForkJoinPool.commonPool().awaitQuiescence(5, TimeUnit.SECONDS)).isTrue();
        for (int later = rowGroup + 1; later < ROW_GROUPS; later++) {
            int laterRowGroup = later;
            assertThat(file.reads())
                    .as("data reads of row group %d once row group %d is prefetched", laterRowGroup, rowGroup)
                    .noneMatch(read -> isDataRead(read.reason(), laterRowGroup));
        }
    }

    private static CountingInputFile.Read firstDataRead(List<CountingInputFile.Read> reads, int rowGroup) {
        return reads.stream()
                .filter(read -> isDataRead(read.reason(), rowGroup))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No data read of row group " + rowGroup + " in " + reads));
    }

    /// Whether `reason` is a read of a column chunk (dictionary, page group or shared region) of
    /// `rowGroup`, whether issued on demand or by a prefetch.
    private static boolean isDataRead(String reason, int rowGroup) {
        String purpose = reason.substring(reason.lastIndexOf('|') + 1).strip();
        return purpose.startsWith("rg=" + rowGroup + " col=") || purpose.startsWith("rg=" + rowGroup + " region=");
    }

    /// Signals, per row group, the first data read a prefetch issues for it.
    private static final class PrefetchSignals implements InputFile {

        private final InputFile delegate;
        private final CountDownLatch[] prefetched = new CountDownLatch[ROW_GROUPS];

        PrefetchSignals(InputFile delegate) {
            this.delegate = delegate;
            for (int rg = 0; rg < ROW_GROUPS; rg++) {
                prefetched[rg] = new CountDownLatch(1);
            }
        }

        /// Waits at most five seconds for a prefetch of `rowGroup` to issue a data read.
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
                if (reason.startsWith("prefetch rg=" + rg + " |") && isDataRead(reason, rg)) {
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
