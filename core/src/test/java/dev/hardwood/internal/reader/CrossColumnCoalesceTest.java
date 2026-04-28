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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// Verifies cross-column ranged-GET coalescing within a row group (#374).
///
/// Adjacent column chunks within a row group are typically stored
/// back-to-back on disk, so a single ranged GET can cover many columns
/// at once. The pipeline coalesces such reads into [SharedRegion]s in
/// `RowGroupIterator.coalesceAcrossColumns`. These tests assert the
/// observable effect: the underlying `readRange` count drops compared to
/// what a per-column-per-chunk fetch path would produce.
class CrossColumnCoalesceTest {

    /// 20-column flat schema, single row group, no OffsetIndex →
    /// SequentialFetchPlan path. 20 leaf columns are stored back-to-back
    /// (zero gaps), all within ~2 KB.
    private static final Path SEQ_FILE = Path.of("src/test/resources/yellow_tripdata_sample.parquet");

    /// 3-column flat schema, single row group, OffsetIndex present →
    /// IndexedFetchPlan path. Columns are stored back-to-back.
    private static final Path INDEXED_FILE = Path.of("src/test/resources/page_index_test.parquet");

    @Test
    void sequentialPlanCoalescesFirstChunkOfEveryColumn() throws Exception {
        CountingInputFile countingFile = new CountingInputFile(InputFile.of(SEQ_FILE));
        countingFile.open();
        try (ParquetFileReader reader = ParquetFileReader.open(countingFile)) {
            int readsBefore = countingFile.readCount();
            try (RowReader rows = reader.rowReader()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
            int readsForData = countingFile.readCount() - readsBefore;
            // The fixture has 20 leaf columns; a per-column fetch would
            // issue 20 readRange calls. With cross-column coalescing,
            // they collapse into one (all columns fit in <2 KB,
            // back-to-back).
            assertThat(readsForData)
                    .as("Data fetches should coalesce to a single ranged GET (was %d)", readsForData)
                    .isEqualTo(1);
        }
    }

    @Test
    void indexedPlanCoalescesFirstChunkOfEveryColumn() throws Exception {
        CountingInputFile countingFile = new CountingInputFile(InputFile.of(INDEXED_FILE));
        countingFile.open();
        try (ParquetFileReader reader = ParquetFileReader.open(countingFile)) {
            int readsBefore = countingFile.readCount();
            try (RowReader rows = reader.rowReader()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
            int readsForData = countingFile.readCount() - readsBefore;
            // 3 columns, all back-to-back, no filter → all 3 first
            // chunks coalesce into one region. The pipeline also fetches
            // the per-RG index region (OffsetIndex / ColumnIndex), so
            // expect at most 2 reads (1 region + 1 index buffer).
            assertThat(readsForData)
                    .as("Data fetches should coalesce to ≤ 2 ranged GETs (was %d)", readsForData)
                    .isLessThanOrEqualTo(2);
        }
    }

    @Test
    void filteredColumnIsNotOverCoalesced() throws Exception {
        // With a selective filter, the IndexedFetchPlan for `id` has
        // page drops → multiple page groups → isCoalesceSafe() == false.
        // That keeps `id` out of the shared region; the filtered read
        // must transfer strictly fewer bytes than the unfiltered one.
        // (If page-dropped columns were swept into the shared region,
        // the dropped bytes would be re-fetched and bytes-read would
        // meet or exceed the unfiltered total.)
        FilterPredicate selective = FilterPredicate.lt("id", 1000L);

        long filteredBytes = readBytesWith(INDEXED_FILE, selective);
        long unfilteredBytes = readBytesWith(INDEXED_FILE, null);

        assertThat(filteredBytes)
                .as("Filtered read must not over-fetch dropped pages via the shared region")
                .isLessThan(unfilteredBytes);
    }

    private static long readBytesWith(Path file, FilterPredicate filter) throws Exception {
        ByteCountingInputFile counter = new ByteCountingInputFile(InputFile.of(file));
        counter.open();
        try (ParquetFileReader reader = ParquetFileReader.open(counter);
                RowReader rows = (filter == null
                        ? reader.buildRowReader()
                                .projection(ColumnProjection.columns("id", "value", "category"))
                                .build()
                        : reader.buildRowReader()
                                .projection(ColumnProjection.columns("id", "value", "category"))
                                .filter(filter).build())) {
            while (rows.hasNext()) {
                rows.next();
            }
        }
        return counter.bytesRead();
    }

    /// Tracks both call count and total bytes read per `readRange`.
    private static class ByteCountingInputFile implements InputFile {

        private final InputFile delegate;
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicLong bytes = new AtomicLong();

        ByteCountingInputFile(InputFile delegate) {
            this.delegate = delegate;
        }

        long bytesRead() {
            return bytes.get();
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            calls.incrementAndGet();
            bytes.addAndGet(length);
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
