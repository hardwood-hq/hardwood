/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;

import static org.assertj.core.api.Assertions.assertThat;

/// Asserts that `ParquetModel.readPreviewPage` uses the row-based seek
/// primitive — a jump to a row in the last row group reads only that
/// row group's bytes, not the whole prefix.
class DataPreviewIoTest {

    /// 3 row groups of 100 rows each: id 1..100, 101..200, 201..300.
    private static Path fixture() {
        return Path.of(DataPreviewIoTest.class.getResource("/filter_pushdown_int.parquet").getPath());
    }

    @Test
    void jumpToLastPageYieldsTheExpectedRows() throws IOException {
        ByteCountingInputFile counting = new ByteCountingInputFile(InputFile.of(fixture()));

        try (ParquetModel model = ParquetModel.open(counting, "filter_pushdown_int.parquet")) {
            // Jump to the last 30 rows (270..299, ids 271..300). With
            // firstRow-based seeking the model opens RG 2 only.
            AtomicInteger rowsYielded = new AtomicInteger();
            AtomicLong firstId = new AtomicLong(-1);
            AtomicLong lastId = new AtomicLong(-1);
            model.readPreviewPage(270, 30, reader -> {
                long id = reader.getLong("id");
                if (firstId.get() < 0) {
                    firstId.set(id);
                }
                lastId.set(id);
                rowsYielded.incrementAndGet();
            });

            assertThat(rowsYielded.get()).as("rows yielded on last-page jump").isEqualTo(30);
            assertThat(firstId.get()).as("first id on last-page jump").isEqualTo(271L);
            assertThat(lastId.get()).as("last id on last-page jump").isEqualTo(300L);
        }
    }

    @Test
    void jumpToLastRowGroupSkipsEarlierRowGroupBytes() throws IOException {
        ByteCountingInputFile counting = new ByteCountingInputFile(InputFile.of(fixture()));

        try (ParquetModel model = ParquetModel.open(counting, "filter_pushdown_int.parquet")) {
            long openBytes = counting.bytesRead();

            model.readPreviewPage(270, 30, reader -> { });
            long jumpDelta = counting.bytesRead() - openBytes;
            long fileSize = counting.length();

            assertThat(jumpDelta).as("bytes read on jump").isPositive();
            // One RG of bytes is well under half the file's data-page
            // bytes for a 3-RG fixture.
            assertThat(jumpDelta).as("bytes read on RG-2 jump vs file size")
                    .isLessThan(fileSize / 2);
        }
    }

    @Test
    void forwardWithinSameRowGroupReusesCursor() throws IOException {
        ByteCountingInputFile counting = new ByteCountingInputFile(InputFile.of(fixture()));

        try (ParquetModel model = ParquetModel.open(counting, "filter_pushdown_int.parquet")) {
            // First page: lands in RG 0, opens it.
            model.readPreviewPage(0, 10, reader -> { });
            long after1 = counting.bytesRead();

            // Continue forward within the same RG. The cursor is reused
            // (no firstRow-rebuild); whatever extra bytes are read are
            // page-by-page top-ups, not a fresh RG fetch.
            model.readPreviewPage(50, 10, reader -> { });
            long after2 = counting.bytesRead();

            long step = after2 - after1;
            assertThat(step).as("bytes for forward intra-RG step")
                    .isLessThanOrEqualTo(after1);
        }
    }

    @Test
    void backwardSeekRebuildsCursor() throws IOException {
        // Backward navigation closes the cursor and reopens via
        // firstRow(target). Bytes are re-fetched for the target RG (no
        // model-level row cache yet — that's #377). This test pins the
        // re-fetch is bounded to the current RG, not the prefix from row 0.
        ByteCountingInputFile counting = new ByteCountingInputFile(InputFile.of(fixture()));

        try (ParquetModel model = ParquetModel.open(counting, "filter_pushdown_int.parquet")) {
            // Walk into RG 2, near the end.
            model.readPreviewPage(290, 10, reader -> { });
            long afterEnd = counting.bytesRead();

            // Backward within the same RG.
            AtomicInteger rowsYielded = new AtomicInteger();
            AtomicLong firstId = new AtomicLong(-1);
            model.readPreviewPage(220, 10, reader -> {
                long id = reader.getLong("id");
                if (firstId.get() < 0) {
                    firstId.set(id);
                }
                rowsYielded.incrementAndGet();
            });
            long backDelta = counting.bytesRead() - afterEnd;

            assertThat(rowsYielded.get()).as("rows yielded on backward seek").isEqualTo(10);
            assertThat(firstId.get()).as("first id on backward seek").isEqualTo(221L);
            // Re-fetch bounded by current RG, not full prefix.
            assertThat(backDelta).as("backward intra-RG re-fetch is bounded")
                    .isLessThanOrEqualTo(afterEnd);
        }
    }

    /// Tracks total bytes returned by [#readRange]. The Parquet footer
    /// fetch and the data-page fetches both flow through it.
    private static final class ByteCountingInputFile implements InputFile {

        private final InputFile delegate;
        private final AtomicLong bytesRead = new AtomicLong();

        ByteCountingInputFile(InputFile delegate) {
            this.delegate = delegate;
        }

        long bytesRead() {
            return bytesRead.get();
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            bytesRead.addAndGet(length);
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
