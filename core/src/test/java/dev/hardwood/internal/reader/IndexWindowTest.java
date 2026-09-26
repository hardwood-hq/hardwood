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
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Window formation, fetching and release, on the row groups of page_index_windows.parquet
/// (twelve row groups, twenty columns, a page index).
class IndexWindowTest {

    private static final Path FIXTURE = Path.of("src/test/resources/page_index_windows.parquet");
    private static final int COLUMNS = 20;

    @Test
    void windowsTakeAsManyRowGroupsAsTheBudgetHolds() throws Exception {
        List<RowGroup> rowGroups = rowGroups();
        // Every column's OffsetIndex: one row group's slices are contiguous, and so are
        // consecutive row groups', so a window fetches the sum of its members' OffsetIndexes.
        // Their sizes differ by a few bytes, so any three fit a budget of three of the largest
        // and no four do.
        long largest = 0;
        long smallest = Long.MAX_VALUE;
        for (RowGroup rowGroup : rowGroups) {
            largest = Math.max(largest, offsetIndexBytes(rowGroup));
            smallest = Math.min(smallest, offsetIndexBytes(rowGroup));
        }
        long budget = 3 * largest;
        assertThat(4 * smallest).isGreaterThan(budget);

        List<IndexWindow> windows = plan(budget, rowGroups, allColumns(), new BitSet());

        assertThat(windows).extracting(IndexWindow::memberCount).containsExactly(3, 3, 3, 3);
        assertThat(windows).allSatisfy(window -> assertThat(window.fetchedBytes())
                .isLessThanOrEqualTo(budget));
    }

    @Test
    void aRowGroupLargerThanTheBudgetIsAWindowOfItsOwn() throws Exception {
        List<RowGroup> rowGroups = rowGroups();
        List<IndexWindow> windows = plan(1, rowGroups, allColumns(), new BitSet());

        assertThat(windows).hasSize(12).allSatisfy(window -> assertThat(window.memberCount()).isOne());
    }

    @Test
    void aFileWhoseIndexFitsTheBudgetIsOneWindow() throws Exception {
        List<IndexWindow> windows = plan(16L * 1024 * 1024, rowGroups(), allColumns(), allColumns());

        assertThat(windows).extracting(IndexWindow::memberCount).containsExactly(12);
    }

    @Test
    void theFirstRequestFetchesTheWholeWindowOncePerStructure() throws Exception {
        CountingInputFile file = countingFile(InputFile.of(FIXTURE));
        IndexWindow window = window(file, 3, column(0), column(0));

        RowGroupIndexBuffers second = window.buffersFor(1);
        RowGroupIndexBuffers first = window.buffersFor(0);

        assertThat(file.readCount()).isEqualTo(2);
        assertThat(first.forColumn(0).offsetIndex()).isNotNull();
        assertThat(first.forColumn(0).columnIndex()).isNotNull();
        assertThat(second.forColumn(0).offsetIndex()).isNotNull();
        assertThat(file.reads()).extracting(CountingInputFile.Read::reason)
                .containsOnly("rg=0-2 indexes");
    }

    @Test
    void concurrentRequestsFetchTheWindowOnce() throws Exception {
        CountDownLatch bothAsked = new CountDownLatch(2);
        // The fetch waits until both threads have asked, so the second asks while it runs.
        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE)) {
            @Override
            public ByteBuffer readRange(long offset, int length) throws IOException {
                try {
                    bothAsked.await();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
                return super.readRange(offset, length);
            }
        };
        file.open();
        IndexWindow window = window(file, 2, column(0), new BitSet());

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<RowGroupIndexBuffers> a = executor.submit(() -> {
                bothAsked.countDown();
                return window.buffersFor(0);
            });
            Future<RowGroupIndexBuffers> b = executor.submit(() -> {
                bothAsked.countDown();
                return window.buffersFor(1);
            });
            assertThat(a.get().forColumn(0).offsetIndex()).isNotNull();
            assertThat(b.get().forColumn(0).offsetIndex()).isNotNull();
        }
        finally {
            executor.shutdownNow();
        }
        assertThat(file.readCount()).isEqualTo(1);
    }

    @Test
    void aFailedFetchIsRepeatedByTheNextRequest() throws Exception {
        AtomicBoolean fail = new AtomicBoolean(true);
        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE)) {
            @Override
            public ByteBuffer readRange(long offset, int length) throws IOException {
                if (fail.getAndSet(false)) {
                    throw new IOException("injected");
                }
                return super.readRange(offset, length);
            }
        };
        file.open();
        IndexWindow window = window(file, 2, column(0), new BitSet());

        assertThatThrownBy(() -> window.buffersFor(0))
                .isInstanceOf(IOException.class)
                .hasMessage("injected");
        RowGroupIndexBuffers buffers = window.buffersFor(0);

        // The failed attempt threw before reaching the file; the second one read it.
        assertThat(buffers.forColumn(0).offsetIndex()).isNotNull();
        assertThat(file.readCount()).isEqualTo(1);
    }

    @Test
    void aReleasedMemberIsNotServed() throws Exception {
        IndexWindow window = window(countingFile(InputFile.of(FIXTURE)), 2, column(0), new BitSet());
        window.buffersFor(0);

        window.release(0);
        window.release(0);

        assertThatThrownBy(() -> window.buffersFor(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Page index of work item 0 requested after its release");
        assertThat(window.buffersFor(1).forColumn(0).offsetIndex()).isNotNull();

        window.releaseAll();

        assertThatThrownBy(() -> window.buffersFor(1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Page index of work item 1 requested after its release");
    }

    @Test
    void aWorkItemOutsideTheWindowIsRejected() throws Exception {
        IndexWindow window = window(countingFile(InputFile.of(FIXTURE)), 2, column(0), new BitSet());

        assertThatThrownBy(() -> window.buffersFor(2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Work item 2 is not in the index window of work items 0 to 1");
    }

    /// One window over the first `count` row groups of the fixture.
    private static IndexWindow window(InputFile file, int count, BitSet offsetIndexColumns,
            BitSet columnIndexColumns) throws Exception {
        List<RowGroup> members = rowGroups().subList(0, count);
        return IndexWindow.plan(Long.MAX_VALUE, file, 0, members.toArray(new RowGroup[0]),
                indexes(count), repeat(offsetIndexColumns, count), repeat(columnIndexColumns, count))
                .getFirst();
    }

    private static List<IndexWindow> plan(long budget, List<RowGroup> rowGroups,
            BitSet offsetIndexColumns, BitSet columnIndexColumns) throws Exception {
        int count = rowGroups.size();
        return IndexWindow.plan(budget, countingFile(InputFile.of(FIXTURE)), 0,
                rowGroups.toArray(new RowGroup[0]), indexes(count), repeat(offsetIndexColumns, count),
                repeat(columnIndexColumns, count));
    }

    private static long offsetIndexBytes(RowGroup rowGroup) {
        long bytes = 0;
        for (ColumnChunk chunk : rowGroup.columns()) {
            bytes += chunk.offsetIndexLength();
        }
        return bytes;
    }

    private static BitSet allColumns() {
        BitSet columns = new BitSet();
        columns.set(0, COLUMNS);
        return columns;
    }

    private static BitSet column(int column) {
        BitSet columns = new BitSet();
        columns.set(column);
        return columns;
    }

    private static int[] indexes(int count) {
        int[] indexes = new int[count];
        Arrays.setAll(indexes, i -> i);
        return indexes;
    }

    private static BitSet[] repeat(BitSet columns, int count) {
        BitSet[] repeated = new BitSet[count];
        Arrays.fill(repeated, columns);
        return repeated;
    }

    private static List<RowGroup> rowGroups() throws Exception {
        CountingInputFile file = countingFile(InputFile.of(FIXTURE));
        return ParquetMetadataReader.readMetadata(file).rowGroups();
    }

    private static CountingInputFile countingFile(InputFile delegate) throws IOException {
        CountingInputFile file = new CountingInputFile(delegate);
        file.open();
        return file;
    }
}
