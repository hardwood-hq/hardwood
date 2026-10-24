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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.reader.fetch.FetchSequence;
import dev.hardwood.internal.reader.footer.ParquetMetadataReader;
import dev.hardwood.internal.reader.step.Need;
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

        RowGroupIndexBuffers second = buffers(window, 1);
        RowGroupIndexBuffers first = buffers(window, 0);

        assertThat(file.readCount()).isEqualTo(2);
        assertThat(first.forColumn(0).offsetIndex()).isNotNull();
        assertThat(first.forColumn(0).columnIndex()).isNotNull();
        assertThat(second.forColumn(0).offsetIndex()).isNotNull();
        assertThat(file.reads()).extracting(CountingInputFile.Read::reason)
                .containsOnly("rg=0-2 indexes");
    }

    @Test
    void aFailedReadFailsEveryMember() throws Exception {
        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE)) {
            @Override
            public ByteBuffer readRange(long offset, int length) throws IOException {
                throw new IOException("injected");
            }
        };
        file.open();
        IndexWindow window = window(file, 2, column(0), new BitSet());

        awaitMet(window.indexNeed());

        assertThatThrownBy(() -> window.buffersFor(0)).isInstanceOf(IOException.class)
                .rootCause().hasMessage("injected");
        assertThatThrownBy(() -> window.buffersFor(1)).isInstanceOf(IOException.class)
                .rootCause().hasMessage("injected");
    }

    @Test
    void aWindowIsDroppedOnceEveryMemberIsReleased() throws Exception {
        IndexWindow window = window(countingFile(InputFile.of(FIXTURE)), 2, column(0), new BitSet());
        buffers(window, 0);

        window.release(0);

        assertThat(window.buffersFor(1).forColumn(0).offsetIndex()).isNotNull();

        window.release(1);

        assertThatThrownBy(() -> window.buffersFor(1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Page index of work item 1 is not present: the window was not read, or was released");
    }

    @Test
    void closingTheReadDropsTheWindow() throws Exception {
        IndexWindow window = window(countingFile(InputFile.of(FIXTURE)), 2, column(0), new BitSet());
        buffers(window, 0);

        window.releaseAll();

        assertThatThrownBy(() -> window.buffersFor(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Page index of work item 0 is not present: the window was not read, or was released");
    }

    @Test
    void aWorkItemOutsideTheWindowIsRejected() throws Exception {
        IndexWindow window = window(countingFile(InputFile.of(FIXTURE)), 2, column(0), new BitSet());

        assertThatThrownBy(() -> window.buffersFor(2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Work item 2 is not in the index window of work items 0 to 1");
    }

    /// One window over the first `count` row groups of the fixture, its entries in a sequence that
    /// starts them on demand.
    private IndexWindow window(InputFile file, int count, BitSet offsetIndexColumns,
            BitSet columnIndexColumns) throws Exception {
        IndexWindow window = IndexWindow.plan(Long.MAX_VALUE, file, 0,
                members(rowGroups().subList(0, count), offsetIndexColumns, columnIndexColumns)).getFirst();
        window.insertEntries(FetchSequence.demandOnly(context));
        return window;
    }

    /// The index buffers of `workItem`, once the window's page index has arrived: each need names
    /// one request not present, so it is asked again until none is, as the planning chain does.
    private static RowGroupIndexBuffers buffers(IndexWindow window, int workItem) throws Exception {
        for (Need need = window.indexNeed(); need != null; need = window.indexNeed()) {
            awaitMet(need);
        }
        return window.buffersFor(workItem);
    }

    /// Waits until `need` is met; `null` is met already.
    static void awaitMet(Need need) throws InterruptedException {
        if (need != null) {
            CountDownLatch met = new CountDownLatch(1);
            need.whenMet(met::countDown);
            assertThat(met.await(10, TimeUnit.SECONDS)).as("need met").isTrue();
        }
    }

    @AfterEach
    void closeContext() {
        context.close();
    }

    private final HardwoodContextImpl context = HardwoodContextImpl.create(0);

    private static List<IndexWindow> plan(long budget, List<RowGroup> rowGroups,
            BitSet offsetIndexColumns, BitSet columnIndexColumns) throws Exception {
        return IndexWindow.plan(budget, countingFile(InputFile.of(FIXTURE)), 0,
                members(rowGroups, offsetIndexColumns, columnIndexColumns));
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

    /// The row groups as window members, each the row group of its index, needing the same
    /// index structures and no bloom filter.
    private static List<IndexWindow.Member> members(List<RowGroup> rowGroups, BitSet offsetIndexColumns,
            BitSet columnIndexColumns) {
        List<IndexWindow.Member> members = new ArrayList<>(rowGroups.size());
        for (int i = 0; i < rowGroups.size(); i++) {
            members.add(new IndexWindow.Member(rowGroups.get(i), i, offsetIndexColumns, columnIndexColumns,
                    new BitSet()));
        }
        return members;
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
