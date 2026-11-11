/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.reader.footer.ParquetMetadataReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;

import static org.assertj.core.api.Assertions.assertThat;

/// Window formation and requests, on the row groups of page_index_windows.parquet
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
    void aWindowRequestsEachStructureOnceForEveryMember() throws Exception {
        IndexWindow window = IndexWindow.plan(Long.MAX_VALUE, countingFile(InputFile.of(FIXTURE)),
                members(rowGroups().subList(0, 3), column(0), column(0))).getFirst();

        assertThat(window.indexSpans()).hasSize(2).allSatisfy(span -> {
            assertThat(span.reason()).isEqualTo("rg=0-2 indexes");
            assertThat(span.holders()).isEqualTo(3);
        });
    }

    private static List<IndexWindow> plan(long budget, List<RowGroup> rowGroups,
            BitSet offsetIndexColumns, BitSet columnIndexColumns) throws Exception {
        return IndexWindow.plan(budget, countingFile(InputFile.of(FIXTURE)),
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
