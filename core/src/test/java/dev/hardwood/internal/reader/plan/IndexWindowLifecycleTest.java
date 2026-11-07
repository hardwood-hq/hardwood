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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// How a read drives its page-index windows: how a failed window read reaches the read, the budget
/// in a whole read, and that every window is released.
///
/// Uses page_index_windows.parquet: twelve row groups of 400 rows over twenty INT64 columns, a
/// page index. At the default budget the twelve row groups are one window; the tests that need
/// several give the read a budget of one byte, which makes every row group a window of its
/// own.
class IndexWindowLifecycleTest {

    private static final Path FIXTURE = Path.of("src/test/resources/page_index_windows.parquet");
    private static final int ROW_GROUPS = 12;
    private static final long DEFAULT_BUDGET = 16L * 1024 * 1024;
    private static final long ONE_ROW_GROUP_PER_WINDOW = 1;
    private static final long ROWS = 12 * 400;

    @Test
    void aFileWhoseIndexFitsTheBudgetIsFetchedOnce() throws Exception {
        CountingInputFile file = countingFile(InputFile.of(FIXTURE));

        assertThat(readColumn(file, DEFAULT_BUDGET)).isEqualTo(ROWS);
        assertThat(indexReasons(file)).containsExactly("rg=0-11 indexes");
    }

    /// A window read that fails is read again, as every request of a read is, and the read goes on.
    @Test
    void aWindowThatFailsOnceIsReadAgain() throws Exception {
        AtomicBoolean failed = new AtomicBoolean();
        CountingInputFile file = countingFile(new FailingInputFile(InputFile.of(FIXTURE)) {
            @Override
            boolean fails() {
                return FetchReason.current().equals("rg=1 indexes") && !failed.getAndSet(true);
            }
        });

        assertThat(readColumn(file, ONE_ROW_GROUP_PER_WINDOW)).isEqualTo(ROWS);
        assertThat(failed).isTrue();
        assertThat(indexReasons(file)).as("the failed read and the read again")
                .filteredOn(reason -> reason.equals("rg=1 indexes")).hasSize(2);
    }

    /// A window that cannot be fetched fails the read at its row group, behind every row before it.
    @Test
    void aWindowThatCannotBeFetchedFailsTheReadAtItsRowGroup() throws Exception {
        CountingInputFile file = countingFile(new FailingInputFile(InputFile.of(FIXTURE)) {
            @Override
            boolean fails() {
                return FetchReason.current().equals("rg=1 indexes");
            }
        });
        AtomicLong rows = new AtomicLong();

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(file);
                    ColumnReader column = withBudget(ONE_ROW_GROUP_PER_WINDOW,
                            () -> reader.buildColumnReader("c05").build())) {
                while (column.nextBatch()) {
                    rows.addAndGet(column.getRecordCount());
                }
            }
        })
                .isInstanceOf(IOException.class)
                .hasMessage("[page_index_windows.parquet] Failed to fetch metadata for row group 1")
                .rootCause()
                .hasMessage("injected");
        assertThat(rows).hasValue(ROWS / ROW_GROUPS);
    }

    @Test
    void eachWindowOfAWholeReadStaysWithinTheBudget() throws Exception {
        long budget = 3000;
        CountingInputFile file = countingFile(InputFile.of(FIXTURE));
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                RowReader rows = withBudget(budget,
                        () -> reader.buildRowReader().projection(ColumnProjection.columns("c05")).build())) {
            while (rows.hasNext()) {
                rows.next();
            }
        }

        Map<String, Long> bytesPerWindow = new TreeMap<>();
        for (CountingInputFile.Read read : file.reads()) {
            if (read.reason().endsWith(" indexes")) {
                bytesPerWindow.merge(read.reason(), (long) read.length(), Long::sum);
            }
        }
        // Several windows, each within the budget, at least one of several row groups.
        assertThat(bytesPerWindow).hasSizeGreaterThan(1);
        assertThat(bytesPerWindow.values()).allSatisfy(bytes -> assertThat(bytes).isLessThanOrEqualTo(budget));
        assertThat(bytesPerWindow.keySet()).anySatisfy(reason -> assertThat(reason).contains("-"));
    }

    @Test
    void aWholeReadReleasesEveryWindow() throws Exception {
        CountingInputFile file = countingFile(InputFile.of(FIXTURE));
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            FileMetadataCache metadata = new FileMetadataCache(List.of(file), context);
            ReadPlanning planning = planning(metadata, context);
            List<WorkItem> workItems = workItems(planning);
            for (WorkItem workItem : workItems) {
                RowGroupIterators.plans(planning, workItem.workItemIndex());
                planning.release(workItem.workItemIndex());
            }

            assertEveryWindowReleased(workItems);
            metadata.close();
        }
    }

    @Test
    void closingAReadThatStoppedEarlyReleasesEveryWindow() throws Exception {
        CountingInputFile file = countingFile(InputFile.of(FIXTURE));
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            FileMetadataCache metadata = new FileMetadataCache(List.of(file), context);
            ReadPlanning planning = planning(metadata, context);
            List<WorkItem> workItems = workItems(planning);
            RowGroupIterators.plans(planning, 0);
            planning.discard();

            assertEveryWindowReleased(workItems);
            metadata.close();
        }
    }

    private static void assertEveryWindowReleased(List<WorkItem> workItems) {
        assertThat(workItems).hasSize(ROW_GROUPS);
        for (WorkItem workItem : workItems) {
            int index = workItem.workItemIndex();
            assertThatThrownBy(() -> workItem.indexWindow().buffersFor(index))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Page index of work item " + index
                            + " is not present: the window was not read, or was released");
        }
    }

    /// The planning of a read of the fixture, projecting `c05` alone, each row group a window of
    /// its own.
    private static ReadPlanning planning(FileMetadataCache metadata, HardwoodContextImpl context) throws Exception {
        return RowGroupIterators.planning(metadata, context, ColumnProjection.columns("c05"),
                ONE_ROW_GROUP_PER_WINDOW);
    }

    private static List<WorkItem> workItems(ReadPlanning planning) {
        List<WorkItem> workItems = new ArrayList<>();
        for (int i = 0; i < ROW_GROUPS; i++) {
            workItems.add(planning.workItem(i));
        }
        return workItems;
    }

    /// The rows of column `c05` of `file`, read through a column reader whose windows fetch at most
    /// `budget` bytes each.
    private static long readColumn(InputFile file, long budget) throws Exception {
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                ColumnReader column = withBudget(budget, () -> reader.buildColumnReader("c05").build())) {
            while (column.nextBatch()) {
                rows += column.getRecordCount();
            }
        }
        return rows;
    }

    @FunctionalInterface
    interface Build<T> {
        T build() throws Exception;
    }

    /// Runs `build` with the window budget set to `budget`; the read takes it as it is created.
    static <T> T withBudget(long budget, Build<T> build) throws Exception {
        String previous = System.setProperty("hardwood.internal.indexWindowBytes", Long.toString(budget));
        try {
            return build.build();
        }
        finally {
            if (previous == null) {
                System.clearProperty("hardwood.internal.indexWindowBytes");
            }
            else {
                System.setProperty("hardwood.internal.indexWindowBytes", previous);
            }
        }
    }

    private static List<String> indexReasons(CountingInputFile file) {
        return file.reads().stream()
                .map(CountingInputFile.Read::reason)
                .filter(reason -> reason.endsWith(" indexes"))
                .toList();
    }

    private static CountingInputFile countingFile(InputFile delegate) throws IOException {
        CountingInputFile file = new CountingInputFile(delegate);
        file.open();
        return file;
    }

    /// Fails a read with "injected" while [#fails] says so.
    private abstract static class FailingInputFile implements InputFile {

        private final InputFile delegate;

        FailingInputFile(InputFile delegate) {
            this.delegate = delegate;
        }

        abstract boolean fails();

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            if (fails()) {
                throw new IOException("injected");
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
