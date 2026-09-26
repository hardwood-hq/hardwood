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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// How a read drives its page-index windows: which windows a read that stops early fetches, how
/// a failed prefetch reaches the demand path, the budget in a whole read, and that every window
/// is released.
///
/// Uses page_index_windows.parquet: twelve row groups of 400 rows over twenty INT64 columns, a
/// page index. At the default budget the twelve row groups are one window; the tests that need
/// several give the iterator a budget of one byte, which makes every row group a window of its
/// own.
class IndexWindowLifecycleTest {

    private static final Path FIXTURE = Path.of("src/test/resources/page_index_windows.parquet");
    private static final int ROW_GROUPS = 12;
    private static final long DEFAULT_BUDGET = 16L * 1024 * 1024;
    private static final long ONE_ROW_GROUP_PER_WINDOW = 1;

    @Test
    void aReadThatStopsAfterTheFirstRowGroupFetchesNoWindowBeyondThePrefetchedOne() throws Exception {
        CountDownLatch prefetched = new CountDownLatch(1);
        CountingInputFile file = countingFile(new FailingInputFile(InputFile.of(FIXTURE)) {
            @Override
            boolean fails() {
                if (FetchReason.current().equals("rg=1 indexes")) {
                    prefetched.countDown();
                }
                return false;
            }
        });
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            RowGroupIterator iterator = iterator(file, context, ONE_ROW_GROUP_PER_WINDOW);
            try {
                iterator.getColumnPlan(iterator.workItemAt(0), 0);
                // Closing at once would stop the prefetch before it reads anything.
                assertThat(prefetched.await(10, TimeUnit.SECONDS)).isTrue();
            }
            finally {
                iterator.close();
            }
        }

        assertThat(indexReasons(file)).containsExactlyInAnyOrder("rg=0 indexes", "rg=1 indexes");
    }

    @Test
    void aFileWhoseIndexFitsTheBudgetIsFetchedOnce() throws Exception {
        CountingInputFile file = countingFile(InputFile.of(FIXTURE));
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            RowGroupIterator iterator = iterator(file, context, DEFAULT_BUDGET);
            try {
                for (RowGroupIterator.WorkItem workItem : iterator.getWorkItems()) {
                    iterator.getColumnPlan(workItem, 0);
                }
            }
            finally {
                iterator.close();
            }
        }

        assertThat(indexReasons(file)).containsExactly("rg=0-11 indexes");
    }

    @Test
    void aWindowWhosePrefetchFailedIsFetchedByTheDemandPath() throws Exception {
        CountDownLatch prefetchFailed = new CountDownLatch(1);
        AtomicBoolean failed = new AtomicBoolean();
        CountingInputFile file = countingFile(new FailingInputFile(InputFile.of(FIXTURE)) {
            @Override
            boolean fails() {
                if (FetchReason.current().equals("rg=1 indexes") && !failed.getAndSet(true)) {
                    prefetchFailed.countDown();
                    return true;
                }
                return false;
            }
        });
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            RowGroupIterator iterator = iterator(file, context, ONE_ROW_GROUP_PER_WINDOW);
            try {
                iterator.getColumnPlan(iterator.workItemAt(0), 0);
                assertThat(prefetchFailed.await(10, TimeUnit.SECONDS)).isTrue();

                FetchPlan plan = iterator.getColumnPlan(iterator.workItemAt(1), 0);

                assertThat(plan.isEmpty()).isFalse();
            }
            finally {
                iterator.close();
            }
        }
        // The prefetch's failed attempt, then the demand path's.
        assertThat(indexReasons(file)).containsExactlyInAnyOrder(
                "rg=0 indexes", "rg=1 indexes", "rg=1 indexes");
    }

    @Test
    void aWindowThatCannotBeFetchedFailsTheDemandPath() throws Exception {
        CountingInputFile file = countingFile(new FailingInputFile(InputFile.of(FIXTURE)) {
            @Override
            boolean fails() {
                return FetchReason.current().equals("rg=1 indexes");
            }
        });
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            RowGroupIterator iterator = iterator(file, context, ONE_ROW_GROUP_PER_WINDOW);
            try {
                iterator.getColumnPlan(iterator.workItemAt(0), 0);
                RowGroupIterator.WorkItem second = iterator.workItemAt(1);

                assertThatThrownBy(() -> iterator.getColumnPlan(second, 0))
                        .isInstanceOf(IOException.class)
                        .hasMessage("[page_index_windows.parquet] Failed to fetch metadata for row group 1")
                        .cause()
                        .hasMessage("injected");
            }
            finally {
                iterator.close();
            }
        }
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
            RowGroupIterator iterator = iterator(file, context, ONE_ROW_GROUP_PER_WINDOW);
            try {
                List<RowGroupIterator.WorkItem> workItems = iterator.getWorkItems();
                for (RowGroupIterator.WorkItem workItem : workItems) {
                    iterator.getColumnPlan(workItem, 0);
                    iterator.releaseWorkItem(workItem);
                }

                assertEveryWindowReleased(workItems);
            }
            finally {
                iterator.close();
            }
        }
    }

    @Test
    void closingAReadThatStoppedEarlyReleasesEveryWindow() throws Exception {
        CountingInputFile file = countingFile(InputFile.of(FIXTURE));
        List<RowGroupIterator.WorkItem> workItems;
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            RowGroupIterator iterator = iterator(file, context, ONE_ROW_GROUP_PER_WINDOW);
            workItems = iterator.getWorkItems();
            iterator.getColumnPlan(workItems.get(0), 0);
            iterator.close();
        }

        assertEveryWindowReleased(workItems);
    }

    private static void assertEveryWindowReleased(List<RowGroupIterator.WorkItem> workItems) {
        assertThat(workItems).hasSize(ROW_GROUPS);
        for (RowGroupIterator.WorkItem workItem : workItems) {
            int index = workItem.workItemIndex();
            assertThatThrownBy(() -> workItem.indexWindow().buffersFor(index))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Page index of work item " + index + " requested after its release");
        }
    }

    /// An iterator over the fixture, projecting `c05` alone, so each work item has one column to
    /// release it, whose windows fetch at most `budget` bytes each.
    private static RowGroupIterator iterator(CountingInputFile file, HardwoodContextImpl context,
            long budget) throws Exception {
        FileMetaData metaData;
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            metaData = reader.getFileMetaData();
            schema = reader.getFileSchema();
        }
        RowGroupIterator iterator = withBudget(budget,
                () -> new RowGroupIterator(List.of(file), context, 0));
        iterator.setFirstFile(schema, metaData.rowGroups());
        iterator.initialize(ProjectedSchema.create(schema, ColumnProjection.columns("c05")), null);
        return iterator;
    }

    @FunctionalInterface
    interface Build<T> {
        T build() throws Exception;
    }

    /// Runs `build` with the window budget set to `budget`; the iterator reads it as it is
    /// created.
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
