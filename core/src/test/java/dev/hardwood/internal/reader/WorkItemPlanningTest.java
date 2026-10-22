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
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A work item's planning completes with its outcome, which every column that reaches the work item
/// takes, and meets the need of a step that yielded on it once it completes, failed or not. An
/// `Error` it fails with reaches the caller once: through a column that takes it, or through the
/// close of the read's prefetch tasks when none does.
class WorkItemPlanningTest {

    @Test
    void aStepYieldingOnAPlanningResumesOnceItCompletes() {
        WorkItemPlanning planning = new WorkItemPlanning();
        AtomicInteger resumed = new AtomicInteger();
        planning.whenMet(resumed::incrementAndGet);
        assertThat(planning.isDone()).isFalse();
        assertThat(resumed).hasValue(0);

        planning.run(p -> null, new PrefetchTasks(Runnable::run));

        assertThat(planning.isDone()).isTrue();
        assertThat(resumed).hasValue(1);
    }

    @Test
    void aFailedPlanningResumesTheStepAndFailsEveryColumn() {
        IOException failure = new IOException("index unreadable");
        WorkItemPlanning planning = new WorkItemPlanning();
        AtomicInteger resumed = new AtomicInteger();
        planning.whenMet(resumed::incrementAndGet);

        planning.run(p -> {
            throw failure;
        }, new PrefetchTasks(Runnable::run));

        assertThat(resumed).hasValue(1);
        assertThat(planning.failed()).isTrue();
        assertThatThrownBy(planning::workItem).isSameAs(failure);
        assertThatThrownBy(planning::workItem).isSameAs(failure);
    }

    /// A checked exception an `InputFile` throws undeclared fails the planning, so the steps
    /// waiting for it resume and fail instead of waiting forever.
    @Test
    void aCheckedExceptionThrownUndeclaredFailsThePlanning() {
        TimeoutException timeout = new TimeoutException("too slow");
        WorkItemPlanning planning = new WorkItemPlanning();

        planning.run(p -> {
            throw WorkItemPlanningTest.<RuntimeException>sneaky(timeout);
        }, new PrefetchTasks(Runnable::run));

        assertThatThrownBy(planning::workItem)
                .isInstanceOf(IOException.class)
                .hasMessage("java.util.concurrent.TimeoutException: too slow")
                .cause()
                .isSameAs(timeout);
    }

    /// An `Error` a planning fails with reaches the column that takes it, and the close does not
    /// throw it a second time.
    @Test
    void anErrorOfAPlanningReachesTheColumnOnly() {
        AssertionError error = new AssertionError("planning broke");
        PrefetchTasks tasks = new PrefetchTasks(Runnable::run);
        WorkItemPlanning planning = new WorkItemPlanning();

        planning.run(p -> {
            throw error;
        }, tasks);

        assertThatThrownBy(planning::workItem).isSameAs(error);
        tasks.awaitAll();
    }

    /// An `Error` a planning fails with that no column takes, as when every column stopped while it
    /// ran, is thrown by the close.
    @Test
    void anErrorNoColumnTookIsThrownByTheClose() {
        AssertionError error = new AssertionError("planning broke");
        PrefetchTasks tasks = new PrefetchTasks(Runnable::run);
        WorkItemPlanning planning = new WorkItemPlanning();

        planning.run(p -> {
            throw error;
        }, tasks);

        assertThatThrownBy(tasks::awaitAll).isSameAs(error);
    }

    @Test
    void aPlanningTakenBeforeItCompletesIsRejected() {
        WorkItemPlanning planning = new WorkItemPlanning();

        assertThatThrownBy(planning::workItem)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The work item has not been planned");
    }

    /// Three row groups with a page index; a filter with one value in each makes every row group's
    /// planning read its page index.
    private static final Path FILE = Path.of("src/test/resources/multi_row_group_page_index.parquet");
    private static final FilterPredicate IN_EVERY_ROW_GROUP = FilterPredicate.in("id", 500L, 1500L, 2500L);

    /// An `Error` a read of the first row group's planning fails with, on its virtual thread, reaches
    /// the caller through the columns, once: the close throws it no second time.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void anErrorWhilePlanningReachesTheCallerOnce() {
        AssertionError error = new AssertionError("planning read broke");
        InputFile file = new PlanningInputFile(FILE, () -> {
            throw error;
        });

        AtomicReference<Throwable> thrown = new AtomicReference<>();
        try {
            readAll(file);
        }
        catch (Throwable e) {
            thrown.set(e);
        }

        assertThat(thrown.get()).isSameAs(error);
        assertThat(error.getSuppressed()).isEmpty();
    }

    /// A close while a work item's planning waits for its read waits for the planning, and the read
    /// ends once the read completes: no step stays parked on the planning. The close waits for a
    /// planning a column's step yields on through that column, whose step holds its count of tasks,
    /// and for one no step has reached, such as the next row group's planning ahead of its read,
    /// through the read's prefetch tasks. The second of three files is the one held, so the reader is
    /// built and reading when the close comes.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aCloseWhilePlanningWaitsForThePlanning() throws Exception {
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger planningReads = new AtomicInteger();
        AtomicReference<Thread> planningThread = new AtomicReference<>();
        PlanningRead hold = () -> {
            // The window's first request; the others go through once it is released
            if (planningReads.incrementAndGet() != 1) {
                return;
            }
            planningThread.set(Thread.currentThread());
            held.countDown();
            try {
                release.await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted", e);
            }
        };
        PlanningRead pass = () -> {
        };
        ParquetFileReader reader = ParquetFileReader.openAll(List.of(new PlanningInputFile(FILE, pass),
                new PlanningInputFile(FILE, hold), new PlanningInputFile(FILE, pass)));
        // Batches of one row, so the reader is built, its first batch taken from the first file,
        // when the second file's planning is held
        Thread consumer = Thread.ofPlatform().start(() -> {
            try (ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("id"))
                    .filter(IN_EVERY_ROW_GROUP).batchSize(1).build()) {
                while (columns.nextBatch()) {
                    // read until the close ends the read
                }
            }
            catch (IOException | RuntimeException e) {
                // the close ends the read
            }
        });
        assertThat(held.await(10, TimeUnit.SECONDS)).as("the planning's read is held").isTrue();
        assertThat(planningThread.get().isVirtual()).as("the planning runs on a virtual thread").isTrue();

        Thread closer = Thread.ofPlatform().start(() -> {
            try {
                reader.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        assertThat(awaitFrame(closer, ScheduledColumn.class.getName(), "close")
                || awaitFrame(closer, PrefetchTasks.class.getName(), "awaitAll"))
                .as("the close waits for the planning, through a column's step or the prefetch tasks")
                .isTrue();

        release.countDown();
        closer.join(10_000);
        consumer.join(10_000);
        assertThat(closer.isAlive()).as("the close returned").isFalse();
        assertThat(consumer.isAlive()).as("the read ended").isFalse();
    }

    /// Whether `thread` reaches a frame of `method` in `className` within five seconds.
    private static boolean awaitFrame(Thread thread, String className, String method) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            for (StackTraceElement frame : thread.getStackTrace()) {
                if (frame.getClassName().equals(className) && frame.getMethodName().equals(method)) {
                    return true;
                }
            }
            Thread.sleep(5);
        }
        return false;
    }

    private static void readAll(InputFile file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                RowReader rows = reader.buildRowReader().filter(IN_EVERY_ROW_GROUP).build()) {
            while (rows.hasNext()) {
                rows.next();
            }
        }
    }

    /// Runs before a planning read.
    @FunctionalInterface
    private interface PlanningRead {
        void onRead() throws IOException;
    }

    /// A file not in memory that runs a [PlanningRead] before the planning of its first row group
    /// reads the page index window that row group falls into.
    private static final class PlanningInputFile implements InputFile {

        private final InputFile delegate;
        private final PlanningRead onPlanningRead;

        PlanningInputFile(Path path, PlanningRead onPlanningRead) {
            this.delegate = InputFile.of(path);
            this.onPlanningRead = onPlanningRead;
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            // The first row group's window, read by its planning or by the prefetch of the row group
            // before it, which prefixes its own reason
            if (FetchReason.current().matches("(.* \\| )?rg=0(-\\d+)? indexes")) {
                onPlanningRead.onRead();
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
        public Optional<String> identity() throws IOException {
            return delegate.identity();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> T sneaky(Throwable e) throws T {
        throw (T) e;
    }
}
