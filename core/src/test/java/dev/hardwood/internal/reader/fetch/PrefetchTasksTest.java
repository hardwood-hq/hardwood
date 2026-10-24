/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.reader.plan.FileMetadataCache;
import dev.hardwood.internal.reader.plan.RowGroupIterator;
import dev.hardwood.internal.reader.plan.RowGroupIterators;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A read started ahead of demand that fails does not fail the read: an exception is logged at
/// `DEBUG`, and an `Error` no step or load took is thrown by [PrefetchTasks#awaitAll()], which
/// the close of the read that started it calls.
class PrefetchTasksTest {

    private final List<HardwoodContextImpl> contexts = new ArrayList<>();

    @AfterEach
    void closeContexts() {
        contexts.forEach(HardwoodContextImpl::close);
    }

    /// A cache of `file` whose prefetches run on `ioExecutor`.
    private FileMetadataCache cache(InputFile file, Executor ioExecutor) {
        HardwoodContextImpl context = HardwoodContextImpl.withIoExecutor(1, ioExecutor);
        contexts.add(context);
        return new FileMetadataCache(List.of(file), context);
    }

    /// Four row groups of 50 rows, with a page index.
    private static final Path FOUR_ROW_GROUPS = Path.of("src/test/resources/differential/diff_order_multi.parquet");
    private static final String FIRST_FILE = "src/test/resources/multi_file_part0.parquet";
    private static final String LATER_FILE = "src/test/resources/multi_file_part1.parquet";

    @Test
    void awaitAllThrowsTheFirstErrorWithLaterOnesSuppressed() {
        PrefetchTasks tasks = new PrefetchTasks(Runnable::run);
        SpeculativeError first = new SpeculativeError("first");
        SpeculativeError second = new SpeculativeError("second");
        failing(tasks, first).submit();
        new RequestAttempt<>("read", true, tasks, () -> "read", () -> null).submit();
        failing(tasks, second).submit();

        assertThatThrownBy(tasks::awaitAll)
                .isSameAs(first)
                .hasMessage("first");
        assertThat(first.getSuppressed()).containsExactly(second);
        // Thrown once: a second close finds nothing to report.
        tasks.awaitAll();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void taskNotStartedByAwaitAllSkipsItsWork() throws Exception {
        List<Runnable> queued = new ArrayList<>();
        PrefetchTasks tasks = new PrefetchTasks(queued::add);
        AtomicBoolean ran = new AtomicBoolean();
        tasks.submit(() -> ran.set(true), () -> {});

        Thread closer = Thread.ofVirtual().start(tasks::awaitAll);
        while (closer.getState() != Thread.State.WAITING) {
            Thread.onSpinWait();
        }
        queued.forEach(Runnable::run);
        closer.join();

        assertThat(ran).isFalse();
    }

    @Test
    void taskSubmittedAfterAwaitAllIsDropped() {
        List<Runnable> queued = new ArrayList<>();
        PrefetchTasks tasks = new PrefetchTasks(queued::add);
        tasks.awaitAll();

        AtomicBoolean dropped = new AtomicBoolean();
        tasks.submit(() -> {}, () -> dropped.set(true));

        assertThat(queued).isEmpty();
        assertThat(dropped).isTrue();
    }

    /// The first row group's plans make the second the read's next unit, whose data read raises an
    /// `Error`; nothing reads the second row group, and the iterator's close throws the `Error`
    /// once it has closed the file it owns.
    @Tag("pool")
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void errorOfARowGroupPrefetchIsThrownByTheIteratorsCloseAfterClosingTheFile() throws Exception {
        FileMetaData metaData;
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FOUR_ROW_GROUPS))) {
            metaData = reader.getFileMetaData();
            schema = reader.getFileSchema();
        }
        PrefetchFailingInputFile file = new PrefetchFailingInputFile(InputFile.of(FOUR_ROW_GROUPS), "rg=1 col=");
        file.open();
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2)) {
            RowGroupIterator iterator = RowGroupIterators.standalone(List.of(file), context);
            iterator.setFirstFile(schema, metaData.rowGroups());
            iterator.initialize(ReadProjection.of(
                    ProjectedSchema.create(schema, ColumnProjection.columns("__row__"))), null, true);
            iterator.getColumnPlan(iterator.workItemAt(0), 0);
            assertThat(file.awaitFailedRead()).isTrue();

            assertThatThrownBy(iterator::close)
                    .isSameAs(file.failure)
                    .hasMessage("prefetch read failed");
        }
        assertThat(file.closeCount()).isEqualTo(1);
    }

    /// The second file's footer is prefetched while the first is planned, and its load raises an
    /// `Error`. The read stops within the first file and never asks for the second, so the
    /// parent's close, which also closes the child reader left open, throws the `Error` once it
    /// has closed both files.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void errorOfAFooterPrefetchNoReadAskedForIsThrownByTheParentsCloseAfterClosingTheFiles() throws Exception {
        CountingInputFile first = new CountingInputFile(InputFile.of(Paths.get(FIRST_FILE)));
        PrefetchFailingInputFile later = new PrefetchFailingInputFile(InputFile.of(Paths.get(LATER_FILE)), "footer-");
        ParquetFileReader reader = ParquetFileReader.openAll(List.of(first, later));
        RowReader rows = reader.buildRowReader().head(10).build();
        int read = 0;
        while (rows.hasNext()) {
            rows.next();
            read++;
        }
        assertThat(read).isEqualTo(10);
        assertThat(later.awaitFailedRead()).isTrue();

        assertThatThrownBy(reader::close)
                .isSameAs(later.failure)
                .hasMessage("prefetch read failed");
        assertThat(first.closeCount()).isEqualTo(1);
        assertThat(later.closeCount()).isEqualTo(1);
    }

    /// A footer read ahead that fails is logged once, at `DEBUG`, as it fails: the load that takes
    /// the failure reports it.
    @Test
    void failedFooterReadAheadIsLoggedAtDebug() {
        FailingFooterFile file = new FailingFooterFile(new IOException("connection reset"));
        FileMetadataCache cache = cache(file, Runnable::run);
        PrefetchTasks tasks = new PrefetchTasks(Runnable::run);

        List<LogEvent> events = captureLog(RequestAttempt.class, () -> cache.prefetch(0, tasks, () -> {}));

        assertThat(events).hasSize(1);
        assertThat(events.getFirst().getLevel()).isEqualTo(Level.DEBUG);
        assertThat(events.getFirst().getThrown().getCause()).isSameAs(file.failure);
        tasks.awaitAll();
        cache.close();
    }

    /// An `Error` of a footer read ahead that no load took is thrown by the close of the read
    /// that started it.
    @Test
    void errorOfAFooterReadAheadNoLoadTookIsThrownByTheReadsClose() {
        SpeculativeError error = new SpeculativeError("footer read failed");
        FailingFooterFile file = new FailingFooterFile(error);
        FileMetadataCache cache = cache(file, Runnable::run);
        PrefetchTasks tasks = new PrefetchTasks(Runnable::run);
        cache.prefetch(0, tasks, () -> {});

        assertThatThrownBy(tasks::awaitAll)
                .isSameAs(error)
                .hasMessage("footer read failed");
        cache.close();
    }

    /// An `Error` of a footer read ahead that a load took is not thrown again by the close of the
    /// read that started it.
    @Test
    void errorOfAFooterReadAheadALoadTookIsNotThrownAgain() {
        SpeculativeError error = new SpeculativeError("footer read failed");
        FailingFooterFile file = new FailingFooterFile(error);
        FileMetadataCache cache = cache(file, Runnable::run);
        PrefetchTasks tasks = new PrefetchTasks(Runnable::run);
        cache.prefetch(0, tasks, () -> {});
        assertThatThrownBy(() -> cache.getFile(0))
                .isSameAs(error)
                .hasMessage("footer read failed");

        tasks.awaitAll();
        cache.close();
    }

    /// A read on `tasks` that fails with `error`.
    private static RequestAttempt<String> failing(PrefetchTasks tasks, Error error) {
        return new RequestAttempt<>("read", true, tasks, () -> {
            throw error;
        }, () -> null);
    }

    private static List<LogEvent> captureLog(Class<?> logger, Runnable action) {
        String name = logger.getName();
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = context.getConfiguration();
        CapturingAppender appender = new CapturingAppender();
        appender.start();
        LoggerConfig loggerConfig = new LoggerConfig(name, Level.DEBUG, false);
        loggerConfig.addAppender(appender, null, null);
        configuration.addLogger(name, loggerConfig);
        context.updateLoggers();
        try {
            action.run();
        }
        finally {
            configuration.removeLogger(name);
            context.updateLoggers();
            appender.stop();
        }
        return List.copyOf(appender.events);
    }

    private static final class CapturingAppender extends AbstractAppender {

        private final List<LogEvent> events = new CopyOnWriteArrayList<>();

        CapturingAppender() {
            super("capture", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }
    }

    private static final class SpeculativeError extends Error {

        SpeculativeError(String message) {
            super(message);
        }
    }

    /// A file whose every read fails with `failure`.
    private static final class FailingFooterFile implements InputFile {

        private final Throwable failure;

        FailingFooterFile(Throwable failure) {
            this.failure = failure;
        }

        @Override
        public void open() {
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            switch (failure) {
                case IOException e -> throw e;
                case Error e -> throw e;
                default -> throw new IllegalStateException(failure);
            }
        }

        @Override
        public long length() {
            return 1024;
        }

        @Override
        public String name() {
            return "failing.parquet";
        }

        @Override
        public void close() {
        }
    }

    /// Fails the first read a speculative task issues under a [FetchReason] starting with
    /// `reasonPrefix` with an `Error`.
    private static final class PrefetchFailingInputFile extends CountingInputFile {

        private final SpeculativeError failure = new SpeculativeError("prefetch read failed");
        private final String reasonPrefix;
        private final AtomicBoolean failed = new AtomicBoolean();
        private final CountDownLatch failedRead = new CountDownLatch(1);

        PrefetchFailingInputFile(InputFile delegate, String reasonPrefix) {
            super(delegate);
            this.reasonPrefix = reasonPrefix;
        }

        boolean awaitFailedRead() throws InterruptedException {
            return failedRead.await(5, TimeUnit.SECONDS);
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            if (Thread.currentThread().getName().equals("hardwood-io")
                    && FetchReason.current().startsWith(reasonPrefix) && failed.compareAndSet(false, true)) {
                failedRead.countDown();
                throw failure;
            }
            return super.readRange(offset, length);
        }
    }
}
