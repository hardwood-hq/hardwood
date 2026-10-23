/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.stream.Stream;

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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A speculative task's failure does not fail the read: a failure of the file is logged at
/// `DEBUG`, any other `RuntimeException` at `WARNING`, and an `Error` is thrown by
/// [PrefetchTasks#awaitAll()], which the iterator's close calls, or, for a footer prefetch no
/// read asked for, by the close of the [FileMetadataCache].
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
    void runtimeFailureIsLoggedAsWarning() {
        IllegalStateException failure = new IllegalStateException("speculative bug");
        PrefetchTasks tasks = new PrefetchTasks(Runnable::run);

        List<LogEvent> events = captureLog(() -> {
            tasks.submit(() -> {
                throw failure;
            });
            tasks.awaitAll();
        });

        assertThat(events).hasSize(1);
        assertThat(events.getFirst().getLevel()).isEqualTo(Level.WARN);
        assertThat(events.getFirst().getMessage().getFormattedMessage()).isEqualTo("Prefetch failed");
        assertThat(events.getFirst().getThrown()).isSameAs(failure);
    }

    static Stream<RuntimeException> fileFailures() {
        return Stream.of(new UncheckedIOException(new IOException("connection reset")),
                new ParquetReadException("malformed page header"),
                new UnsupportedOperationException("column chunk too large"));
    }

    @ParameterizedTest
    @MethodSource("fileFailures")
    void fileFailureIsLoggedAtDebug(RuntimeException failure) {
        PrefetchTasks tasks = new PrefetchTasks(Runnable::run);

        List<LogEvent> events = captureLog(() -> {
            tasks.submit(() -> {
                throw failure;
            });
            tasks.awaitAll();
        });

        assertThat(events).hasSize(1);
        assertThat(events.getFirst().getLevel()).isEqualTo(Level.DEBUG);
        assertThat(events.getFirst().getThrown()).isSameAs(failure);
    }

    @Test
    void awaitAllThrowsTheFirstErrorWithLaterOnesSuppressed() {
        PrefetchTasks tasks = new PrefetchTasks(Runnable::run);
        SpeculativeError first = new SpeculativeError("first");
        SpeculativeError second = new SpeculativeError("second");
        tasks.submit(() -> {
            throw first;
        });
        tasks.submit(() -> {
        });
        tasks.submit(() -> {
            throw second;
        });

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
        tasks.submit(() -> ran.set(true));

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

        tasks.submit(() -> {
        });

        assertThat(queued).isEmpty();
    }

    /// The first row group's plans start the prefetch of the second, whose read raises an
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
        PrefetchFailingInputFile file = new PrefetchFailingInputFile(InputFile.of(FOUR_ROW_GROUPS), "prefetch rg=1 |");
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

    /// A footer prefetch that fails and that no caller asks for is reported once, by the close.
    @Test
    void failedFooterPrefetchNoCallerAskedForIsLoggedByTheClose() {
        FailingFooterFile file = new FailingFooterFile(new IOException("connection reset"));
        FileMetadataCache cache = cache(file, Runnable::run);
        cache.prefetch(0);

        List<LogEvent> events = captureLog(cache::close);

        assertThat(events).hasSize(1);
        assertThat(events.getFirst().getLevel()).isEqualTo(Level.DEBUG);
        assertThat(events.getFirst().getThrown().getCause()).isSameAs(file.failure);
    }

    /// A footer prefetch whose failure a caller received is not reported again by the close.
    @Test
    void failedFooterPrefetchACallerReceivedIsNotReportedAgain() {
        FailingFooterFile file = new FailingFooterFile(new IOException("connection reset"));
        FileMetadataCache cache = cache(file, Runnable::run);
        cache.prefetch(0);
        assertThatThrownBy(() -> cache.getFile(0))
                .isInstanceOf(IOException.class)
                .hasMessage("[failing.parquet] Failed to read metadata")
                .hasCause(file.failure);

        List<LogEvent> events = captureLog(cache::close);

        assertThat(events).isEmpty();
    }

    /// An `Error` of a footer prefetch a caller received is not thrown again by the close.
    @Test
    void errorOfAFooterPrefetchACallerReceivedIsNotThrownAgain() {
        SpeculativeError error = new SpeculativeError("footer read failed");
        FailingFooterFile file = new FailingFooterFile(error);
        FileMetadataCache cache = cache(file, Runnable::run);
        cache.prefetch(0);
        assertThatThrownBy(() -> cache.getFile(0))
                .isSameAs(error)
                .hasMessage("footer read failed");

        cache.close();
    }

    private static List<LogEvent> captureLog(Runnable action) {
        String name = PrefetchTasks.class.getName();
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
