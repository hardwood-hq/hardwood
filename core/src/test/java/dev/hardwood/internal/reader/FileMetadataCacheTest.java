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
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ReadScope;
import dev.hardwood.internal.reader.FileMetadataCache.PreparedFile;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileMetadataCacheTest {

    private static final long WAIT_SECONDS = 5;
    private static final String FIRST_FILE = "src/test/resources/multi_file_part0.parquet";
    private static final String LATER_FILE = "src/test/resources/multi_file_part1.parquet";

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentCheckedAndDataLookupsJoinOneInProgressLoad() throws Exception {
        LatchInputFile inputFile = LatchInputFile.blocked(LATER_FILE);
        FileMetadataCache cache = new FileMetadataCache(List.of(inputFile));

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<FileMetaData> firstChecked = executor.submit(() -> cache.getFileMetaData(0));
            assertThat(inputFile.awaitFooterRead()).isTrue();

            CountDownLatch callersReady = new CountDownLatch(2);
            CountDownLatch callTogether = new CountDownLatch(1);
            Future<FileMetaData> secondChecked = executor.submit(() -> {
                callersReady.countDown();
                callTogether.await();
                return cache.getFileMetaData(0);
            });
            Future<PreparedFile> dataPath = executor.submit(() -> {
                callersReady.countDown();
                callTogether.await();
                return cache.getFile(0);
            });
            assertThat(callersReady.await(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
            callTogether.countDown();

            assertThat(secondChecked.isDone()).isFalse();
            assertThat(dataPath.isDone()).isFalse();
            inputFile.releaseFooterRead();

            FileMetaData metadata = firstChecked.get(WAIT_SECONDS, TimeUnit.SECONDS);
            assertThat(secondChecked.get(WAIT_SECONDS, TimeUnit.SECONDS)).isSameAs(metadata);
            assertThat(dataPath.get(WAIT_SECONDS, TimeUnit.SECONDS).metaData()).isSameAs(metadata);
            assertThat(inputFile.openCount()).isEqualTo(1);
        }
        finally {
            inputFile.releaseFooterRead();
            cache.close();
            assertThat(inputFile.closeCount()).isZero();
            inputFile.close();
        }
    }

    @Test
    void closePreventsLaterLoadsWithoutClosingInputs() throws Exception {
        LatchInputFile inputFile = LatchInputFile.unblocked(LATER_FILE);
        FileMetadataCache cache = new FileMetadataCache(List.of(inputFile));

        cache.close();
        cache.prefetch(0);

        assertThat(inputFile.openCount()).isZero();
        assertThat(inputFile.closeCount()).isZero();
        assertThatThrownBy(() -> cache.getFile(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("FileMetadataCache is closed");

        cache.close();
        assertThat(inputFile.closeCount()).isZero();
        inputFile.close();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void parentCloseWaitsForRegisteredLoadBeforeClosingInputs() throws Exception {
        CountingInputFile first = new CountingInputFile(InputFile.of(Paths.get(FIRST_FILE)));
        LatchInputFile later = LatchInputFile.blocked(LATER_FILE);
        ParquetFileReader reader = ParquetFileReader.openAll(List.of(first, later));
        Future<Void> closeFuture = null;

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<FileMetaData> metadata = executor.submit(() -> reader.getFileMetaData(1));
            assertThat(later.awaitFooterRead()).isTrue();

            CountDownLatch closeInvoked = new CountDownLatch(1);
            closeFuture = executor.submit(() -> {
                closeInvoked.countDown();
                reader.close();
                return null;
            });
            assertThat(closeInvoked.await(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
            Future<Void> registeredClose = closeFuture;
            assertThatThrownBy(() -> registeredClose.get(250, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            assertThat(first.closeCount()).isZero();
            assertThat(later.closeCount()).isZero();

            later.releaseFooterRead();
            assertThat(metadata.get(WAIT_SECONDS, TimeUnit.SECONDS).numRows()).isEqualTo(100);
            closeFuture.get(WAIT_SECONDS, TimeUnit.SECONDS);
        }
        finally {
            later.releaseFooterRead();
            if (closeFuture == null) {
                reader.close();
            }
            else {
                closeFuture.get(WAIT_SECONDS, TimeUnit.SECONDS);
            }
        }

        assertThat(first.closeCount()).isEqualTo(1);
        assertThat(later.closeCount()).isEqualTo(1);
    }

    private static final class LatchInputFile implements InputFile {

        private final InputFile delegate;
        private final CountDownLatch footerReadStarted = new CountDownLatch(1);
        private final CountDownLatch releaseFooterRead;
        private final AtomicBoolean firstFooterRead = new AtomicBoolean(true);
        private final AtomicInteger openCount = new AtomicInteger();
        private final AtomicInteger closeCount = new AtomicInteger();

        private LatchInputFile(String path, boolean blocked) {
            this.delegate = InputFile.of(Paths.get(path));
            this.releaseFooterRead = new CountDownLatch(blocked ? 1 : 0);
        }

        static LatchInputFile blocked(String path) {
            return new LatchInputFile(path, true);
        }

        static LatchInputFile unblocked(String path) {
            return new LatchInputFile(path, false);
        }

        boolean awaitFooterRead() throws InterruptedException {
            return footerReadStarted.await(WAIT_SECONDS, TimeUnit.SECONDS);
        }

        void releaseFooterRead() {
            releaseFooterRead.countDown();
        }

        int openCount() {
            return openCount.get();
        }

        int closeCount() {
            return closeCount.get();
        }

        @Override
        public void open() throws IOException {
            openCount.incrementAndGet();
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            if (readingFileMetadata() && firstFooterRead.compareAndSet(true, false)) {
                footerReadStarted.countDown();
                try {
                    if (!releaseFooterRead.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                        throw new IOException("Timed out waiting to release footer read");
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting to release footer read", e);
                }
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
            closeCount.incrementAndGet();
            delegate.close();
        }
    }

    /// Whether the read in progress is of the file's own bookkeeping — the
    /// magic markers, the footer length, the footer — rather than of its data.
    private static boolean readingFileMetadata() {
        ReadScope.Place place = ReadScope.current();
        return place != null && place.region() != null && place.region().isFileMetadata();
    }
}
