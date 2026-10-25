/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.reader.plan.FileMetadataCache.PreparedFile;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.ParsedFooter;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileMetadataCacheTest {

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

    private static final long WAIT_SECONDS = 5;
    private static final String FIRST_FILE = "src/test/resources/multi_file_part0.parquet";
    private static final String LATER_FILE = "src/test/resources/multi_file_part1.parquet";

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentCheckedAndDataLookupsJoinOneInProgressLoad() throws Exception {
        LatchInputFile inputFile = LatchInputFile.blocked(LATER_FILE);
        FileMetadataCache cache = cache(inputFile, Runnable::run);

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

    /// A read never waits for a footer: a load another thread has registered is a claim pending
    /// on it, which completes with the load, after which the claim takes the file.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aClaimFindsALoadAnotherThreadRegisteredPending() throws Exception {
        LatchInputFile inputFile = LatchInputFile.blocked(LATER_FILE);
        HardwoodContextImpl context = HardwoodContextImpl.create(1);
        contexts.add(context);
        FileMetadataCache cache = new FileMetadataCache(List.of(inputFile), context);

        try (ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<FileMetaData> offPool = callers.submit(() -> cache.getFileMetaData(0));
            assertThat(inputFile.awaitFooterRead()).isTrue();

            FileMetadataCache.Claim claim = cache.claim(0);
            assertThat(claim.pending()).as("the load under way").isNotNull();
            assertThat(claim.load()).isNull();
            assertThat(claim.pending().isDone()).as("done before the load completes").isFalse();

            inputFile.releaseFooterRead();
            FileMetaData metadata = offPool.get(WAIT_SECONDS, TimeUnit.SECONDS);
            claim.pending().get(WAIT_SECONDS, TimeUnit.SECONDS);
            assertThat(cache.claim(0).file().metaData()).isSameAs(metadata);
        }
        finally {
            inputFile.releaseFooterRead();
            cache.close();
            inputFile.close();
        }
    }

    @Test
    void aFooterOutsideAReadIsLoadedOnTheCallingThread() throws Exception {
        LatchInputFile inputFile = LatchInputFile.unblocked(LATER_FILE);
        FileMetadataCache cache = cache(inputFile, task -> {
                    throw new AssertionError("a footer the caller waits for runs on the caller's thread");
                });

        try {
            assertThat(cache.getFileMetaData(0).numRows()).isEqualTo(100);
            assertThat(inputFile.openCount()).isEqualTo(1);
        }
        finally {
            cache.close();
            inputFile.close();
        }
    }

    /// A load a read claimed runs its read and parse where the read runs them, and a lookup of the
    /// file meanwhile joins it rather than loading the file again.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aLookupJoinsALoadAReadClaimed() throws Exception {
        LatchInputFile inputFile = LatchInputFile.unblocked(LATER_FILE);
        FileMetadataCache cache = cache(inputFile, Runnable::run);

        try (ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor()) {
            FileMetadataCache.ClaimedLoad load = cache.claim(0).load();
            assertThat(load).isNotNull();
            assertThat(load.readsInPlace()).isFalse();
            assertThat(cache.claim(0).pending()).as("the claimed load under way").isNotNull();
            Future<FileMetaData> lookup = callers.submit(() -> cache.getFileMetaData(0));

            load.read();
            FileMetaData parsed = load.parse().metaData();

            assertThat(lookup.get(WAIT_SECONDS, TimeUnit.SECONDS)).isSameAs(parsed);
            assertThat(parsed.numRows()).isEqualTo(100);
            assertThat(inputFile.openCount()).isEqualTo(1);
        }
        finally {
            cache.close();
            inputFile.close();
        }
    }

    /// A file whose bytes are in memory is read where its load's parse runs: a read on a thread of
    /// its own would only move a slice of the mapping to another thread and back.
    @Test
    void aClaimedLoadOfAFileInMemoryReadsInPlace() throws Exception {
        InputFile inputFile = InputFile.of(Paths.get(LATER_FILE));
        FileMetadataCache cache = cache(inputFile, Runnable::run);

        try {
            FileMetadataCache.ClaimedLoad load = cache.claim(0).load();
            assertThat(load.readsInPlace()).isTrue();
            assertThat(load.parse().metaData().numRows()).isEqualTo(100);
        }
        finally {
            cache.close();
            inputFile.close();
        }
    }

    /// A footer a `MetadataSource` supplies is read on a virtual thread for a file in memory too:
    /// the source may wait for a remote store, which no thread of the pool may.
    @Test
    @Tag("pool")
    void aSuppliedFooterIsReadOnAVirtualThreadForAFileInMemory() throws Exception {
        ParsedFooter footer;
        try (InputFile source = InputFile.of(Paths.get(LATER_FILE))) {
            source.open();
            footer = ParsedFooter.readFrom(source);
        }
        List<Thread> callers = new CopyOnWriteArrayList<>();
        HardwoodContextImpl context = (HardwoodContextImpl) HardwoodContext.builder().threads(1).metadataSource(file -> {
            callers.add(Thread.currentThread());
            return footer;
        }).build();
        contexts.add(context);
        InputFile inputFile = InputFile.of(Paths.get(LATER_FILE));
        FileMetadataCache cache = new FileMetadataCache(List.of(inputFile), context);

        try {
            FileMetadataCache.ClaimedLoad load = cache.claim(0).load();
            assertThat(load.readsInPlace()).isFalse();
            Thread reader = Thread.ofVirtual().start(() -> {
                try {
                    load.read();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            reader.join();
            assertThat(load.parse().metaData().numRows()).isEqualTo(100);
            assertThat(callers).singleElement().satisfies(caller -> assertThat(caller.isVirtual()).isTrue());
        }
        finally {
            cache.close();
            inputFile.close();
        }
    }

    @Test
    void closePreventsLaterLoadsWithoutClosingInputs() throws Exception {
        LatchInputFile inputFile = LatchInputFile.unblocked(LATER_FILE);
        FileMetadataCache cache = cache(inputFile, Runnable::run);

        cache.close();

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
            if (FetchReason.current().startsWith("footer-") && firstFooterRead.compareAndSet(true, false)) {
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
}
