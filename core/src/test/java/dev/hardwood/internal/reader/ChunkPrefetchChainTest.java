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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;

import static org.assertj.core.api.Assertions.assertThat;

/// A chained [ChunkHandle] or [SharedRegion] prefetches its successor on its first demand
/// access, including when a prefetch already fetched it, so prefetch stays one ahead along the
/// whole chain without running further ahead (#1332).
class ChunkPrefetchChainTest {

    private static final int CHUNK = 100;
    private static final int CHUNKS = 4;

    @Test
    void everyChunkHandlePrefetchesItsSuccessor() throws Exception {
        ReadSignals signals = new ReadSignals();
        CountingInputFile file = new CountingInputFile(signals);
        PrefetchTasks prefetchTasks = new PrefetchTasks();
        ChunkHandle[] handles = new ChunkHandle[CHUNKS];
        for (int i = 0; i < CHUNKS; i++) {
            handles[i] = new ChunkHandle(file, (long) i * CHUNK, CHUNK, "chunk " + i, prefetchTasks);
        }
        for (int i = 0; i < CHUNKS - 1; i++) {
            handles[i].setNextChunk(handles[i + 1]);
        }

        // Each handle is reached only once its predecessor's access has prefetched it.
        for (int i = 0; i < CHUNKS - 1; i++) {
            handles[i].ensureFetched();
            assertThat(signals.awaitRead((long) (i + 1) * CHUNK))
                    .as("prefetch of chunk %d after the access to chunk %d", i + 1, i)
                    .isTrue();
            assertReadUpTo(file, i + 1);
        }
        handles[CHUNKS - 1].ensureFetched();

        assertThat(file.reads())
                .extracting(CountingInputFile.Read::offset)
                .containsExactly(0L, 100L, 200L, 300L);
    }

    @Test
    void everySharedRegionPrefetchesItsSuccessor() throws Exception {
        ReadSignals signals = new ReadSignals();
        CountingInputFile file = new CountingInputFile(signals);
        PrefetchTasks prefetchTasks = new PrefetchTasks();
        SharedRegion[] regions = new SharedRegion[CHUNKS];
        for (int i = 0; i < CHUNKS; i++) {
            regions[i] = new SharedRegion(file, (long) i * CHUNK, CHUNK, "region " + i, prefetchTasks);
        }
        for (int i = 0; i < CHUNKS - 1; i++) {
            regions[i].setNextRegion(regions[i + 1]);
        }

        for (int i = 0; i < CHUNKS - 1; i++) {
            regions[i].ensureFetched();
            assertThat(signals.awaitRead((long) (i + 1) * CHUNK))
                    .as("prefetch of region %d after the access to region %d", i + 1, i)
                    .isTrue();
            assertReadUpTo(file, i + 1);
        }
        regions[CHUNKS - 1].ensureFetched();

        assertThat(file.reads())
                .extracting(CountingInputFile.Read::offset)
                .containsExactly(0L, 100L, 200L, 300L);
    }

    /// Asserts, once the prefetch tasks have run, that links `0..last` and none past them have
    /// been read: a prefetch fetches only its own link and does not cascade along the chain.
    private static void assertReadUpTo(CountingInputFile file, int last) {
        assertThat(ForkJoinPool.commonPool().awaitQuiescence(5, TimeUnit.SECONDS)).isTrue();
        assertThat(file.reads())
                .extracting(CountingInputFile.Read::offset)
                .as("reads once link %d is prefetched", last)
                .containsExactly(LongStream.rangeClosed(0, last).map(i -> i * CHUNK).boxed().toArray(Long[]::new));
    }

    /// A file of `CHUNKS * CHUNK` bytes that signals each read by its offset.
    private static final class ReadSignals implements InputFile {

        private final InputFile delegate = InputFile.of(ByteBuffer.allocate(CHUNKS * CHUNK));
        private final ConcurrentHashMap<Long, CountDownLatch> reads = new ConcurrentHashMap<>();

        /// Waits at most five seconds for a read at `offset`.
        boolean awaitRead(long offset) throws InterruptedException {
            return latch(offset).await(5, TimeUnit.SECONDS);
        }

        private CountDownLatch latch(long offset) {
            return reads.computeIfAbsent(offset, o -> new CountDownLatch(1));
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            ByteBuffer data = delegate.readRange(offset, length);
            latch(offset).countDown();
            return data;
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return "chunks";
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
