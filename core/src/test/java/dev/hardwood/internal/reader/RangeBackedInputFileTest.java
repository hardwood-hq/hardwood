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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RangeBackedInputFileTest {

    /// In-memory delegate that hands out slices of a fixed byte[]. Counts
    /// readRange invocations so tests can assert cache hits.
    private static final class CountingDelegate implements InputFile {
        private final byte[] data;
        private final AtomicInteger reads = new AtomicInteger();

        CountingDelegate(byte[] data) {
            this.data = data;
        }

        int readCount() {
            return reads.get();
        }

        @Override public void open() {
        }

        @Override public ByteBuffer readRange(long offset, int length) {
            reads.incrementAndGet();
            return ByteBuffer.wrap(data, Math.toIntExact(offset), length);
        }

        @Override public long length() {
            return data.length;
        }

        @Override public String name() {
            return "fake";
        }

        @Override public void close() {
        }
    }

    private static byte[] makeData(int size) {
        byte[] b = new byte[size];
        for (int i = 0; i < size; i++) {
            b[i] = (byte) (i & 0xff);
        }
        return b;
    }

    @Test
    void repeatReadOfSameRangeHitsCache(@TempDir Path tempDir) throws IOException {
        byte[] data = makeData(4096);
        CountingDelegate inner = new CountingDelegate(data);
        try (RangeBackedInputFile cached = new RangeBackedInputFile(inner, tempDir)) {
            cached.open();
            ByteBuffer first = cached.readRange(100, 200);
            int afterFirst = inner.readCount();
            ByteBuffer second = cached.readRange(100, 200);

            assertThat(inner.readCount())
                    .as("repeat read should hit the cache")
                    .isEqualTo(afterFirst);
            assertThat(toBytes(second)).isEqualTo(toBytes(first));
        }
    }

    @Test
    void subRangeOfCachedRangeHitsCache(@TempDir Path tempDir) throws IOException {
        byte[] data = makeData(4096);
        CountingDelegate inner = new CountingDelegate(data);
        try (RangeBackedInputFile cached = new RangeBackedInputFile(inner, tempDir)) {
            cached.open();
            cached.readRange(0, 1000);
            int afterFirst = inner.readCount();

            cached.readRange(100, 200);

            assertThat(inner.readCount())
                    .as("sub-range read should be served from the cache")
                    .isEqualTo(afterFirst);
        }
    }

    @Test
    void gapBetweenCachedRangesIsFetched(@TempDir Path tempDir) throws IOException {
        byte[] data = makeData(4096);
        CountingDelegate inner = new CountingDelegate(data);
        try (RangeBackedInputFile cached = new RangeBackedInputFile(inner, tempDir)) {
            cached.open();
            cached.readRange(0, 100);
            cached.readRange(200, 100);
            int afterTwo = inner.readCount();
            assertThat(afterTwo).isEqualTo(2);

            // Reading [0, 300) should fetch only the [100, 200) gap.
            cached.readRange(0, 300);

            assertThat(inner.readCount())
                    .as("only the gap should be fetched")
                    .isEqualTo(afterTwo + 1);
        }
    }

    @Test
    void readReturnsCorrectBytesAfterPartialFill(@TempDir Path tempDir) throws IOException {
        byte[] data = makeData(4096);
        CountingDelegate inner = new CountingDelegate(data);
        try (RangeBackedInputFile cached = new RangeBackedInputFile(inner, tempDir)) {
            cached.open();
            // Populate two disjoint regions, then read a span covering both.
            cached.readRange(100, 50);
            cached.readRange(300, 50);
            ByteBuffer span = cached.readRange(50, 350);

            byte[] out = toBytes(span);
            // Verify byte-correctness across the previously-cached and
            // newly-fetched portions.
            for (int i = 0; i < out.length; i++) {
                assertThat(out[i]).isEqualTo((byte) ((50 + i) & 0xff));
            }
        }
    }

    @Test
    void offsetOrLengthOutsideFileThrows(@TempDir Path tempDir) throws IOException {
        CountingDelegate inner = new CountingDelegate(makeData(100));
        try (RangeBackedInputFile cached = new RangeBackedInputFile(inner, tempDir)) {
            cached.open();
            assertThatThrownBy(() -> cached.readRange(-1, 10))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> cached.readRange(50, 100))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void closeDeletesTempFile(@TempDir Path tempDir) throws IOException {
        CountingDelegate inner = new CountingDelegate(makeData(4096));
        long beforeOpen = countCacheFiles(tempDir);
        RangeBackedInputFile cached = new RangeBackedInputFile(inner, tempDir);
        cached.open();
        assertThat(countCacheFiles(tempDir)).isEqualTo(beforeOpen + 1);
        cached.close();
        assertThat(countCacheFiles(tempDir))
                .as("temp file should be deleted on close")
                .isEqualTo(beforeOpen);
    }

    private static long countCacheFiles(Path dir) throws IOException {
        try (var stream = Files.list(dir)) {
            return stream.filter(p -> p.getFileName().toString().startsWith("hardwood-range-")).count();
        }
    }

    private static byte[] toBytes(ByteBuffer buf) {
        ByteBuffer dup = buf.duplicate();
        byte[] out = new byte[dup.remaining()];
        dup.get(out);
        return out;
    }
}
