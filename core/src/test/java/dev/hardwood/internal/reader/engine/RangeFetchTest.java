/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.fetch.FileRange;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RangeFetchTest {

    private static final byte[] MARKER = "BEYOND-2GB-MARK!".getBytes(StandardCharsets.US_ASCII);
    private static final long OFFSET = (long) Integer.MAX_VALUE + 4096;

    /// A local file larger than 2 GB is mapped range by range, and still holds every byte: its
    /// range arrives in place, mapped where it is listed, with no fetch job to wait for.
    @Test
    void aRangeOfAFileMappedByRegionArrivesInPlace(@TempDir Path tmpDir) throws Exception {
        try (InputFile file = InputFile.of(largeFile(tmpDir))) {
            file.open();

            RangeFetch range = RangeFetch.of(file, new FileRange(OFFSET, MARKER.length, "test"));

            assertThat(range.output().hasValue()).isTrue();
            ByteBuffer bytes = range.output().value();
            byte[] read = new byte[MARKER.length];
            bytes.get(read);
            assertThat(read).isEqualTo(MARKER);
        }
    }

    /// A range whose map in place fails is left to its fetch, neither arrived nor failed. Mapping
    /// on an interrupted thread fails and closes the file's channel for every later read.
    @Test
    void aRangeWhoseMapInPlaceFailsIsLeftToItsFetch(@TempDir Path tmpDir) throws Exception {
        try (InputFile file = InputFile.of(largeFile(tmpDir))) {
            file.open();

            RangeFetch range;
            Thread.currentThread().interrupt();
            try {
                range = RangeFetch.of(file, new FileRange(OFFSET, MARKER.length, "test"));
            }
            finally {
                Thread.interrupted();
            }

            assertThat(range.output().hasValue()).isFalse();
            assertThat(range.output().ended()).isFalse();
            assertThatThrownBy(() -> file.readRange(OFFSET, MARKER.length))
                    .isExactlyInstanceOf(ClosedChannelException.class);
        }
    }

    /// A sparse file larger than 2 GB, so mapped range by range, with [#MARKER] at [#OFFSET].
    /// A read is answered from a range that holds it whatever the ranges' order, including one a
    /// later range lies inside, as a footer laying one page-index structure over another makes.
    @Test
    void aReadHeldByARangeAnotherLiesInsideIsSliced() {
        List<RangeFetch> ranges = List.of(arrived(237, 470), arrived(342, 357));

        assertThat(RangeFetch.read(ranges, 237, 233)).isEqualTo(region(237, 470));
    }

    @Test
    void aReadStraddlingTwoRangesIsCopied() {
        List<RangeFetch> ranges = List.of(arrived(10, 20), arrived(0, 10));

        assertThat(RangeFetch.read(ranges, 5, 10)).isEqualTo(region(5, 15));
    }

    /// A read across a gap between the ranges is not held by them, rather than copied with the
    /// gap's bytes left as zeros.
    @Test
    void aReadAcrossAGapIsNotHeld() {
        List<RangeFetch> ranges = List.of(arrived(0, 10), arrived(12, 20));

        assertThat(RangeFetch.read(ranges, 5, 10)).isNull();
    }

    /// The bytes `[start, end)` of a file whose byte at `i` is `i`, as a range that has arrived.
    private static RangeFetch arrived(long start, long end) {
        RangeFetch range = new RangeFetch(new FileRange(start, Math.toIntExact(end - start), "test"));
        range.output().arrived(region(start, end));
        return range;
    }

    private static ByteBuffer region(long start, long end) {
        byte[] bytes = new byte[Math.toIntExact(end - start)];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (start + i);
        }
        return ByteBuffer.wrap(bytes);
    }

    private static Path largeFile(Path tmpDir) throws IOException {
        Path path = tmpDir.resolve("large.bin");
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            // sparse: the hole before the marker takes no blocks
            raf.setLength(OFFSET + MARKER.length);
            raf.seek(OFFSET);
            raf.write(MARKER);
        }
        return path;
    }
}
