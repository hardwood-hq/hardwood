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
