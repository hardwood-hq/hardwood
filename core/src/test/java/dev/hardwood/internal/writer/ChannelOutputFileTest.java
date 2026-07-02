/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/// Buffering behaviour of [ChannelOutputFile]: writes are coalesced but must reach the
/// file byte-for-byte and in order, whether they are buffered, span a flush boundary, or
/// bypass the buffer entirely.
class ChannelOutputFileTest {

    @Test
    void mixedSmallAndLargeWritesConcatenateInOrder(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("out.bin");
        byte[] small = filled(10, (byte) 1);
        byte[] large = ramp(200_000); // larger than the 64 KiB buffer -> bypasses it
        byte[] tail = filled(5, (byte) 2);

        ChannelOutputFile out = new ChannelOutputFile(file);
        out.create();
        out.write(ByteBuffer.wrap(small));
        assertThat(out.position()).isEqualTo(10);
        out.write(ByteBuffer.wrap(large));
        out.write(ByteBuffer.wrap(tail));
        assertThat(out.position()).isEqualTo(10L + large.length + 5);
        out.close();

        byte[] all = Files.readAllBytes(file);
        assertThat(all).hasSize(10 + large.length + 5);
        assertThat(Arrays.copyOfRange(all, 0, 10)).isEqualTo(small);
        assertThat(Arrays.copyOfRange(all, 10, 10 + large.length)).isEqualTo(large);
        assertThat(Arrays.copyOfRange(all, 10 + large.length, all.length)).isEqualTo(tail);
    }

    @Test
    void manySmallWritesFlushAcrossBufferBoundary(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("out.bin");
        ByteArrayOutputStream expected = new ByteArrayOutputStream();

        ChannelOutputFile out = new ChannelOutputFile(file);
        out.create();
        // 200 KiB of 1 KiB writes forces several buffer flushes.
        for (int i = 0; i < 200; i++) {
            byte[] chunk = filled(1024, (byte) i);
            expected.writeBytes(chunk);
            out.write(ByteBuffer.wrap(chunk));
        }
        out.close();

        assertThat(Files.readAllBytes(file)).isEqualTo(expected.toByteArray());
    }

    @Test
    void discardDropsBufferedBytesAndLeavesNoFile(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("out.bin");
        ChannelOutputFile out = new ChannelOutputFile(file);
        out.create();
        out.write(ByteBuffer.wrap(filled(100, (byte) 7))); // buffered, never flushed
        out.discard();

        assertThat(Files.exists(file)).isFalse();
        assertThat(Files.exists(dir.resolve("out.bin.hardwood-tmp"))).isFalse();
    }

    private static byte[] filled(int size, byte value) {
        byte[] b = new byte[size];
        Arrays.fill(b, value);
        return b;
    }

    private static byte[] ramp(int size) {
        byte[] b = new byte[size];
        for (int i = 0; i < size; i++) {
            b[i] = (byte) i;
        }
        return b;
    }
}
