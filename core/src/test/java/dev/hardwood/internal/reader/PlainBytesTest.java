/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.PhysicalType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Plain values copied into a batch's chunks ([PlainBytes]) through each of the copy paths of
/// [BinaryPageValues#copyPlain] across a chunk boundary: a value never spans two chunks, and no
/// chunk is larger than [BinaryBatchValues#CHUNK_BYTES].
class PlainBytesTest {

    private static final int WIDTH = 12;
    /// Enough twelve-byte values for two chunks, and a width that does not divide a chunk.
    private static final int VALUES = BinaryBatchValues.CHUNK_BYTES / WIDTH + 100;

    @Test
    void fixedWidthValuesFillChunksAcrossTheBoundary() {
        byte[] data = fixedWidthValues(VALUES);
        BinaryPageValues page = BinaryPageValues.plain(PhysicalType.FIXED_LEN_BYTE_ARRAY, WIDTH, null, 0, VALUES,
                data, 0, data.length, new PageDecoder.LevelScratch());

        BinaryBatchValues out = copy(page, VALUES);

        assertValues(out, VALUES);
        assertThat(out.bufferCount()).isEqualTo(1 + 2);
    }

    @Test
    void byteStreamSplitValuesFillChunksAcrossTheBoundary() {
        byte[] values = fixedWidthValues(VALUES);
        byte[] split = new byte[values.length];
        for (int v = 0; v < VALUES; v++) {
            for (int b = 0; b < WIDTH; b++) {
                split[b * VALUES + v] = values[v * WIDTH + b];
            }
        }
        BinaryPageValues page = BinaryPageValues.byteStreamSplit(WIDTH, null, 0, VALUES, split, 0, split.length,
                new PageDecoder.LevelScratch());

        BinaryBatchValues out = copy(page, VALUES);

        assertValues(out, VALUES);
        assertThat(out.bufferCount()).isEqualTo(1 + 2);
    }

    /// The segments of a nested cell copy into one [PlainBytes] in turn; the second continues the
    /// first's chunk and starts the next where the chunk ends.
    @Test
    void consecutiveFillsShareChunksAcrossTheBoundary() {
        byte[] data = fixedWidthValues(VALUES);
        BinaryPageValues page = BinaryPageValues.plain(PhysicalType.FIXED_LEN_BYTE_ARRAY, WIDTH, null, 0, VALUES,
                data, 0, data.length, new PageDecoder.LevelScratch());
        BinaryBatchValues out = new BinaryBatchValues(VALUES);
        out.prepareForFill();
        out.installViews();
        PlainBytes dest = new PlainBytes(out, (long) VALUES * WIDTH);
        int half = VALUES / 2;
        page.view().copyPlain(0, half, out.fillStarts(), out.fillEnds(), out.fillBufferIds(), 0, dest);
        page.view().copyPlain(half, VALUES - half, out.fillStarts(), out.fillEnds(), out.fillBufferIds(), half, dest);
        out.finishFill(VALUES, new BinaryBatchValues.ViewDemand());

        assertValues(out, VALUES);
        assertThat(out.bufferCount()).isEqualTo(1 + 2);
    }

    /// A batch compacted from a holder keeps reading its values after the holder is refilled.
    @Test
    void aCompactedBatchKeepsItsBytesWhenItsHolderIsRefilled() {
        BinaryBatchValues holder = new BinaryBatchValues(3);
        fill(holder, "alpha", "bravo", "charlie");
        BinaryBatchValues kept = holder.compact(new int[]{ 0, 2 }, 2);

        fill(holder, "delta", "echo", "foxtrot");

        assertThat(string(kept, 0)).isEqualTo("alpha");
        assertThat(string(kept, 1)).isEqualTo("charlie");
        assertThat(string(holder, 1)).isEqualTo("echo");
    }

    /// A recycled holder's batch keeps its full chunks and fills them again, and only those: a
    /// value longer than a chunk gets a buffer of its own, which is not kept.
    @Test
    void aHolderThatKeepsChunksFillsThemAgain() {
        BinaryBatchValues holder = new BinaryBatchValues(3);
        holder.keepChunks();
        String half = "h".repeat(BinaryBatchValues.CHUNK_BYTES / 2);
        String longValue = "y".repeat(BinaryBatchValues.CHUNK_BYTES + 1);
        fill(holder, half, longValue, half);
        // The long value takes a buffer of its own; the value after it starts a second chunk
        byte[] first = holder.buffer(0);
        byte[] second = holder.buffer(2);
        assertThat(first).hasSize(BinaryBatchValues.CHUNK_BYTES).isNotSameAs(second);
        assertThat(second).hasSize(BinaryBatchValues.CHUNK_BYTES);
        assertThat(holder.buffer(1)).hasSize(BinaryBatchValues.CHUNK_BYTES + 1);

        fill(holder, half, "echo", "foxtrot");

        assertThat(holder.buffer(0)).isIn(first, second);
        assertThat(holder.buffer(1)).isSameAs(holder.buffer(0));
        assertThat(holder.bufferCount()).isEqualTo(1 + 1);
        assertThat(string(holder, 1)).isEqualTo("echo");
        assertThat(string(holder, 2)).isEqualTo("foxtrot");
    }

    /// A fill of fewer bytes than half a chunk takes no more than them, even in a batch that keeps
    /// its chunks, so that small segments do not each hold a full chunk.
    @Test
    void aSmallFillTakesOnlyItsBytesInABatchThatKeepsChunks() {
        BinaryBatchValues holder = new BinaryBatchValues(3);
        holder.keepChunks();

        fill(holder, "alpha", "bravo", "charlie");

        assertThat(holder.buffer(0)).hasSize("alphabravocharlie".length());
        assertThat(string(holder, 2)).isEqualTo("charlie");
    }

    /// A holder keeps the chunks of its last fill only, not the spares an earlier, larger fill left.
    @Test
    void aHolderKeepsTheChunksOfItsLastFillOnly() {
        BinaryBatchValues holder = new BinaryBatchValues(3);
        holder.keepChunks();
        String full = "z".repeat(BinaryBatchValues.CHUNK_BYTES);
        fill(holder, full, full, full);
        Set<byte[]> first = Collections.newSetFromMap(new IdentityHashMap<>());
        for (int i = 0; i < 3; i++) {
            first.add(holder.buffer(i));
        }
        fill(holder, full, "a", "b");

        fill(holder, full, full, full);

        // The second fill reused one of the first fill's chunks; the other two were dropped
        int fromFirst = 0;
        for (int i = 0; i < 3; i++) {
            fromFirst += first.contains(holder.buffer(i)) ? 1 : 0;
        }
        assertThat(fromFirst).isEqualTo(1);
    }

    @Test
    void aBatchThatKeepsChunksIsNotCompacted() {
        BinaryBatchValues holder = new BinaryBatchValues(2);
        holder.keepChunks();
        fill(holder, "alpha", "bravo");

        assertThatThrownBy(() -> holder.compact(new int[]{ 0 }, 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("A batch that keeps its chunks for its next fill cannot be compacted");
    }

    private static BinaryBatchValues copy(BinaryPageValues page, int values) {
        BinaryBatchValues out = new BinaryBatchValues(values);
        out.prepareForFill();
        out.installViews();
        page.copyPlain(0, values, out.fillStarts(), out.fillEnds(), out.fillBufferIds(), 0,
                new PlainBytes(out, page.bytes(0, values)));
        out.finishFill(values, new BinaryBatchValues.ViewDemand());
        return out;
    }

    private static void fill(BinaryBatchValues values, String... entries) {
        values.prepareForFill();
        values.installViews();
        long total = 0;
        for (String entry : entries) {
            total += entry.length();
        }
        PlainBytes dest = new PlainBytes(values, total);
        for (int i = 0; i < entries.length; i++) {
            byte[] bytes = entries[i].getBytes(StandardCharsets.UTF_8);
            System.arraycopy(bytes, 0, dest.room(bytes.length), dest.position(), bytes.length);
            values.fillBufferIds()[i] = dest.id();
            values.fillStarts()[i] = dest.position();
            values.fillEnds()[i] = dest.position() + bytes.length;
            dest.advance(bytes.length);
        }
        values.finishFill(entries.length, new BinaryBatchValues.ViewDemand());
    }

    private static String string(BinaryBatchValues values, int i) {
        return new String(values.buffer(i), values.starts()[i], values.ends()[i] - values.starts()[i],
                StandardCharsets.UTF_8);
    }

    /// Value `v` is twelve bytes, each `v + b` truncated to a byte, so neighbours differ.
    private static byte[] fixedWidthValues(int count) {
        byte[] data = new byte[count * WIDTH];
        for (int v = 0; v < count; v++) {
            for (int b = 0; b < WIDTH; b++) {
                data[v * WIDTH + b] = (byte) (v + b);
            }
        }
        return data;
    }

    private static void assertValues(BinaryBatchValues out, int count) {
        byte[][] buffers = out.buffers();
        int[] bufferIds = out.bufferIds();
        int[] starts = out.starts();
        int[] ends = out.ends();
        byte[] expected = fixedWidthValues(count);
        for (int v = 0; v < count; v++) {
            byte[] buffer = buffers[bufferIds[v]];
            assertThat(buffer.length).as("buffer of value %d", v).isLessThanOrEqualTo(BinaryBatchValues.CHUNK_BYTES);
            assertThat(Arrays.copyOfRange(buffer, starts[v], ends[v])).as("value %d", v)
                    .isEqualTo(Arrays.copyOfRange(expected, v * WIDTH, (v + 1) * WIDTH));
        }
    }
}
