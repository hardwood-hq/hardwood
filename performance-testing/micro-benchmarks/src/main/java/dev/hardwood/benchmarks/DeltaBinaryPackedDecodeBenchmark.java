/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import dev.hardwood.internal.encoding.DeltaBinaryPackedDecoder;
import dev.hardwood.internal.encoding.RleBitPackingHybridDecoder;
import dev.hardwood.internal.encoding.RleBitPackingHybridEncoder;

/// Measures DELTA_BINARY_PACKED decode throughput for INT32 and INT64 columns.
///
/// `rleBitPackedInts` decodes the same value count at the same bit width through
/// [RleBitPackingHybridDecoder], which unpacks in bulk. It is the reference point
/// for how fast unpacking that many values of that width can go; the ratio between
/// it and `deltaInts` is the headroom in [DeltaBinaryPackedDecoder].
///
/// The encoded input is produced by parquet-java's writers rather than by hand, so
/// the block and miniblock layout is the one Hardwood sees in real files.
///
/// Run with:
/// ```shell
/// java -jar benchmarks.jar DeltaBinaryPackedDecodeBenchmark
/// ```
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms512m", "-Xmx512m" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class DeltaBinaryPackedDecodeBenchmark {

    /// Parquet's default DELTA_BINARY_PACKED block layout: 128 values per block,
    /// 4 miniblocks of 32 values each.
    private static final int BLOCK_VALUES = 128;
    private static final int MINIBLOCKS = 4;

    private static final int SLAB_SIZE = 64 * 1024;
    private static final int PAGE_SIZE = 1024 * 1024;

    /// Values decoded per invocation, i.e. the size of one decoded page.
    @Param({ "4096" })
    private int size;

    /// Bits each delta needs once min_delta has been subtracted. This is what drives
    /// the iteration count of the bit-at-a-time unpacking loop, so it is the axis
    /// along which the decoders are expected to diverge.
    @Param({ "1", "5", "12", "21" })
    private int deltaBits;

    private byte[] deltaInt32Data;
    private byte[] deltaInt64Data;
    private byte[] rleData;

    private int[] intOutput;
    private long[] longOutput;

    @Setup
    public void setUp() throws IOException {
        intOutput = new int[size];
        longOutput = new long[size];

        int[] expectedInts = generateValues(size, deltaBits);
        deltaInt32Data = encodeInt32Deltas(expectedInts);
        deltaInt64Data = encodeInt64Deltas(expectedInts);
        rleData = encodeBitPacked(size, deltaBits);

        verify(expectedInts);
    }

    /// Decode each input once and check it round-trips, so a decoder that stops early
    /// or misreads the layout cannot quietly post a good score.
    private void verify(int[] expectedInts) throws IOException {
        deltaInts();
        for (int i = 0; i < size; i++) {
            if (intOutput[i] != expectedInts[i]) {
                throw new IllegalStateException("DELTA_BINARY_PACKED INT32 mismatch at " + i
                        + ": expected " + expectedInts[i] + ", got " + intOutput[i]);
            }
        }

        deltaLongs();
        for (int i = 0; i < size; i++) {
            if (longOutput[i] != expectedInts[i]) {
                throw new IllegalStateException("DELTA_BINARY_PACKED INT64 mismatch at " + i
                        + ": expected " + expectedInts[i] + ", got " + longOutput[i]);
            }
        }

        // The RLE reference is only a fair yardstick if the encoder chose bit-packed
        // runs; an all-RLE stream would decode far less data than it appears to.
        int bitPackedBytes = (size * deltaBits + 7) / 8;
        if (rleData.length < bitPackedBytes / 2) {
            throw new IllegalStateException("RLE reference collapsed to run-length runs: "
                    + rleData.length + " bytes for " + size + " values of " + deltaBits + " bits");
        }
    }

    @Benchmark
    public int[] deltaInts() throws IOException {
        DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(deltaInt32Data, 0);
        decoder.readInts(intOutput, null, 0);
        return intOutput;
    }

    @Benchmark
    public long[] deltaLongs() throws IOException {
        DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(deltaInt64Data, 0);
        decoder.readLongs(longOutput, null, 0);
        return longOutput;
    }

    @Benchmark
    public int[] rleBitPackedInts() {
        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(rleData, deltaBits);
        decoder.readInts(intOutput, 0, size);
        return intOutput;
    }

    /// Generate an ascending sequence whose successive deltas occupy `deltaBits` bits.
    private static int[] generateValues(int count, int deltaBits) {
        Random random = new Random(42);
        int bound = 1 << deltaBits;

        int[] values = new int[count];
        int value = 0;
        for (int i = 0; i < count; i++) {
            values[i] = value;
            value += random.nextInt(bound);
        }
        return values;
    }

    private static byte[] encodeInt32Deltas(int[] values) throws IOException {
        try (DeltaBinaryPackingValuesWriterForInteger writer = new DeltaBinaryPackingValuesWriterForInteger(
                BLOCK_VALUES, MINIBLOCKS, SLAB_SIZE, PAGE_SIZE, HeapByteBufferAllocator.getInstance())) {
            for (int value : values) {
                writer.writeInteger(value);
            }
            return writer.getBytes().toByteArray();
        }
    }

    private static byte[] encodeInt64Deltas(int[] values) throws IOException {
        try (DeltaBinaryPackingValuesWriterForLong writer = new DeltaBinaryPackingValuesWriterForLong(
                BLOCK_VALUES, MINIBLOCKS, SLAB_SIZE, PAGE_SIZE, HeapByteBufferAllocator.getInstance())) {
            for (int value : values) {
                writer.writeLong(value);
            }
            return writer.getBytes().toByteArray();
        }
    }

    /// Encode `count` values of `bitWidth` bits as an RLE/bit-packing hybrid run.
    /// Values are drawn at random so the encoder emits bit-packed rather than RLE runs.
    private static byte[] encodeBitPacked(int count, int bitWidth) {
        Random random = new Random(42);
        int bound = 1 << bitWidth;

        RleBitPackingHybridEncoder encoder = new RleBitPackingHybridEncoder(bitWidth);
        for (int i = 0; i < count; i++) {
            encoder.writeInt(random.nextInt(bound));
        }
        return encoder.toByteArray();
    }
}
