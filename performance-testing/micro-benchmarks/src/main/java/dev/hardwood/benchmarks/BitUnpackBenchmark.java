/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.util.Random;
import java.util.concurrent.TimeUnit;

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

import dev.hardwood.internal.encoding.RleBitPackingHybridDecoder;
import dev.hardwood.internal.encoding.RleBitPackingHybridEncoder;

/// Measures bit-packed index decode throughput across bit widths.
///
/// This is the dictionary-index and definition-level path. `decodeBitPacked` has bulk paths for
/// a bit width of 1, for widths up to 8, for each width from 9 to 16 and for widths above 16, so
/// the sweep covers every one of them. Dictionaries of more than 256 entries need 9 bits or more.
/// `chunk` decodes the values in calls of the size a dictionary fill makes, or of a size that ends
/// inside a group of eight; `data` sets whether they form one long bit-packed run or short
/// bit-packed runs between RLE runs, where the cost of entering each run counts.
///
/// Run with:
/// ```shell
/// java -jar benchmarks.jar BitUnpackBenchmark
/// ```
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms512m", "-Xmx512m", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BitUnpackBenchmark {

    /// Values decoded per invocation.
    @Param({ "4096" })
    private int size;

    @Param({ "1", "4", "8", "9", "10", "11", "12", "13", "14", "15", "16", "20", "24", "32" })
    private int bitWidth;

    /// Values decoded per `readInts` call: the chunks a dictionary fill decodes, or chunks that
    /// end inside a group of eight, as a batch's bounds can.
    @Param({ "1024", "1000" })
    private int chunk;

    /// `random` draws every value independently, so the encoder writes one bit-packed run.
    /// `runs` alternates stretches of 8 to 120 independent values with repeats of one value
    /// 8 to 40 long, so short bit-packed runs alternate with RLE runs.
    @Param({ "random", "runs" })
    private String data;

    private byte[] packed;
    private int[] output;

    @Setup
    public void setUp() {
        output = new int[size];

        int[] values = new int[size];
        Random random = new Random(42);
        // A width of 32 cannot use nextInt(1 << 32); mask instead so every width is generated
        // the same way, and so the encoder sees distinct values and emits bit-packed runs.
        int mask = bitWidth == 32 ? -1 : (1 << bitWidth) - 1;
        if (data.equals("random")) {
            for (int i = 0; i < size; i++) {
                values[i] = random.nextInt() & mask;
            }
        }
        else if (data.equals("runs")) {
            int i = 0;
            while (i < size) {
                int end = Math.min(size, i + 8 + random.nextInt(113));
                for (; i < end; i++) {
                    values[i] = random.nextInt() & mask;
                }
                end = Math.min(size, i + 8 + random.nextInt(33));
                int repeated = random.nextInt() & mask;
                for (; i < end; i++) {
                    values[i] = repeated;
                }
            }
        }
        else {
            throw new IllegalArgumentException("Unknown data shape: " + data);
        }

        RleBitPackingHybridEncoder encoder = new RleBitPackingHybridEncoder(bitWidth);
        encoder.writeInts(values, 0, size);
        packed = encoder.toByteArray();

        verify(values);
    }

    /// Decode once and check the values round-trip, so a decoder that stops early cannot post
    /// a good score, and confirm the encoder chose bit-packed rather than run-length runs.
    private void verify(int[] expected) {
        decodeBitPacked();
        for (int i = 0; i < size; i++) {
            if (output[i] != expected[i]) {
                throw new IllegalStateException("Mismatch at " + i + " for bit width " + bitWidth
                        + ": expected " + expected[i] + ", got " + output[i]);
            }
        }

        int bitPackedBytes = (size * bitWidth + 7) / 8;
        if (data.equals("random") && packed.length < bitPackedBytes / 2) {
            throw new IllegalStateException("Stream collapsed to run-length runs: " + packed.length
                    + " bytes for " + size + " values of " + bitWidth + " bits");
        }
    }

    @Benchmark
    public int[] decodeBitPacked() {
        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(packed, bitWidth);
        for (int pos = 0; pos < size; pos += chunk) {
            decoder.readInts(output, pos, Math.min(chunk, size - pos));
        }
        return output;
    }
}
