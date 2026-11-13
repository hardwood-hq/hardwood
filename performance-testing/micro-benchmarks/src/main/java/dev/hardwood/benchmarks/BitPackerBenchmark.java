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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import dev.hardwood.internal.encoding.BitPacker;

/// [BitPacker] throughput in nanoseconds per value, in the two shapes the writer calls it in.
///
/// | Benchmark | Call shape | Caller |
/// |---|---|---|
/// | `packLongs` | `pack(long[])`, one group of 1,024 values | ALP's vectors |
/// | `packIntGroups` | `pack(int[])`, 128 groups of eight values | the RLE/bit-packing hybrid's bit-packed runs |
///
/// Values are uniform random at the benchmark's bit width.
///
/// Run with:
/// ```shell
/// java -jar benchmarks.jar BitPackerBenchmark
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 2, jvmArgsAppend = { "-Xms512m", "-Xmx512m" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BitPackerBenchmark {

    private static final int LONG_VALUES = 1024;
    private static final int INT_GROUPS = 128;
    private static final int GROUP_SIZE = 8;

    @State(Scope.Benchmark)
    public static class Longs {

        @Param({ "14", "20", "27", "50" })
        private int bitWidth;

        private final long[] values = new long[LONG_VALUES];
        private final byte[] out = new byte[LONG_VALUES * Long.BYTES];

        @Setup(Level.Trial)
        public void setUp() {
            Random random = new Random(42);
            for (int i = 0; i < LONG_VALUES; i++) {
                values[i] = random.nextLong() & ((1L << bitWidth) - 1);
            }
        }
    }

    @State(Scope.Benchmark)
    public static class IntGroups {

        @Param({ "1", "3", "8", "17" })
        private int bitWidth;

        private final int[] values = new int[INT_GROUPS * GROUP_SIZE];
        private final byte[] out = new byte[INT_GROUPS * GROUP_SIZE * Integer.BYTES];

        @Setup(Level.Trial)
        public void setUp() {
            Random random = new Random(42);
            for (int i = 0; i < values.length; i++) {
                values[i] = random.nextInt(1 << bitWidth);
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(LONG_VALUES)
    public int packLongs(Longs state) {
        return BitPacker.pack(state.values, 0, LONG_VALUES, state.bitWidth, state.out, 0);
    }

    @Benchmark
    @OperationsPerInvocation(INT_GROUPS * GROUP_SIZE)
    public int packIntGroups(IntGroups state) {
        int pos = 0;
        for (int group = 0; group < INT_GROUPS; group++) {
            pos += BitPacker.pack(state.values, group * GROUP_SIZE, GROUP_SIZE, state.bitWidth, state.out, pos);
        }
        return pos;
    }
}
