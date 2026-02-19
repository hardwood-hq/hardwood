/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.util.BitSet;
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
import org.openjdk.jmh.infra.Blackhole;

import dev.hardwood.internal.encoding.simd.ScalarOperations;
import dev.hardwood.internal.encoding.simd.SimdOperations;
import dev.hardwood.internal.encoding.simd.VectorSupport;

/**
 * Benchmark for SIMD operations comparing scalar vs vectorized implementations.
 *
 * <p>Run with:</p>
 * <pre>
 * java --add-modules jdk.incubator.vector -jar benchmarks.jar SimdBenchmark
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms512m", "-Xmx512m", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class SimdBenchmark {

    @Param({"128", "1024", "8192", "65536"})
    private int size;

    @Param({"scalar", "auto"})
    private String implementation;

    private int[] defLevels;
    private int[] indices;
    private int[] dictInt;
    private long[] dictLong;
    private byte[] packedData;
    private SimdOperations ops;

    @Setup
    public void setup() {
        // Print SIMD availability info
        System.out.println("SIMD implementation: " + VectorSupport.implementationName());
        System.out.println("SIMD available: " + VectorSupport.isAvailable());

        // Select implementation
        ops = "scalar".equals(implementation) ? new ScalarOperations() : VectorSupport.operations();

        // Generate test data
        Random random = new Random(42);

        // Definition levels with ~25% each of 0, 1, 2, 3
        defLevels = new int[size];
        for (int i = 0; i < size; i++) {
            defLevels[i] = random.nextInt(4);
        }

        // Dictionary indices (small dictionary)
        dictInt = new int[] { 100, 200, 300, 400, 500, 600, 700, 800 };
        dictLong = new long[] { 100L, 200L, 300L, 400L, 500L, 600L, 700L, 800L };
        indices = new int[size];
        for (int i = 0; i < size; i++) {
            indices[i] = random.nextInt(dictInt.length);
        }

        // Packed bit data for bit-width 1 (one byte = 8 values)
        packedData = new byte[size / 8 + 10];
        random.nextBytes(packedData);
    }

    @Benchmark
    public int countNonNulls() {
        return ops.countNonNulls(defLevels, 3);
    }

    @Benchmark
    public void markNulls(Blackhole bh) {
        BitSet nulls = new BitSet(size);
        ops.markNulls(nulls, defLevels, 0, 0, size, 3);
        bh.consume(nulls);
    }

    @Benchmark
    public void applyDictionaryInts(Blackhole bh) {
        int[] output = new int[size];
        ops.applyDictionaryInts(output, dictInt, indices, size);
        bh.consume(output);
    }

    @Benchmark
    public void applyDictionaryLongs(Blackhole bh) {
        long[] output = new long[size];
        ops.applyDictionaryLongs(output, dictLong, indices, size);
        bh.consume(output);
    }

    @Benchmark
    public void unpackBitWidth1(Blackhole bh) {
        int[] output = new int[size];
        int count = Math.min(size, packedData.length * 8);
        ops.unpackBitWidth1(packedData, 0, output, 0, count);
        bh.consume(output);
    }
}
