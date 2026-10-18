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

import dev.hardwood.internal.encoding.simd.ScalarOperations;
import dev.hardwood.internal.encoding.simd.SimdOperations;
import dev.hardwood.internal.encoding.simd.VectorSupport;

/// Benchmark for SIMD operations comparing scalar vs vectorized implementations.
///
/// Run with:
/// ```shell
/// java --add-modules jdk.incubator.vector -jar benchmarks.jar SimdBenchmark
/// ```
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgsAppend = { "-Xms512m", "-Xmx512m", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class SimdBenchmark {

    @Param({"128", "1024", "8192", "65536"})
    private int size;

    @Param({"scalar", "auto"})
    private String implementation;

    private int[] defLevels;
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
    }

    @Benchmark
    public int countNonNulls() {
        return ops.countNonNulls(defLevels, 0, defLevels.length, 3);
    }
}
