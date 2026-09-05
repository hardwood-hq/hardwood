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

/// Compares the two [SimdOperations] bit-unpack implementations against each other.
///
/// `SimdOperations.unpackBitWidthN` is declared for bit widths 2 to 8 and has a scalar
/// implementation and a Vector-API-module implementation. Wiring the decoder to it is only
/// worth doing if the second is faster than the first, so this measures exactly that.
///
/// Must be run with the Vector API module present, or both benchmarks measure the same
/// implementation; setup fails loudly if the vector path did not load.
///
/// Run with:
/// ```shell
/// java -jar benchmarks.jar BitUnpackPrimitiveBenchmark
/// ```
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms512m", "-Xmx512m", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BitUnpackPrimitiveBenchmark {

    @Param({ "4096" })
    private int size;

    /// The full range `unpackBitWidthN` declares support for.
    @Param({ "2", "4", "8" })
    private int bitWidth;

    private byte[] packed;
    private int[] output;

    private SimdOperations scalarOps;
    private SimdOperations vectorOps;

    @Setup
    public void setUp() {
        scalarOps = new ScalarOperations();
        vectorOps = VectorSupport.operations();

        if (!VectorSupport.isAvailable()) {
            throw new IllegalStateException("Vector API not available (" + VectorSupport.implementationName()
                    + "); rerun with --add-modules jdk.incubator.vector or this benchmark compares "
                    + "the scalar implementation with itself");
        }

        output = new int[size];
        packed = new byte[size * bitWidth / 8 + 16];
        new Random(42).nextBytes(packed);

        // Both implementations must agree before either is timed.
        int[] fromScalar = new int[size];
        int[] fromVector = new int[size];
        scalarOps.unpackBitWidthN(packed, 0, fromScalar, 0, size, bitWidth);
        vectorOps.unpackBitWidthN(packed, 0, fromVector, 0, size, bitWidth);
        for (int i = 0; i < size; i++) {
            if (fromScalar[i] != fromVector[i]) {
                throw new IllegalStateException("Implementations disagree at " + i + " for bit width "
                        + bitWidth + ": scalar " + fromScalar[i] + ", vector " + fromVector[i]);
            }
        }
    }

    @Benchmark
    public int[] scalarUnpack() {
        scalarOps.unpackBitWidthN(packed, 0, output, 0, size, bitWidth);
        return output;
    }

    @Benchmark
    public int[] vectorUnpack() {
        vectorOps.unpackBitWidthN(packed, 0, output, 0, size, bitWidth);
        return output;
    }
}
