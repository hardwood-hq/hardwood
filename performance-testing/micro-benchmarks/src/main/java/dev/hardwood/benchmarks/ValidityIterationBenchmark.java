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

import dev.hardwood.reader.Validity;

/// Compares two ways of summing a `long[]` column while honoring a
/// per-item null bitmap:
///
/// - `wordIterate` — pull the backing `long[]` once and iterate only the
///   set bits with `Long.numberOfTrailingZeros` + `present &= present - 1`.
/// - `perItemCheck` — straight `for` loop with `validity.isNotNull(i)` on
///   every position, gated by a `hasNulls()` fast-path.
///
/// Run with:
/// ```shell
/// ./mvnw -pl performance-testing/micro-benchmarks -am -Pperformance-test -DskipTests package
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar ValidityIterationBenchmark
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = { "-Xms512m", "-Xmx512m" })
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
public class ValidityIterationBenchmark {

    @Param({ "1024", "65536" })
    private int count;

    @Param({ "0.0", "0.05", "0.95" })
    private double nullDensity;

    private long[] values;
    private Validity validity;

    @Setup
    public void setup() {
        Random random = new Random(42);
        values = new long[count];
        for (int i = 0; i < count; i++) values[i] = random.nextLong();

        if (nullDensity == 0.0) {
            validity = Validity.NO_NULLS;
        } else {
            int wordCount = (count + 63) >>> 6;
            long[] words = new long[wordCount];
            for (int i = 0; i < count; i++) {
                if (random.nextDouble() >= nullDensity) words[i >>> 6] |= 1L << i;
            }
            validity = Validity.of(words);
        }
    }

    @Benchmark
    public long wordIterate() {
        long sum = 0L;
        long[] values = this.values;
        int count = this.count;
        long[] words = validity.words();
        if (words == null) {
            for (int i = 0; i < count; i++) sum += values[i];
        } else {
            int wordCount = (count + 63) >>> 6;
            for (int w = 0; w < wordCount; w++) {
                long present = words[w];
                while (present != 0L) {
                    int bit = Long.numberOfTrailingZeros(present);
                    sum += values[(w << 6) + bit];
                    present &= present - 1L;
                }
            }
        }
        return sum;
    }

    @Benchmark
    public long perItemCheck() {
        long sum = 0L;
        long[] values = this.values;
        int count = this.count;
        Validity validity = this.validity;
        boolean hasNulls = validity.hasNulls();
        for (int i = 0; i < count; i++) {
            if (!hasNulls || validity.isNotNull(i)) {
                sum += values[i];
            }
        }
        return sum;
    }
}
