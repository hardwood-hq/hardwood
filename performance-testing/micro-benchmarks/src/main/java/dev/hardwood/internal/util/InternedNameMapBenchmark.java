/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.util;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import dev.hardwood.benchmarks.FieldNames;

/// The cost of an [InternedNameMap] lookup and of building the map, as a function of the number
/// of names, over [#SETS] different sets of random field names per count: how the table is
/// sized decides how many names share a slot, and that differs from one set to the next.
///
/// - `lookupInterned`: the names of one map, by the interned instance a string literal is.
///   One invocation makes [#LOOKUPS] lookups, passing over the map's names as often as that
///   takes, and the next invocation takes the next map, so the lookups run on a map that is in
///   cache, as a row reader's are row after row.
/// - `lookupOtherInstance`: the same, by an equal instance that is not the interned one, as a
///   name built at runtime is.
/// - `build`: one map built from one set of names, cycling through the sets.
///
/// Each lookup result is in ns per lookup, `build` in µs per map. The trial setup prints the
/// range of table sizes the map may take for each count.
///
/// ```shell
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar InternedNameMapBenchmark
/// ```
@BenchmarkMode(Mode.AverageTime)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgsAppend = { "-Xms1g", "-Xmx1g" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class InternedNameMapBenchmark {

    private static final int SETS = 16;
    private static final int LOOKUPS = 8192;

    @Param({ "20", "64", "105", "128", "129", "300", "1000" })
    private int count;

    private String[][] interned;
    private String[][] others;
    private InternedNameMap[] maps;
    private int next;

    @Setup
    public void setup() {
        interned = new String[SETS][];
        others = new String[SETS][];
        maps = new InternedNameMap[SETS];
        for (int s = 0; s < SETS; s++) {
            interned[s] = FieldNames.random(count, 1_000L * count + s);
            others[s] = new String[count];
            for (int i = 0; i < count; i++) {
                others[s][i] = new String(interned[s][i].toCharArray());
            }
            maps[s] = new InternedNameMap(interned[s]);
            for (int i = 0; i < count; i++) {
                if (maps[s].get(interned[s][i]) != i || maps[s].get(others[s][i]) != i) {
                    throw new IllegalStateException("Name " + i + " of set " + s + " not found");
                }
            }
        }
        int min = InternedNameMap.minSlots(count);
        int max = InternedNameMap.maxSlots(count);
        System.out.printf("%n[table] %d names: %d to %d slots (%.1f to %.1f KB)%n",
                count, min, max, min * 8.0 / 1024, max * 8.0 / 1024);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @OperationsPerInvocation(LOOKUPS)
    public long lookupInterned() {
        return lookups(interned);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @OperationsPerInvocation(LOOKUPS)
    public long lookupOtherInstance() {
        return lookups(others);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public InternedNameMap build() {
        int s = next;
        next = (s + 1) % SETS;
        return new InternedNameMap(interned[s]);
    }

    /// [#LOOKUPS] lookups in the next map, of its names in order, from the first again once
    /// they run out.
    private long lookups(String[][] names) {
        int s = next;
        next = (s + 1) % SETS;
        InternedNameMap map = maps[s];
        String[] set = names[s];
        long sum = 0;
        int remaining = LOOKUPS;
        while (remaining > 0) {
            int n = Math.min(remaining, set.length);
            for (int i = 0; i < n; i++) {
                sum += map.get(set[i]);
            }
            remaining -= n;
        }
        return sum;
    }
}
