/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import dev.hardwood.internal.predicate.BinaryComparator;
import dev.hardwood.internal.predicate.dictionary.DictionaryFilterSupport;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.metadata.PhysicalType;

/// Compares kernels for deciding an `IN` leaf against a row group's dictionary (#1594): whether
/// every literal of the list is absent from the dictionary, the case that drops the row group.
///
/// The dictionary is unsorted and holds `entries` values; the predicate holds `literals` values,
/// none of them in the dictionary, so every kernel scans the whole dictionary. The contenders
/// are private prototypes in this class, except `PRODUCTION`, which is
/// [DictionaryFilterSupport#absentAll(Dictionary, long[])] as it ships. `SORTED_SEARCH` is the
/// code that method ran before #1594, so its score is the baseline and the other kernels are
/// checked against it in setup.
///
/// - `SORTED_SEARCH`: sort the literals once, then `Arrays.binarySearch` for every entry. The
///   branches of the search go a different way on every entry, so they mispredict.
/// - `PRODUCTION`: the shipped method, which picks one of the kernels below by literal count
///   and span.
/// - `PASS_PER_LITERAL`: one sequential scan of the dictionary per literal, the single-value
///   check repeated `literals` times. The issue's lower bound: `k` single-value checks.
/// - `BITS_PASS_PER_LITERAL`: the same passes over a `DOUBLE` dictionary comparing
///   `Double.doubleToLongBits` of each entry with the literal's, which is equal exactly when
///   `Double.compare` is zero, instead of calling `Double.compare` per entry. `DOUBLE` only.
/// - `INNER_LOOP`: one pass over the dictionary with an inner loop over the unsorted literals,
///   the shape `absentAllFloat16` has. Every branch falls through in the all-absent case.
/// - `MASKED_SEARCH`: a binary search over the sorted literals, padded to a power of two, that
///   steers with an arithmetic mask instead of a branch, so its cost does not depend on C2
///   turning a branch into a conditional move.
/// - `HASH_SET`: an open-addressing table over the literals at a load factor of at most 1/4,
///   probed once per entry. The empty slot marker is a value chosen not to be a literal. The
///   binary table hashes an entry a word at a time and only when its length is a literal's.
/// - `RANGE_BITMASK`: one bit per value in `[min, max]` of the literals, after a range check,
///   when that span is below 2^24 bits; otherwise it falls back to `HASH_SET`. Only integer
///   types have a span; the `double` kernels work on the canonical `doubleToLongBits`, which
///   `Double.compare` orders, so the kernel is not applicable to `DOUBLE` and `BINARY16` and
///   its setup throws for those types.
///
/// `distribution` shapes the values: `SPARSE` draws them over the type's whole range; the
/// `DENSE_<n>X` distributions draw dictionary and literals from the same span of `n * entries`
/// consecutive integers, the shape of an id column, so a range check cannot reject most entries
/// and the bitmask has to be probed. At 131072 entries the bitmask is 32 KiB, 128 KiB and 2 MiB.
///
/// `absentAll` measures a call as the evaluator makes it per row group today: build the
/// structure over the literals, then scan. `scanOnly` measures the scan with the structure
/// built in setup, which is what caching the structure in the resolved predicate would save.
/// `SORTED_SEARCH` and `PRODUCTION` build inside the call, so both of their scores include it;
/// `PASS_PER_LITERAL` and `INNER_LOOP` build nothing, so their two scores agree.
///
/// Run:
/// ```shell
/// ./mvnw -pl core install -DskipTests
/// ./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar DictionaryInListBenchmark
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgsAppend = { "-Xms1g", "-Xmx1g" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class DictionaryInListBenchmark {

    public enum Type {
        LONG,
        INT,
        DOUBLE,
        BINARY16
    }

    public enum Kernel {
        SORTED_SEARCH,
        PRODUCTION,
        PASS_PER_LITERAL,
        BITS_PASS_PER_LITERAL,
        INNER_LOOP,
        MASKED_SEARCH,
        HASH_SET,
        RANGE_BITMASK
    }

    /// Where the values come from: the type's whole range, or a span of consecutive integers
    /// `multiplier` times the entry count wide, the shape of an id column.
    public enum Distribution {
        SPARSE(0),
        DENSE_2X(2),
        DENSE_8X(8),
        DENSE_128X(128);

        final int multiplier;

        Distribution(int multiplier) {
            this.multiplier = multiplier;
        }
    }

    /// Largest literal span the bitmask kernel builds a bitmask for, in bits: 2 MiB of bits.
    private static final long BITMASK_SPAN_LIMIT = 1L << 24;

    private static final int BINARY_WIDTH = 16;

    private static final VarHandle LONG_LE =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    @Param
    private Type type;

    @Param
    private Kernel kernel;

    @Param
    private Distribution distribution;

    /// Dictionary entries; 131072 `long`s fill parquet-java's and PyArrow's default 1 MiB
    /// dictionary page.
    @Param({ "131072" })
    private int entries;

    /// Literals in the `IN` list.
    @Param({ "1", "8", "64", "512", "4096" })
    private int literals;

    private Probe probe;
    private Probe builtProbe;

    @Setup(Level.Trial)
    public void setUp() {
        Random random = new Random(1594L);
        long[] dictionaryValues = distinctValues(random, entries, null);
        long[] literalValues = distinctValues(random, literals, dictionaryValues);
        verifyAgainstBaseline(random, dictionaryValues, literalValues);
        probe = probe(dictionaryValues, literalValues);
        builtProbe = probe(dictionaryValues, literalValues);
        builtProbe.build();
    }

    @Benchmark
    public boolean absentAll() {
        probe.build();
        return probe.scan();
    }

    @Benchmark
    public boolean scanOnly() {
        return builtProbe.scan();
    }

    /// `count` distinct values of the parameterised type and distribution, as `long`s, none of
    /// them in `excluded`.
    private long[] distinctValues(Random random, int count, long[] excluded) {
        Set<Long> taken = new HashSet<>();
        if (excluded != null) {
            for (long value : excluded) {
                taken.add(value);
            }
        }
        long[] values = new long[count];
        for (int i = 0; i < count; i++) {
            long value;
            do {
                value = nextValue(random);
            } while (!taken.add(value));
            values[i] = value;
        }
        return values;
    }

    private long nextValue(Random random) {
        if (distribution != Distribution.SPARSE) {
            return random.nextInt(Math.multiplyExact(distribution.multiplier, entries));
        }
        return switch (type) {
            case LONG, BINARY16 -> random.nextLong();
            case INT -> random.nextInt();
            // Canonical bits, as doubleToLongBits returns them, so no two values compare equal.
            case DOUBLE -> Double.doubleToLongBits(Double.longBitsToDouble(random.nextLong()));
        };
    }

    /// Every kernel must agree with today's code: all literals absent, then one literal replaced
    /// by the dictionary's first, middle and last entry in turn.
    private void verifyAgainstBaseline(Random random, long[] dictionaryValues, long[] literalValues) {
        requireResult(dictionaryValues, literalValues, true);
        int[] positions = { 0, dictionaryValues.length / 2, dictionaryValues.length - 1 };
        for (int position : positions) {
            long[] withPresent = literalValues.clone();
            withPresent[random.nextInt(withPresent.length)] = dictionaryValues[position];
            requireResult(dictionaryValues, withPresent, false);
        }
    }

    private void requireResult(long[] dictionaryValues, long[] literalValues, boolean expected) {
        Probe baseline = new LongProbes(dictionaryValues, literalValues).probe(Kernel.SORTED_SEARCH);
        baseline.build();
        if (baseline.scan() != expected) {
            throw new IllegalStateException("The baseline disagrees with the fixture");
        }
        Probe candidate = probe(dictionaryValues, literalValues);
        candidate.build();
        if (candidate.scan() != expected) {
            throw new IllegalStateException("Kernel " + kernel + " disagrees with " + Kernel.SORTED_SEARCH);
        }
    }

    private Probe probe(long[] dictionaryValues, long[] literalValues) {
        return switch (type) {
            case LONG -> new LongProbes(dictionaryValues, literalValues).probe(kernel);
            case INT -> new IntProbes(dictionaryValues, literalValues).probe(kernel);
            case DOUBLE -> new DoubleProbes(dictionaryValues, literalValues).probe(kernel);
            case BINARY16 -> new BinaryProbes(dictionaryValues, literalValues).probe(kernel);
        };
    }

    /// One kernel bound to one dictionary and literal list. `build` prepares the structure over
    /// the literals, `scan` decides absence; `build` is idempotent so `absentAll` can repeat it.
    private interface Probe {
        void build();

        boolean scan();
    }

    private static UnsupportedOperationException notApplicable(Kernel kernel, Type type) {
        return new UnsupportedOperationException(kernel + " is not applicable to " + type);
    }

    /// Stafford's variant 13 of the SplitMix64 finalizer: a bijective mix whose high bits
    /// depend on every input bit.
    private static long mix(long value) {
        long z = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    /// Table length for `count` keys at a load factor of at most 1/4, never below 16.
    private static int tableLength(int count) {
        return Math.max(16, Integer.highestOneBit(count * 4 - 1) << 1);
    }

    /// A `long` that is not among `values`: `0` when it is free, else the first free value of a
    /// deterministic sequence. Each candidate costs one scan of `values`; a hit is improbable.
    private static long freeValue(long[] values) {
        long candidate = 0;
        while (contains(values, candidate)) {
            candidate = mix(candidate) + 1;
        }
        return candidate;
    }

    private static boolean contains(long[] values, long value) {
        for (long v : values) {
            if (v == value) {
                return true;
            }
        }
        return false;
    }

    /// The kernel [DictionaryFilterSupport] ran before #1594: a sorted copy of the literals,
    /// searched per entry.
    private static boolean sortedSearch(long[] dict, long[] literals) {
        long[] probes = literals.clone();
        Arrays.sort(probes);
        for (long entry : dict) {
            if (Arrays.binarySearch(probes, entry) >= 0) {
                return false;
            }
        }
        return true;
    }

    private static boolean sortedSearch(int[] dict, int[] literals) {
        int[] probes = literals.clone();
        Arrays.sort(probes);
        for (int entry : dict) {
            if (Arrays.binarySearch(probes, entry) >= 0) {
                return false;
            }
        }
        return true;
    }

    /// `Arrays.sort(double[])` and `Arrays.binarySearch(double[], double)` order by
    /// `Double.compare`, so this agrees with the bit-equality the other kernels apply.
    private static boolean sortedSearch(double[] dict, double[] literals) {
        double[] probes = literals.clone();
        Arrays.sort(probes);
        for (double entry : dict) {
            if (Arrays.binarySearch(probes, entry) >= 0) {
                return false;
            }
        }
        return true;
    }

    /// Whether `sortedProbes`, in unsigned lexicographic order, holds the bytes `[from, to)`.
    private static boolean containsSorted(byte[][] sortedProbes, byte[] bytes, int from, int to) {
        int low = 0;
        int high = sortedProbes.length - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = BinaryComparator.compareUnsigned(bytes, from, to, sortedProbes[mid]);
            if (cmp == 0) {
                return true;
            }
            if (cmp > 0) {
                low = mid + 1;
            }
            else {
                high = mid - 1;
            }
        }
        return false;
    }

    /// `-1` when `a < b` as signed `long`s, else `0`, without a branch (Hacker's Delight 2-12).
    private static long lessThanMask(long a, long b) {
        long d = a - b;
        return (d ^ ((a ^ b) & (d ^ a))) >> 63;
    }

    /// `-1` when `a < b` as signed `int`s, else `0`.
    private static int lessThanMask(int a, int b) {
        int d = a - b;
        return (d ^ ((a ^ b) & (d ^ a))) >> 31;
    }

    /// The literals sorted and padded to a power of two with copies of the largest, so a
    /// branch-free search over them runs a fixed number of halving steps.
    private static long[] paddedSorted(long[] values) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        int length = Math.max(1, Integer.highestOneBit(sorted.length * 2 - 1));
        long[] padded = Arrays.copyOf(sorted, length);
        Arrays.fill(padded, sorted.length, length, sorted[sorted.length - 1]);
        return padded;
    }

    private static int[] paddedSorted(int[] values) {
        int[] sorted = values.clone();
        Arrays.sort(sorted);
        int length = Math.max(1, Integer.highestOneBit(sorted.length * 2 - 1));
        int[] padded = Arrays.copyOf(sorted, length);
        Arrays.fill(padded, sorted.length, length, sorted[sorted.length - 1]);
        return padded;
    }

    /// Whether `padded`, sorted with a power-of-two length, holds `key`: a search whose steps
    /// move the base by `half & mask` instead of branching.
    private static boolean containsMasked(long[] padded, long key) {
        int base = 0;
        for (int half = padded.length >>> 1; half > 0; half >>>= 1) {
            // Step forward unless key < padded[base + half].
            base += half & (int) ~lessThanMask(key, padded[base + half]);
        }
        return padded[base] == key;
    }

    private static boolean containsMasked(int[] padded, int key) {
        int base = 0;
        for (int half = padded.length >>> 1; half > 0; half >>>= 1) {
            base += half & ~lessThanMask(key, padded[base + half]);
        }
        return padded[base] == key;
    }

    /// Open-addressing table of `long` keys with linear probing. Slots hold `empty`, which is
    /// not a key, so a probe for `empty` itself finds it and correctly reports no key.
    private static final class LongTable {
        private final long[] slots;
        private final int shift;
        private final long empty;

        LongTable(long[] keys) {
            int length = tableLength(keys.length);
            slots = new long[length];
            shift = Long.SIZE - Integer.numberOfTrailingZeros(length);
            empty = freeValue(keys);
            Arrays.fill(slots, empty);
            int mask = length - 1;
            for (long key : keys) {
                int i = (int) (mix(key) >>> shift);
                while (slots[i] != empty) {
                    i = (i + 1) & mask;
                }
                slots[i] = key;
            }
        }

        boolean contains(long key) {
            int mask = slots.length - 1;
            int i = (int) (mix(key) >>> shift);
            while (true) {
                long slot = slots[i];
                // Empty first: a probe for the marker itself must report no key.
                if (slot == empty) {
                    return false;
                }
                if (slot == key) {
                    return true;
                }
                i = (i + 1) & mask;
            }
        }
    }

    /// One bit per value of `[min, max]` over the literals. The caller checks [#spans] first and
    /// uses another kernel when the span is too wide.
    private static final class RangeBitmask {
        private final long min;
        private final long span;
        private final long[] bits;

        static boolean spans(long[] keys) {
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            for (long key : keys) {
                min = Math.min(min, key);
                max = Math.max(max, key);
            }
            long span = max - min;
            // A negative span overflowed; the literals straddle more than half the range.
            return span >= 0 && span < BITMASK_SPAN_LIMIT;
        }

        RangeBitmask(long[] keys) {
            long lowest = Long.MAX_VALUE;
            long highest = Long.MIN_VALUE;
            for (long key : keys) {
                lowest = Math.min(lowest, key);
                highest = Math.max(highest, key);
            }
            min = lowest;
            span = highest - lowest;
            bits = new long[(int) (span >>> 6) + 1];
            for (long key : keys) {
                long offset = key - min;
                bits[(int) (offset >>> 6)] |= 1L << offset;
            }
        }

        boolean contains(long key) {
            long offset = key - min;
            if (Long.compareUnsigned(offset, span) > 0) {
                return false;
            }
            return (bits[(int) (offset >>> 6)] & (1L << offset)) != 0;
        }
    }

    /// Kernels over a [Dictionary.LongDictionary].
    private static final class LongProbes {
        private final Dictionary dictionary;
        private final long[] dict;
        private final long[] literals;

        LongProbes(long[] dictionaryValues, long[] literalValues) {
            dict = dictionaryValues;
            dictionary = new Dictionary.LongDictionary(dict);
            literals = literalValues;
        }

        Probe probe(Kernel kernel) {
            return switch (kernel) {
                case SORTED_SEARCH -> new Probe() {
                    @Override
                    public void build() {
                    }

                    @Override
                    public boolean scan() {
                        return sortedSearch(dict, literals);
                    }
                };
                case PRODUCTION -> new Probe() {
                    @Override
                    public void build() {
                    }

                    @Override
                    public boolean scan() {
                        return DictionaryFilterSupport.absentAll(dictionary, literals);
                    }
                };
                case PASS_PER_LITERAL -> new Probe() {
                    @Override
                    public void build() {
                    }

                    @Override
                    public boolean scan() {
                        for (long literal : literals) {
                            if (!DictionaryFilterSupport.valueAbsent(dictionary, literal)) {
                                return false;
                            }
                        }
                        return true;
                    }
                };
                case INNER_LOOP -> new Probe() {
                    @Override
                    public void build() {
                    }

                    @Override
                    public boolean scan() {
                        for (long entry : dict) {
                            for (long literal : literals) {
                                if (entry == literal) {
                                    return false;
                                }
                            }
                        }
                        return true;
                    }
                };
                case MASKED_SEARCH -> new Probe() {
                    private long[] padded;

                    @Override
                    public void build() {
                        padded = paddedSorted(literals);
                    }

                    @Override
                    public boolean scan() {
                        for (long entry : dict) {
                            if (containsMasked(padded, entry)) {
                                return false;
                            }
                        }
                        return true;
                    }
                };
                case HASH_SET -> new Probe() {
                    private LongTable table;

                    @Override
                    public void build() {
                        table = new LongTable(literals);
                    }

                    @Override
                    public boolean scan() {
                        for (long entry : dict) {
                            if (table.contains(entry)) {
                                return false;
                            }
                        }
                        return true;
                    }
                };
                case RANGE_BITMASK -> RangeBitmask.spans(literals) ? new Probe() {
                    private RangeBitmask bitmask;

                    @Override
                    public void build() {
                        bitmask = new RangeBitmask(literals);
                    }

                    @Override
                    public boolean scan() {
                        for (long entry : dict) {
                            if (bitmask.contains(entry)) {
                                return false;
                            }
                        }
                        return true;
                    }
                } : probe(Kernel.HASH_SET);
                case BITS_PASS_PER_LITERAL -> throw notApplicable(kernel, Type.LONG);
            };
        }
    }

    /// Kernels over a [Dictionary.IntDictionary]; the fixture's values are narrowed, which the
    /// `INT` distribution keeps within range.
    private static final class IntProbes {
        private final Dictionary dictionary;
        private final int[] dict;
        private final int[] literals;

        IntProbes(long[] dictionaryValues, long[] literalValues) {
            dict = narrow(dictionaryValues);
            dictionary = new Dictionary.IntDictionary(dict);
            literals = narrow(literalValues);
        }

        private static int[] narrow(long[] values) {
            int[] narrowed = new int[values.length];
            for (int i = 0; i < values.length; i++) {
                narrowed[i] = Math.toIntExact(values[i]);
            }
            return narrowed;
        }

        private long[] widenedLiterals() {
            long[] widened = new long[literals.length];
            for (int i = 0; i < literals.length; i++) {
                widened[i] = literals[i];
            }
            return widened;
        }

        Probe probe(Kernel kernel) {
            return switch (kernel) {
                case SORTED_SEARCH -> new Probe() {
                    @Override
                    public void build() {
                    }

                    @Override
                    public boolean scan() {
                        return sortedSearch(dict, literals);
                    }
                };
                case PRODUCTION -> new Probe() {
                    @Override
                    public void build() {
                    }

                    @Override
                    public boolean scan() {
                        return DictionaryFilterSupport.absentAll(dictionary, literals);
                    }
                };
                case PASS_PER_LITERAL -> new Probe() {
                    @Override
                    public void build() {
                    }

                    @Override
                    public boolean scan() {
                        for (int literal : literals) {
                            if (!DictionaryFilterSupport.valueAbsent(dictionary, literal)) {
                                return false;
                            }
                        }
                        return true;
                    }
                };
                case INNER_LOOP -> new Probe() {
                    @Override
                    public void build() {
                    }

                    @Override
                    public boolean scan() {
                        for (int entry : dict) {
                            for (int literal : literals) {
                                if (entry == literal) {
                                    return false;
                                }
                            }
                        }
                        return true;
                    }
                };
                case MASKED_SEARCH -> new Probe() {
                    private int[] padded;

                    @Override
                    public void build() {
                        padded = paddedSorted(literals);
                    }

                    @Override
                    public boolean scan() {
                        for (int entry : dict) {
                            if (containsMasked(padded, entry)) {
                                return false;
                            }
                        }
                        return true;
                    }
                };
                case HASH_SET -> new Probe() {
                    private LongTable table;

                    @Override
                    public void build() {
                        table = new LongTable(widenedLiterals());
                    }

                    @Override
                    public boolean scan() {
                        for (int entry : dict) {
                            if (table.contains(entry)) {
                                return false;
                            }
                        }
                        return true;
                    }
                };
                case RANGE_BITMASK -> RangeBitmask.spans(widenedLiterals()) ? new Probe() {
                    private RangeBitmask bitmask;

                    @Override
                    public void build() {
                        bitmask = new RangeBitmask(widenedLiterals());
                    }

                    @Override
                    public boolean scan() {
                        for (int entry : dict) {
                            if (bitmask.contains(entry)) {
                                return false;
                            }
                        }
                        return true;
                    }
                } : probe(Kernel.HASH_SET);
                case BITS_PASS_PER_LITERAL -> throw notApplicable(kernel, Type.INT);
            };
        }
    }

    /// Kernels over a [Dictionary.DoubleDictionary]. The fixture's `long`s are the canonical
    /// bits of the values, and the indexed kernels key on `Double.doubleToLongBits`, which is
    /// equal exactly when `Double.compare` is zero.
    private static final class DoubleProbes {
        private final Dictionary dictionary;
        private final double[] dict;
        private final double[] literals;
        private final long[] literalBits;

        DoubleProbes(long[] dictionaryValues, long[] literalValues) {
            dict = widen(dictionaryValues);
            dictionary = new Dictionary.DoubleDictionary(dict);
            literals = widen(literalValues);
            literalBits = literalValues;
        }

        private static double[] widen(long[] bits) {
            double[] values = new double[bits.length];
            for (int i = 0; i < bits.length; i++) {
                values[i] = Double.longBitsToDouble(bits[i]);
            }
            return values;
        }

        Probe probe(Kernel kernel) {
            return switch (kernel) {
                case SORTED_SEARCH -> new Probe() {
                    @Override
                    public void build() {
                    }

                    @Override
                    public boolean scan() {
                        return sortedSearch(dict, literals);
                    }
                };
                case PRODUCTION -> new Probe() {
                    @Override
                    public void build() {
                    }

                    @Override
                    public boolean scan() {
                        return DictionaryFilterSupport.absentAll(dictionary, literals);
                    }
                };
                case PASS_PER_LITERAL -> new Probe() {
                    @Override
                    public void build() {
                    }

                    @Override
                    public boolean scan() {
                        for (double literal : literals) {
                            if (!DictionaryFilterSupport.valueAbsent(dictionary, literal)) {
                                return false;
                            }
                        }
                        return true;
                    }
                };
                case BITS_PASS_PER_LITERAL -> new Probe() {
                    @Override
                    public void build() {
                    }

                    @Override
                    public boolean scan() {
                        for (long bits : literalBits) {
                            for (double entry : dict) {
                                if (Double.doubleToLongBits(entry) == bits) {
                                    return false;
                                }
                            }
                        }
                        return true;
                    }
                };
                case INNER_LOOP -> new Probe() {
                    @Override
                    public void build() {
                    }

                    @Override
                    public boolean scan() {
                        for (double entry : dict) {
                            for (double literal : literals) {
                                if (Double.compare(entry, literal) == 0) {
                                    return false;
                                }
                            }
                        }
                        return true;
                    }
                };
                case MASKED_SEARCH -> new Probe() {
                    private long[] padded;

                    @Override
                    public void build() {
                        padded = paddedSorted(literalBits);
                    }

                    @Override
                    public boolean scan() {
                        for (double entry : dict) {
                            if (containsMasked(padded, Double.doubleToLongBits(entry))) {
                                return false;
                            }
                        }
                        return true;
                    }
                };
                case HASH_SET -> new Probe() {
                    private LongTable table;

                    @Override
                    public void build() {
                        table = new LongTable(literalBits);
                    }

                    @Override
                    public boolean scan() {
                        for (double entry : dict) {
                            if (table.contains(Double.doubleToLongBits(entry))) {
                                return false;
                            }
                        }
                        return true;
                    }
                };
                case RANGE_BITMASK -> throw notApplicable(kernel, Type.DOUBLE);
            };
        }
    }

    /// Kernels over a [Dictionary.ByteArrayDictionary] of 16-byte entries, each the big-endian
    /// bytes of a fixture `long` followed by eight zero bytes.
    private static final class BinaryProbes {
        private final Dictionary dictionary;
        private final byte[] bytes;
        private final int[] offsets;
        private final int size;
        private final byte[][] literals;

        BinaryProbes(long[] dictionaryValues, long[] literalValues) {
            byte[] encoded = new byte[dictionaryValues.length * BINARY_WIDTH];
            for (int i = 0; i < dictionaryValues.length; i++) {
                putLong(encoded, i * BINARY_WIDTH, dictionaryValues[i]);
            }
            Dictionary.ByteArrayDictionary dict = (Dictionary.ByteArrayDictionary) Dictionary.parse(
                    encoded, encoded.length, dictionaryValues.length, PhysicalType.FIXED_LEN_BYTE_ARRAY, BINARY_WIDTH);
            dictionary = dict;
            bytes = dict.entryBytes();
            offsets = dict.entryOffsets();
            size = dict.size();
            literals = new byte[literalValues.length][];
            for (int i = 0; i < literalValues.length; i++) {
                literals[i] = new byte[BINARY_WIDTH];
                putLong(literals[i], 0, literalValues[i]);
            }
        }

        private static void putLong(byte[] target, int offset, long value) {
            for (int i = 0; i < Long.BYTES; i++) {
                target[offset + i] = (byte) (value >>> (Long.SIZE - Byte.SIZE * (i + 1)));
            }
        }

        /// Hash of the bytes `[from, to)`: a word at a time, the tail byte by byte, then mixed.
        private static long hash(byte[] bytes, int from, int to) {
            long h = to - from;
            int i = from;
            for (; i + Long.BYTES <= to; i += Long.BYTES) {
                h = (h ^ (long) LONG_LE.get(bytes, i)) * 0x9e3779b97f4a7c15L;
            }
            for (; i < to; i++) {
                h = (h ^ bytes[i]) * 0x9e3779b97f4a7c15L;
            }
            return mix(h);
        }

        /// Bit `n` set when some literal is `n` bytes long, for `n < 64`; bit 63 stands for every
        /// longer length. An entry whose length bit is clear matches no literal.
        private static long lengthMask(byte[][] literals) {
            long mask = 0;
            for (byte[] literal : literals) {
                mask |= 1L << Math.min(literal.length, Long.SIZE - 1);
            }
            return mask;
        }

        private static boolean lengthPossible(long lengthMask, int length) {
            return (lengthMask & (1L << Math.min(length, Long.SIZE - 1))) != 0;
        }

        Probe probe(Kernel kernel) {
            return switch (kernel) {
                case SORTED_SEARCH -> new Probe() {
                    @Override
                    public void build() {
                    }

                    @Override
                    public boolean scan() {
                        // Unsigned lexicographic order compares equal exactly when the arrays are equal.
                        byte[][] probes = literals.clone();
                        Arrays.sort(probes, BinaryComparator::compareUnsigned);
                        for (int i = 0; i < size; i++) {
                            if (containsSorted(probes, bytes, offsets[i], offsets[i + 1])) {
                                return false;
                            }
                        }
                        return true;
                    }
                };
                case PRODUCTION -> new Probe() {
                    @Override
                    public void build() {
                    }

                    @Override
                    public boolean scan() {
                        return DictionaryFilterSupport.absentAll(dictionary, literals);
                    }
                };
                case PASS_PER_LITERAL -> new Probe() {
                    @Override
                    public void build() {
                    }

                    @Override
                    public boolean scan() {
                        for (byte[] literal : literals) {
                            if (!DictionaryFilterSupport.valueAbsent(dictionary, literal)) {
                                return false;
                            }
                        }
                        return true;
                    }
                };
                case INNER_LOOP -> new Probe() {
                    @Override
                    public void build() {
                    }

                    @Override
                    public boolean scan() {
                        for (int i = 0; i < size; i++) {
                            int from = offsets[i];
                            int to = offsets[i + 1];
                            for (byte[] literal : literals) {
                                if (BinaryComparator.sliceEquals(bytes, from, to, literal)) {
                                    return false;
                                }
                            }
                        }
                        return true;
                    }
                };
                case HASH_SET -> new Probe() {
                    /// Slots hold a literal index, `-1` when empty.
                    private int[] slots;
                    private int shift;
                    private long lengths;

                    @Override
                    public void build() {
                        int length = tableLength(literals.length);
                        slots = new int[length];
                        Arrays.fill(slots, -1);
                        shift = Long.SIZE - Integer.numberOfTrailingZeros(length);
                        lengths = lengthMask(literals);
                        int mask = length - 1;
                        for (int index = 0; index < literals.length; index++) {
                            byte[] literal = literals[index];
                            int i = (int) (hash(literal, 0, literal.length) >>> shift);
                            while (slots[i] != -1) {
                                i = (i + 1) & mask;
                            }
                            slots[i] = index;
                        }
                    }

                    @Override
                    public boolean scan() {
                        int mask = slots.length - 1;
                        for (int e = 0; e < size; e++) {
                            int from = offsets[e];
                            int to = offsets[e + 1];
                            if (!lengthPossible(lengths, to - from)) {
                                continue;
                            }
                            int i = (int) (hash(bytes, from, to) >>> shift);
                            int index;
                            while ((index = slots[i]) != -1) {
                                if (BinaryComparator.sliceEquals(bytes, from, to, literals[index])) {
                                    return false;
                                }
                                i = (i + 1) & mask;
                            }
                        }
                        return true;
                    }
                };
                case BITS_PASS_PER_LITERAL, MASKED_SEARCH, RANGE_BITMASK -> throw notApplicable(kernel, Type.BINARY16);
            };
        }
    }
}
