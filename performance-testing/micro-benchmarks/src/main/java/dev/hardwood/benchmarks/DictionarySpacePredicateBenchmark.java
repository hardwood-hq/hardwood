/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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

import dev.hardwood.internal.predicate.BinaryBatchMatcher;
import dev.hardwood.internal.predicate.DictionaryBinaryBatchMatcher;
import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate.Comparison;
import dev.hardwood.internal.predicate.matcher.binaries.BinaryEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.binaries.BinaryInBatchMatcher;
import dev.hardwood.internal.predicate.matcher.binaries.BinaryLtBatchMatcher;
import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.internal.reader.BinaryBatchValues;
import dev.hardwood.internal.reader.Dictionary;
import dev.hardwood.metadata.PhysicalType;

/// Compares eager and lazy dictionary-space predicates with the ordinary
/// packed-value loop.
///
/// The cold arms install fresh eager and lazy wrappers for every invocation, so
/// dictionary preparation and entry decisions occur inside the measured
/// operation. The hot arms reuse wrappers whose outcomes were populated by a
/// previous batch. `SPARSE` references four entries from a potentially large
/// dictionary; `FULL` references every entry.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms1g", "-Xmx1g"})
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class DictionarySpacePredicateBenchmark {

    public enum Access {
        SPARSE,
        FULL
    }

    public enum PredicateKind {
        EQ,
        LT,
        IN
    }

    public enum ValueWidth {
        SHORT,
        LONG
    }

    @Param({"16", "100", "4096"})
    private int cardinality;

    @Param
    private Access access;

    @Param
    private PredicateKind predicateKind;

    @Param
    private ValueWidth valueWidth;

    @Param({"65536"})
    private int rows;

    private BatchExchange.Batch batch;
    private BinaryBatchMatcher packedMatcher;
    private BinaryBatchMatcher coldLazyMatcher;
    private BinaryBatchMatcher hotLazyMatcher;
    private BinaryBatchMatcher coldEagerMatcher;
    private BinaryBatchMatcher hotEagerMatcher;
    private BinaryBatchMatcher coldInlineLazyMatcher;
    private BinaryBatchMatcher hotInlineLazyMatcher;
    private long[] outWords;

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        byte[][] entries = new byte[cardinality][];
        int encodedSize = 0;
        for (int i = 0; i < cardinality; i++) {
            String value = switch (valueWidth) {
                case SHORT -> String.format("c%05d", i);
                case LONG -> String.format("https://example.com/items/category-%05d", i);
            };
            entries[i] = value.getBytes(StandardCharsets.UTF_8);
            encodedSize += Integer.BYTES + entries[i].length;
        }

        ByteBuffer encoded = ByteBuffer.allocate(encodedSize).order(ByteOrder.LITTLE_ENDIAN);
        for (byte[] entry : entries) {
            encoded.putInt(entry.length);
            encoded.put(entry);
        }
        Dictionary.ByteArrayDictionary dictionary =
                (Dictionary.ByteArrayDictionary) Dictionary.parse(
                        encoded.array(), entries.length, PhysicalType.BYTE_ARRAY, null);

        int referencedEntries = access == Access.SPARSE ? Math.min(4, cardinality) : cardinality;
        int packedSize = 0;
        for (int row = 0; row < rows; row++) {
            packedSize += entries[row % referencedEntries].length;
        }
        byte[] bytes = new byte[packedSize];
        int[] offsets = new int[rows + 1];
        int[] dictionaryIndices = new int[rows];
        int position = 0;
        for (int row = 0; row < rows; row++) {
            int dictionaryIndex = row % referencedEntries;
            byte[] entry = entries[dictionaryIndex];
            System.arraycopy(entry, 0, bytes, position, entry.length);
            position += entry.length;
            offsets[row + 1] = position;
            dictionaryIndices[row] = dictionaryIndex;
        }

        BinaryBatchValues values = new BinaryBatchValues(bytes, offsets);
        values.dictionary = dictionary;
        values.dictIndices = dictionaryIndices;
        batch = new BatchExchange.Batch();
        batch.values = values;
        batch.recordCount = rows;

        int middle = referencedEntries / 2;
        byte[] literal = entries[middle];
        packedMatcher = switch (predicateKind) {
            case EQ -> new BinaryEqBatchMatcher(literal, Comparison.BYTE_STRING);
            case LT -> new BinaryLtBatchMatcher(literal, Comparison.BYTE_STRING);
            case IN -> new BinaryInBatchMatcher(
                    new byte[][]{entries[0], entries[middle], entries[referencedEntries - 1]},
                    Comparison.BYTE_STRING);
        };
        outWords = new long[(rows + 63) >>> 6];
        verifyEquivalentResults();
        hotLazyMatcher = new DictionaryBinaryBatchMatcher(packedMatcher);
        hotLazyMatcher.test(batch, outWords);
        hotEagerMatcher = new EagerDictionaryMatcher(packedMatcher);
        hotEagerMatcher.test(batch, outWords);
        hotInlineLazyMatcher = new InlineLazyDictionaryMatcher(packedMatcher);
        hotInlineLazyMatcher.test(batch, outWords);
    }

    @Setup(Level.Invocation)
    public void setupInvocation() {
        coldLazyMatcher = new DictionaryBinaryBatchMatcher(packedMatcher);
        coldEagerMatcher = new EagerDictionaryMatcher(packedMatcher);
        coldInlineLazyMatcher = new InlineLazyDictionaryMatcher(packedMatcher);
    }

    @Benchmark
    public long lazyCold() {
        coldLazyMatcher.test(batch, outWords);
        return checksum();
    }

    @Benchmark
    public long lazyHot() {
        hotLazyMatcher.test(batch, outWords);
        return checksum();
    }

    @Benchmark
    public long eagerCold() {
        coldEagerMatcher.test(batch, outWords);
        return checksum();
    }

    @Benchmark
    public long eagerHot() {
        hotEagerMatcher.test(batch, outWords);
        return checksum();
    }

    @Benchmark
    public long inlineLazyCold() {
        coldInlineLazyMatcher.test(batch, outWords);
        return checksum();
    }

    @Benchmark
    public long inlineLazyHot() {
        hotInlineLazyMatcher.test(batch, outWords);
        return checksum();
    }

    @Benchmark
    public long packedValues() {
        packedMatcher.test(batch, outWords);
        return checksum();
    }

    private long checksum() {
        long checksum = 1L;
        for (long word : outWords) {
            checksum = 31L * checksum + word;
        }
        return checksum;
    }

    private void verifyEquivalentResults() {
        long[] expected = new long[outWords.length];
        packedMatcher.test(batch, expected);
        verifyEquivalentResults(
                "two-pass lazy", new DictionaryBinaryBatchMatcher(packedMatcher), expected);
        verifyEquivalentResults("eager", new EagerDictionaryMatcher(packedMatcher), expected);
        verifyEquivalentResults(
                "one-pass lazy", new InlineLazyDictionaryMatcher(packedMatcher), expected);
    }

    private void verifyEquivalentResults(
            String strategy, BinaryBatchMatcher matcher, long[] expected) {
        long[] actual = new long[expected.length];
        matcher.test(batch, actual);
        requireEquivalentResults(strategy + " cold", expected, actual);
        Arrays.fill(actual, 0L);
        matcher.test(batch, actual);
        requireEquivalentResults(strategy + " hot", expected, actual);
    }

    private void requireEquivalentResults(String contender, long[] expected, long[] actual) {
        if (!Arrays.equals(expected, actual)) {
            throw new IllegalStateException(
                    contender + " produced different results from packed matching");
        }
    }

    /// Benchmark-only eager strategy: decide every dictionary entry when a new
    /// dictionary object arrives, then gather one cached outcome per encoded
    /// row. Plain batches and `-1` rows preserve the production fallback.
    private static final class EagerDictionaryMatcher implements BinaryBatchMatcher {

        private final BinaryBatchMatcher delegate;
        private Dictionary.ByteArrayDictionary cachedDictionary;
        private boolean[] entryMatches;

        private EagerDictionaryMatcher(BinaryBatchMatcher delegate) {
            this.delegate = delegate;
        }

        @Override
        public void test(BatchExchange.Batch batch, long[] outWords) {
            BinaryBatchValues values = (BinaryBatchValues) batch.values;
            Dictionary.ByteArrayDictionary dictionary = values.dictionary;
            if (dictionary == null) {
                delegate.test(batch, outWords);
                return;
            }
            int[] dictionaryIndices = values.dictIndices;
            if (dictionaryIndices == null) {
                throw new IllegalStateException(
                        "A binary batch with a dictionary must retain its dictionary indices");
            }
            if (dictionary != cachedDictionary) {
                cachedDictionary = dictionary;
                byte[][] entries = dictionary.values();
                entryMatches = new boolean[entries.length];
                for (int i = 0; i < entries.length; i++) {
                    byte[] entry = entries[i];
                    entryMatches[i] = delegate.testValue(entry, 0, entry.length);
                }
            }
            writeMatches(values, dictionaryIndices, batch.validity, batch.recordCount, outWords);
        }

        @Override
        public boolean testValue(byte[] bytes, int from, int to) {
            return delegate.testValue(bytes, from, to);
        }

        private void writeMatches(BinaryBatchValues values, int[] dictionaryIndices,
                                  long[] validity, int recordCount, long[] outWords) {
            int activeWords = (recordCount + 63) >>> 6;
            for (int wordIndex = 0; wordIndex < activeWords; wordIndex++) {
                int base = wordIndex << 6;
                int rows = Math.min(64, recordCount - base);
                long present = validity != null ? validity[wordIndex] : -1L;
                long word = 0L;
                for (int bit = 0; bit < rows; bit++) {
                    if ((present & (1L << bit)) == 0L) {
                        continue;
                    }
                    int row = base + bit;
                    int dictionaryIndex = dictionaryIndices[row];
                    boolean matches = dictionaryIndex >= 0
                            ? entryMatches[dictionaryIndex]
                            : delegate.testValue(
                                    values.bytes, values.offsets[row], values.offsets[row + 1]);
                    if (matches) {
                        word |= 1L << bit;
                    }
                }
                outWords[wordIndex] = word;
            }
        }
    }

    /// Benchmark-only one-pass lazy strategy. The first row referencing an
    /// unknown ID decides that entry and immediately consumes the outcome, so
    /// sparse dictionaries avoid eager work without a separate discovery pass.
    private static final class InlineLazyDictionaryMatcher implements BinaryBatchMatcher {

        private static final byte UNKNOWN = 0;
        private static final byte NO_MATCH = 1;
        private static final byte MATCH = 2;

        private final BinaryBatchMatcher delegate;
        private Dictionary.ByteArrayDictionary cachedDictionary;
        private byte[] entryStates;

        private InlineLazyDictionaryMatcher(BinaryBatchMatcher delegate) {
            this.delegate = delegate;
        }

        @Override
        public void test(BatchExchange.Batch batch, long[] outWords) {
            BinaryBatchValues values = (BinaryBatchValues) batch.values;
            Dictionary.ByteArrayDictionary dictionary = values.dictionary;
            if (dictionary == null) {
                delegate.test(batch, outWords);
                return;
            }
            int[] dictionaryIndices = values.dictIndices;
            if (dictionaryIndices == null) {
                throw new IllegalStateException(
                        "A binary batch with a dictionary must retain its dictionary indices");
            }
            if (dictionary != cachedDictionary) {
                cachedDictionary = dictionary;
                int size = dictionary.size();
                if (entryStates == null || entryStates.length < size) {
                    entryStates = new byte[size];
                }
                else {
                    Arrays.fill(entryStates, 0, size, UNKNOWN);
                }
            }
            writeMatches(values, dictionaryIndices, batch.validity, batch.recordCount, outWords);
        }

        @Override
        public boolean testValue(byte[] bytes, int from, int to) {
            return delegate.testValue(bytes, from, to);
        }

        private void writeMatches(BinaryBatchValues values, int[] dictionaryIndices,
                                  long[] validity, int recordCount, long[] outWords) {
            int activeWords = (recordCount + 63) >>> 6;
            for (int wordIndex = 0; wordIndex < activeWords; wordIndex++) {
                int base = wordIndex << 6;
                int rows = Math.min(64, recordCount - base);
                long present = validity != null ? validity[wordIndex] : -1L;
                long word = 0L;
                for (int bit = 0; bit < rows; bit++) {
                    if ((present & (1L << bit)) == 0L) {
                        continue;
                    }
                    int row = base + bit;
                    int dictionaryIndex = dictionaryIndices[row];
                    boolean matches;
                    if (dictionaryIndex < 0) {
                        matches = delegate.testValue(
                                values.bytes, values.offsets[row], values.offsets[row + 1]);
                    }
                    else {
                        byte state = entryStates[dictionaryIndex];
                        if (state == UNKNOWN) {
                            byte[] entry = cachedDictionary.values()[dictionaryIndex];
                            state = delegate.testValue(entry, 0, entry.length) ? MATCH : NO_MATCH;
                            entryStates[dictionaryIndex] = state;
                        }
                        matches = state == MATCH;
                    }
                    if (matches) {
                        word |= 1L << bit;
                    }
                }
                outWords[wordIndex] = word;
            }
        }
    }
}
