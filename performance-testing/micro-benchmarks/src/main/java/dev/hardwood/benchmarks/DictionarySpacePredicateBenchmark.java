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
import dev.hardwood.internal.predicate.matcher.binaries.BinaryShortEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.binaries.BinaryShortInBatchMatcher;
import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.internal.reader.BinaryBatchValues;
import dev.hardwood.internal.reader.Dictionary;
import dev.hardwood.metadata.PhysicalType;

/// Compares [DictionaryBinaryBatchMatcher] with the ordinary per-value loop of its delegate over
/// one dictionary-encoded batch.
///
/// The cold arm installs a fresh wrapper for every invocation, so dictionary preparation and entry
/// decisions occur inside the measured operation. The hot arm reuses a wrapper whose outcomes were
/// populated by a previous batch. `SPARSE` references four entries from a potentially large
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
    private BinaryBatchMatcher coldDictionaryMatcher;
    private BinaryBatchMatcher hotDictionaryMatcher;
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
                        encoded.array(), encoded.array().length,
                        entries.length, PhysicalType.BYTE_ARRAY, null);

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

        BinaryBatchValues values = new BinaryBatchValues(bytes,
                Arrays.copyOf(offsets, rows), Arrays.copyOfRange(offsets, 1, rows + 1), bytes.length);
        values.dictionary = dictionary;
        values.dictIndices = dictionaryIndices;
        batch = new BatchExchange.Batch();
        batch.values = values;
        batch.recordCount = rows;

        int middle = referencedEntries / 2;
        byte[] literal = entries[middle];
        byte[][] members = {entries[0], entries[middle], entries[referencedEntries - 1]};
        packedMatcher = switch (predicateKind) {
            case EQ -> BinaryShortEqBatchMatcher.supports(Comparison.BYTE_STRING, literal)
                    ? new BinaryShortEqBatchMatcher(literal, Comparison.BYTE_STRING, false)
                    : new BinaryEqBatchMatcher(literal, Comparison.BYTE_STRING);
            case LT -> new BinaryLtBatchMatcher(literal, Comparison.BYTE_STRING);
            case IN -> BinaryShortInBatchMatcher.supports(Comparison.BYTE_STRING, members)
                    ? new BinaryShortInBatchMatcher(members, Comparison.BYTE_STRING, false)
                    : new BinaryInBatchMatcher(members, Comparison.BYTE_STRING);
        };
        outWords = new long[(rows + 63) >>> 6];
        verifyEquivalentResults();
        hotDictionaryMatcher = new DictionaryBinaryBatchMatcher(packedMatcher);
        hotDictionaryMatcher.test(batch, outWords);
    }

    @Setup(Level.Invocation)
    public void setupInvocation() {
        coldDictionaryMatcher = new DictionaryBinaryBatchMatcher(packedMatcher);
    }

    @Benchmark
    public long dictionaryCold() {
        coldDictionaryMatcher.test(batch, outWords);
        return checksum();
    }

    @Benchmark
    public long dictionaryHot() {
        hotDictionaryMatcher.test(batch, outWords);
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
        verifyEquivalentResults(new DictionaryBinaryBatchMatcher(packedMatcher), expected);
    }

    private void verifyEquivalentResults(BinaryBatchMatcher matcher, long[] expected) {
        long[] actual = new long[expected.length];
        matcher.test(batch, actual);
        requireEquivalentResults("cold", expected, actual);
        Arrays.fill(actual, 0L);
        matcher.test(batch, actual);
        requireEquivalentResults("hot", expected, actual);
    }

    private void requireEquivalentResults(String contender, long[] expected, long[] actual) {
        if (!Arrays.equals(expected, actual)) {
            throw new IllegalStateException(
                    "The " + contender + " dictionary matcher produced different results from per-value matching");
        }
    }
}
