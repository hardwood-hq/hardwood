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
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import dev.hardwood.internal.predicate.matcher.binary.BinaryEqBatchMatcher;
import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.internal.reader.BinaryBatchValues;
import dev.hardwood.internal.reader.Dictionary;
import dev.hardwood.metadata.PhysicalType;

/// Isolates the per-row CPU cost of binary equality over a dictionary-encoded
/// batch: integer dictionary-ID comparison versus comparison of each packed
/// byte range.
///
/// `packedValues` is the matcher's own fallback for rows without a usable
/// dictionary ID, not the path this replaced. Record filtering previously
/// materialised a `byte[]` per row before comparing, so this baseline
/// understates the end-to-end saving — which is the point: it attributes the
/// difference to dictionary-space evaluation alone rather than to avoided
/// allocation.
///
/// Run:
/// ```shell
/// ./mvnw -pl core install -DskipTests
/// ./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar DictionarySpacePredicateBenchmark
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms1g", "-Xmx1g"})
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class DictionarySpacePredicateBenchmark {

    @Param({"16", "256"})
    private int cardinality;

    @Param({"65536"})
    private int rows;

    private BatchExchange.Batch batch;
    private BinaryEqBatchMatcher matcher;
    private byte[] literal;
    private long[] outWords;

    @Setup
    public void setup() throws Exception {
        byte[][] entries = new byte[cardinality][];
        int dictionaryBytes = 0;
        for (int i = 0; i < cardinality; i++) {
            entries[i] = ("category-" + i).getBytes(StandardCharsets.UTF_8);
            dictionaryBytes += Integer.BYTES + entries[i].length;
        }
        ByteBuffer encodedDictionary = ByteBuffer.allocate(dictionaryBytes).order(ByteOrder.LITTLE_ENDIAN);
        for (byte[] entry : entries) {
            encodedDictionary.putInt(entry.length);
            encodedDictionary.put(entry);
        }
        Dictionary.ByteArrayDictionary dictionary =
                (Dictionary.ByteArrayDictionary) Dictionary.parse(
                        encodedDictionary.array(), entries.length, PhysicalType.BYTE_ARRAY, null);

        int packedBytes = 0;
        for (int i = 0; i < rows; i++) {
            packedBytes += entries[i % cardinality].length;
        }
        byte[] bytes = new byte[packedBytes];
        int[] offsets = new int[rows + 1];
        int[] dictionaryIndices = new int[rows];
        int position = 0;
        for (int i = 0; i < rows; i++) {
            int dictionaryId = i % cardinality;
            byte[] entry = entries[dictionaryId];
            System.arraycopy(entry, 0, bytes, position, entry.length);
            position += entry.length;
            offsets[i + 1] = position;
            dictionaryIndices[i] = dictionaryId;
        }

        BinaryBatchValues values = new BinaryBatchValues(bytes, offsets);
        values.dictionary = dictionary;
        values.dictIndices = dictionaryIndices;

        batch = new BatchExchange.Batch();
        batch.values = values;
        batch.recordCount = rows;
        literal = entries[cardinality / 2];
        matcher = new BinaryEqBatchMatcher(literal);
        outWords = new long[(rows + 63) >>> 6];
    }

    @Benchmark
    public long dictionaryIds() {
        matcher.test(batch, outWords);
        return outWords[outWords.length - 1];
    }

    @Benchmark
    public long packedValues() {
        BinaryBatchValues values = (BinaryBatchValues) batch.values;
        int fullWords = rows >>> 6;
        int tail = rows & 63;
        for (int w = 0; w < fullWords; w++) {
            int base = w << 6;
            long word = 0L;
            for (int b = 0; b < 64; b++) {
                int row = base + b;
                int start = values.offsets[row];
                int end = values.offsets[row + 1];
                word |= (Arrays.equals(values.bytes, start, end,
                        literal, 0, literal.length) ? 1L : 0L) << b;
            }
            outWords[w] = word;
        }
        if (tail != 0) {
            int base = fullWords << 6;
            long word = 0L;
            for (int b = 0; b < tail; b++) {
                int row = base + b;
                int start = values.offsets[row];
                int end = values.offsets[row + 1];
                word |= (Arrays.equals(values.bytes, start, end,
                        literal, 0, literal.length) ? 1L : 0L) << b;
            }
            outWords[fullWords] = word;
        }
        return outWords[outWords.length - 1];
    }
}
