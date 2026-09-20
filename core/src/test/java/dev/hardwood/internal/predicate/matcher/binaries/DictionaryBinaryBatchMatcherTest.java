/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.matcher.binaries;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.predicate.BinaryBatchMatcher;
import dev.hardwood.internal.predicate.DictionaryBinaryBatchMatcher;
import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate.Comparison;
import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.internal.reader.BinaryBatchValues;
import dev.hardwood.internal.reader.Dictionary;
import dev.hardwood.metadata.PhysicalType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DictionaryBinaryBatchMatcherTest {

    @Test
    void decidesOnlyReferencedEntriesOnceAcrossBatches() throws Exception {
        Dictionary.ByteArrayDictionary dictionary =
                dictionary(utf8("a"), utf8("match"), utf8("c"), utf8("never"));
        CountingMatcher delegate = new CountingMatcher();
        DictionaryBinaryBatchMatcher matcher = new DictionaryBinaryBatchMatcher(delegate);

        long[] first = run(matcher, binaryBatch(
                dictionary, new int[]{0, 2, -1}, null,
                utf8("a"), utf8("c"), utf8("match")));

        assertThat(first).containsExactly(1L << 2);
        assertThat(delegate.callsFor("a")).isEqualTo(1);
        assertThat(delegate.callsFor("c")).isEqualTo(1);
        assertThat(delegate.callsFor("match")).isEqualTo(1);
        assertThat(delegate.callsFor("never")).isZero();

        BatchExchange.Batch secondBatch = binaryBatch(
                dictionary, new int[]{2, 0, 1}, null,
                utf8("c"), utf8("a"), utf8("match"));
        assertThat(run(matcher, secondBatch)).containsExactly(1L << 2);
        assertThat(run(matcher, secondBatch)).containsExactly(1L << 2);

        assertThat(delegate.callsFor("a")).isEqualTo(1);
        assertThat(delegate.callsFor("c")).isEqualTo(1);
        assertThat(delegate.callsFor("match")).isEqualTo(2);
        assertThat(delegate.callsFor("never")).isZero();
    }

    @Test
    void resetsEntryStatesForANewDictionaryObject() throws Exception {
        CountingMatcher delegate = new CountingMatcher();
        DictionaryBinaryBatchMatcher matcher = new DictionaryBinaryBatchMatcher(delegate);

        Dictionary.ByteArrayDictionary first = dictionary(utf8("match"));
        Dictionary.ByteArrayDictionary second = dictionary(utf8("match"));
        assertThat(run(matcher, binaryBatch(first, new int[]{0}, null, utf8("match"))))
                .containsExactly(1L);
        assertThat(run(matcher, binaryBatch(second, new int[]{0}, null, utf8("match"))))
                .containsExactly(1L);

        assertThat(delegate.callsFor("match")).isEqualTo(2);
    }

    @Test
    void skipsNullRowsWhenDecidingReferencedEntries() throws Exception {
        CountingMatcher delegate = new CountingMatcher();
        DictionaryBinaryBatchMatcher matcher = new DictionaryBinaryBatchMatcher(delegate);
        Dictionary.ByteArrayDictionary dictionary =
                dictionary(utf8("match"), utf8("null-entry"));

        long[] out = run(matcher, binaryBatch(
                dictionary, new int[]{0, 1}, new long[]{1L},
                utf8("match"), utf8("null-entry")));

        assertThat(out).containsExactly(1L);
        assertThat(delegate.callsFor("match")).isEqualTo(1);
        assertThat(delegate.callsFor("null-entry")).isZero();
    }

    @Test
    void delegatesAPlainBatchToTheOptimizedWholeBatchOperation() {
        CountingMatcher delegate = new CountingMatcher();
        DictionaryBinaryBatchMatcher matcher = new DictionaryBinaryBatchMatcher(delegate);
        BatchExchange.Batch batch = binaryBatch(
                null, null, null, utf8("a"), utf8("match"), utf8("c"));

        assertThat(run(matcher, batch)).containsExactly(0b010L);
        assertThat(delegate.batchCalls).isEqualTo(1);
        assertThat(delegate.valueCalls).isZero();
    }

    @Test
    void rejectsADictionaryBatchWithoutRetainedIndices() throws Exception {
        DictionaryBinaryBatchMatcher matcher =
                new DictionaryBinaryBatchMatcher(new CountingMatcher());
        BatchExchange.Batch batch = binaryBatch(
                dictionary(utf8("a")), null, null, utf8("a"));

        assertThatThrownBy(() -> run(matcher, batch))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("A binary batch with a dictionary must retain its dictionary indices");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("binaryOperators")
    void cachesOutcomesForEveryBinaryOperator(
            String description, BinaryBatchMatcher delegate, long expected) throws Exception {
        Dictionary.ByteArrayDictionary dictionary =
                dictionary(utf8("a"), utf8("b"), utf8("c"));
        BatchExchange.Batch batch = binaryBatch(
                dictionary, new int[]{0, 1, 2}, null,
                utf8("a"), utf8("b"), utf8("c"));

        assertThat(run(new DictionaryBinaryBatchMatcher(delegate), batch))
                .as(description)
                .containsExactly(expected);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("comparisonSemantics")
    void dictionaryOutcomesMatchPackedValuesAcrossComparisonModes(
            String description, BinaryBatchMatcher delegate, byte[][] values) throws Exception {
        int[] indices = new int[values.length];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        BatchExchange.Batch packed = binaryBatch(null, null, null, values);
        BatchExchange.Batch dictionaryBacked =
                binaryBatch(dictionary(values), indices, null, values);

        long[] expected = run(delegate, packed);
        assertThat(run(new DictionaryBinaryBatchMatcher(delegate), dictionaryBacked))
                .as(description)
                .containsExactly(expected);
    }

    @Test
    void variableDecimalEqualityUsesTheDelegatesValueSemantics() throws Exception {
        byte[] canonical = {0x7F};
        byte[] padded = {0x00, 0x7F};
        Dictionary.ByteArrayDictionary dictionary = dictionary(padded);
        BinaryBatchMatcher matcher = new DictionaryBinaryBatchMatcher(
                new BinaryEqBatchMatcher(canonical, Comparison.VARIABLE_DECIMAL));

        assertThat(run(matcher, binaryBatch(
                dictionary, new int[]{0}, null, padded))).containsExactly(1L);
    }

    @Test
    void writesMatchesAcrossAWordBoundary() throws Exception {
        byte[] a = utf8("a");
        byte[] b = utf8("b");
        Dictionary.ByteArrayDictionary dictionary = dictionary(a, b);
        int[] indices = new int[70];
        byte[][] values = new byte[70][];
        long firstWord = 0L;
        long secondWord = 0L;
        for (int i = 0; i < values.length; i++) {
            boolean match = (i & 1) != 0;
            indices[i] = match ? 1 : 0;
            values[i] = match ? b : a;
            if (match && i < 64) {
                firstWord |= 1L << i;
            }
            else if (match) {
                secondWord |= 1L << (i - 64);
            }
        }

        BinaryBatchMatcher matcher = new DictionaryBinaryBatchMatcher(
                new BinaryEqBatchMatcher(b, Comparison.BYTE_STRING));
        assertThat(run(matcher, binaryBatch(dictionary, indices, null, values)))
                .containsExactly(firstWord, secondWord);
    }

    private static Stream<Arguments> binaryOperators() {
        byte[] b = utf8("b");
        byte[][] aAndC = {utf8("a"), utf8("c")};
        return Stream.of(
                Arguments.of("eq", new BinaryEqBatchMatcher(b, Comparison.BYTE_STRING), 0b010L),
                Arguments.of("not eq", new BinaryNotEqBatchMatcher(b, Comparison.BYTE_STRING), 0b101L),
                Arguments.of("less than", new BinaryLtBatchMatcher(b, Comparison.BYTE_STRING), 0b001L),
                Arguments.of("less than or equal", new BinaryLtEqBatchMatcher(b, Comparison.BYTE_STRING), 0b011L),
                Arguments.of("greater than", new BinaryGtBatchMatcher(b, Comparison.BYTE_STRING), 0b100L),
                Arguments.of("greater than or equal", new BinaryGtEqBatchMatcher(b, Comparison.BYTE_STRING), 0b110L),
                Arguments.of("in", new BinaryInBatchMatcher(aAndC, Comparison.BYTE_STRING), 0b101L),
                Arguments.of("short in",
                        new BinaryShortInBatchMatcher(aAndC, Comparison.BYTE_STRING, false), 0b101L));
    }

    private static Stream<Arguments> comparisonSemantics() {
        List<Arguments> arguments = new ArrayList<>();
        addComparisonCases(arguments, "byte string", Comparison.BYTE_STRING,
                new byte[][]{utf8("a"), utf8("b"), utf8("c")},
                utf8("b"), new byte[][]{utf8("a"), utf8("c")});
        addComparisonCases(arguments, "stored bytes", Comparison.STORED_BYTES,
                new byte[][]{{0x00}, {0x7F}, {(byte) 0xFF}},
                new byte[]{0x7F}, new byte[][]{{0x00}, {(byte) 0xFF}});
        addComparisonCases(arguments, "fixed decimal", Comparison.FIXED_DECIMAL,
                new byte[][]{int16(-256), int16(-1), int16(0), int16(1), int16(256)},
                int16(0), new byte[][]{int16(-1), int16(256)});
        addComparisonCases(arguments, "variable decimal", Comparison.VARIABLE_DECIMAL,
                new byte[][]{
                        {(byte) 0xFF, 0x00},
                        {(byte) 0x9C},
                        {0x7F},
                        {0x00, 0x7F},
                        {0x00, (byte) 0x80}},
                new byte[]{0x7F}, new byte[][]{{(byte) 0x9C}, {0x00, (byte) 0x80}});

        byte[][] shortMembers = {utf8("a"), utf8("c")};
        byte[][] shortValues = {utf8("a"), utf8("b"), utf8("c")};
        arguments.add(Arguments.of("short byte-exact membership",
                new BinaryShortInBatchMatcher(shortMembers, Comparison.BYTE_STRING, false),
                shortValues));
        arguments.add(Arguments.of("negated short byte-exact membership",
                new BinaryShortInBatchMatcher(shortMembers, Comparison.BYTE_STRING, true),
                shortValues));
        return arguments.stream();
    }

    private static void addComparisonCases(
            List<Arguments> arguments, String name, Comparison comparison,
            byte[][] values, byte[] literal, byte[][] members) {
        arguments.add(Arguments.of(name + " eq",
                new BinaryEqBatchMatcher(literal, comparison), values));
        arguments.add(Arguments.of(name + " not eq",
                new BinaryNotEqBatchMatcher(literal, comparison), values));
        arguments.add(Arguments.of(name + " less than",
                new BinaryLtBatchMatcher(literal, comparison), values));
        arguments.add(Arguments.of(name + " less than or equal",
                new BinaryLtEqBatchMatcher(literal, comparison), values));
        arguments.add(Arguments.of(name + " greater than",
                new BinaryGtBatchMatcher(literal, comparison), values));
        arguments.add(Arguments.of(name + " greater than or equal",
                new BinaryGtEqBatchMatcher(literal, comparison), values));
        arguments.add(Arguments.of(name + " in",
                new BinaryInBatchMatcher(members, comparison), values));
    }

    private static long[] run(BinaryBatchMatcher matcher, BatchExchange.Batch batch) {
        long[] out = new long[(batch.recordCount + 63) >>> 6];
        matcher.test(batch, out);
        int tail = batch.recordCount & 63;
        if (tail != 0) {
            out[out.length - 1] &= (1L << tail) - 1L;
        }
        return out;
    }

    private static BatchExchange.Batch binaryBatch(
            Dictionary.ByteArrayDictionary dictionary, int[] dictionaryIndices,
            long[] validity, byte[]... values) {
        int[] offsets = new int[values.length + 1];
        for (int i = 0; i < values.length; i++) {
            offsets[i + 1] = offsets[i] + values[i].length;
        }
        byte[] bytes = new byte[offsets[values.length]];
        for (int i = 0; i < values.length; i++) {
            System.arraycopy(values[i], 0, bytes, offsets[i], values[i].length);
        }

        BinaryBatchValues binaryValues = new BinaryBatchValues(bytes, offsets);
        binaryValues.dictionary = dictionary;
        binaryValues.dictIndices = dictionaryIndices;

        BatchExchange.Batch batch = new BatchExchange.Batch();
        batch.values = binaryValues;
        batch.validity = validity;
        batch.recordCount = values.length;
        return batch;
    }

    private static Dictionary.ByteArrayDictionary dictionary(byte[]... values) throws Exception {
        int encodedSize = 0;
        for (byte[] value : values) {
            encodedSize += Integer.BYTES + value.length;
        }
        ByteBuffer data = ByteBuffer.allocate(encodedSize).order(ByteOrder.LITTLE_ENDIAN);
        for (byte[] value : values) {
            data.putInt(value.length);
            data.put(value);
        }
        return (Dictionary.ByteArrayDictionary) Dictionary.parse(
                data.array(), values.length, PhysicalType.BYTE_ARRAY, null);
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] int16(int value) {
        return new byte[]{(byte) (value >> 8), (byte) value};
    }

    private static final class CountingMatcher implements BinaryBatchMatcher {
        private final Map<String, Integer> calls = new HashMap<>();
        private int batchCalls;
        private int valueCalls;

        @Override
        public void test(BatchExchange.Batch batch, long[] outWords) {
            batchCalls++;
            outWords[0] = 0b010L;
        }

        @Override
        public boolean testValue(byte[] bytes, int from, int to) {
            valueCalls++;
            String value = new String(bytes, from, to - from, StandardCharsets.UTF_8);
            calls.merge(value, 1, Integer::sum);
            return value.equals("match");
        }

        int callsFor(String value) {
            return calls.getOrDefault(value, 0);
        }
    }
}
