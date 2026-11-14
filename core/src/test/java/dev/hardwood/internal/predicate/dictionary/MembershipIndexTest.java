/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.dictionary;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.metadata.PhysicalType;

import static dev.hardwood.internal.predicate.dictionary.MembershipIndex.BINARY_PASS_LIMIT;
import static dev.hardwood.internal.predicate.dictionary.MembershipIndex.PASS_LIMIT;
import static dev.hardwood.internal.predicate.dictionary.MembershipIndex.SPARSE_PASS_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;

/// The kernel each list type's index chooses at the edges of its thresholds, and the decision that
/// kernel reaches through [DictionaryFilterSupport] on either side of each edge. Exactly one kernel
/// is set, the invariant the scans dispatch on. `DictionaryFilterSupportTest` drives the kernels
/// through randomised and edge-value dictionaries.
class MembershipIndexTest {

    /// `count` values from `first` upwards, `step` apart.
    private static long[] longs(int count, long first, long step) {
        long[] values = new long[count];
        for (int i = 0; i < count; i++) {
            values[i] = first + i * step;
        }
        return values;
    }

    private static int[] ints(int count, int first, int step) {
        int[] values = new int[count];
        for (int i = 0; i < count; i++) {
            values[i] = first + i * step;
        }
        return values;
    }

    private static float[] floats(int count, float first) {
        float[] values = new float[count];
        for (int i = 0; i < count; i++) {
            values[i] = first + i;
        }
        return values;
    }

    private static double[] doubles(int count, double first) {
        double[] values = new double[count];
        for (int i = 0; i < count; i++) {
            values[i] = first + i;
        }
        return values;
    }

    private static byte[][] binaries(int count, String prefix) {
        byte[][] values = new byte[count][];
        for (int i = 0; i < count; i++) {
            values[i] = (prefix + i).getBytes(StandardCharsets.UTF_8);
        }
        return values;
    }

    /// A `BYTE_ARRAY` dictionary of `entries`, parsed from their plain encoding as a page's would be.
    private static Dictionary byteArrayDictionary(byte[]... entries) {
        int encodedSize = 0;
        for (byte[] entry : entries) {
            encodedSize += Integer.BYTES + entry.length;
        }
        ByteBuffer encoded = ByteBuffer.allocate(encodedSize).order(ByteOrder.LITTLE_ENDIAN);
        for (byte[] entry : entries) {
            encoded.putInt(entry.length);
            encoded.put(entry);
        }
        return Dictionary.parse(encoded.array(), encodedSize, entries.length, PhysicalType.BYTE_ARRAY, null);
    }

    private static void assertPasses(MembershipIndex.Longs index) {
        assertThat(index.passes).isNotNull();
        assertThat(index.bitmask).isNull();
        assertThat(index.set).isNull();
    }

    private static void assertBitmask(MembershipIndex.Longs index) {
        assertThat(index.passes).isNull();
        assertThat(index.bitmask).isNotNull();
        assertThat(index.set).isNull();
    }

    private static void assertSet(MembershipIndex.Longs index) {
        assertThat(index.passes).isNull();
        assertThat(index.bitmask).isNull();
        assertThat(index.set).isNotNull();
    }

    private static void assertPasses(MembershipIndex.Ints index) {
        assertThat(index.passes).isNotNull();
        assertThat(index.bitmask).isNull();
        assertThat(index.set).isNull();
    }

    private static void assertBitmask(MembershipIndex.Ints index) {
        assertThat(index.passes).isNull();
        assertThat(index.bitmask).isNotNull();
        assertThat(index.set).isNull();
    }

    private static void assertSet(MembershipIndex.Ints index) {
        assertThat(index.passes).isNull();
        assertThat(index.bitmask).isNull();
        assertThat(index.set).isNotNull();
    }

    @Test
    void longListsTakePassesThenABitmaskOrPassesThenASet() {
        // Close literals: passes up to the limit, a bitmask above it. The dictionary holds the
        // list's last literal, so each kernel is driven to both decisions.
        Dictionary dictionary = new Dictionary.LongDictionary(new long[]{ -1L, PASS_LIMIT });
        MembershipIndex.Longs closeShort = MembershipIndex.Longs.of(longs(PASS_LIMIT, 0, 1));
        assertPasses(closeShort);
        assertThat(DictionaryFilterSupport.absentAll(dictionary, closeShort)).isTrue();
        MembershipIndex.Longs closeLong = MembershipIndex.Longs.of(longs(PASS_LIMIT + 1, 0, 1));
        assertBitmask(closeLong);
        assertThat(DictionaryFilterSupport.absentAll(dictionary, closeLong)).isFalse();

        // Spread literals: passes up to the sparse limit, a set above it.
        long step = RangeBitmask.SPAN_LIMIT;
        Dictionary spread = new Dictionary.LongDictionary(new long[]{ -1L, SPARSE_PASS_LIMIT * step });
        MembershipIndex.Longs spreadShort = MembershipIndex.Longs.of(longs(SPARSE_PASS_LIMIT, 0, step));
        assertPasses(spreadShort);
        assertThat(DictionaryFilterSupport.absentAll(spread, spreadShort)).isTrue();
        MembershipIndex.Longs spreadLong = MembershipIndex.Longs.of(longs(SPARSE_PASS_LIMIT + 1, 0, step));
        assertSet(spreadLong);
        assertThat(DictionaryFilterSupport.absentAll(spread, spreadLong)).isFalse();
    }

    @Test
    void intListsTakePassesThenABitmaskOrPassesThenASet() {
        Dictionary dictionary = new Dictionary.IntDictionary(new int[]{ -1, PASS_LIMIT });
        MembershipIndex.Ints closeShort = MembershipIndex.Ints.of(ints(PASS_LIMIT, 0, 1));
        assertPasses(closeShort);
        assertThat(DictionaryFilterSupport.absentAll(dictionary, closeShort)).isTrue();
        MembershipIndex.Ints closeLong = MembershipIndex.Ints.of(ints(PASS_LIMIT + 1, 0, 1));
        assertBitmask(closeLong);
        assertThat(DictionaryFilterSupport.absentAll(dictionary, closeLong)).isFalse();

        int step = 1 << 24;
        Dictionary spread = new Dictionary.IntDictionary(new int[]{ -1, SPARSE_PASS_LIMIT * step });
        MembershipIndex.Ints spreadShort = MembershipIndex.Ints.of(ints(SPARSE_PASS_LIMIT, 0, step));
        assertPasses(spreadShort);
        assertThat(DictionaryFilterSupport.absentAll(spread, spreadShort)).isTrue();
        MembershipIndex.Ints spreadLong = MembershipIndex.Ints.of(ints(SPARSE_PASS_LIMIT + 1, 0, step));
        assertSet(spreadLong);
        assertThat(DictionaryFilterSupport.absentAll(spread, spreadLong)).isFalse();
    }

    @Test
    void floatingPointListsTakePassesThenASet() {
        Dictionary floatDictionary = new Dictionary.FloatDictionary(new float[]{ -1f, SPARSE_PASS_LIMIT });
        MembershipIndex.Floats floatPasses = MembershipIndex.Floats.of(floats(SPARSE_PASS_LIMIT, 0f));
        assertThat(floatPasses.passes).isNotNull();
        assertThat(floatPasses.set).isNull();
        assertThat(DictionaryFilterSupport.absentAll(floatDictionary, floatPasses)).isTrue();
        MembershipIndex.Floats floatSet = MembershipIndex.Floats.of(floats(SPARSE_PASS_LIMIT + 1, 0f));
        assertThat(floatSet.passes).isNull();
        assertThat(floatSet.set).isNotNull();
        assertThat(floatSet.nan).isFalse();
        assertThat(DictionaryFilterSupport.absentAll(floatDictionary, floatSet)).isFalse();

        Dictionary doubleDictionary = new Dictionary.DoubleDictionary(new double[]{ -1, SPARSE_PASS_LIMIT });
        MembershipIndex.Doubles doublePasses = MembershipIndex.Doubles.of(doubles(SPARSE_PASS_LIMIT, 0));
        assertThat(doublePasses.passes).isNotNull();
        assertThat(doublePasses.set).isNull();
        assertThat(DictionaryFilterSupport.absentAll(doubleDictionary, doublePasses)).isTrue();
        MembershipIndex.Doubles doubleSet = MembershipIndex.Doubles.of(doubles(SPARSE_PASS_LIMIT + 1, 0));
        assertThat(doubleSet.passes).isNull();
        assertThat(doubleSet.set).isNotNull();
        assertThat(DictionaryFilterSupport.absentAll(doubleDictionary, doubleSet)).isFalse();
    }

    @Test
    void theNaNFlagHoldsInEitherForm() {
        float[] shortList = floats(SPARSE_PASS_LIMIT, 0f);
        shortList[0] = Float.NaN;
        assertThat(MembershipIndex.Floats.of(shortList).nan).isTrue();
        double[] longList = doubles(SPARSE_PASS_LIMIT + 1, 0);
        longList[0] = Double.NaN;
        assertThat(MembershipIndex.Doubles.of(longList).nan).isTrue();
    }

    @Test
    void aFloatingPointListOfNaNsAloneHasNoSet() {
        float[] floatNaNs = new float[SPARSE_PASS_LIMIT + 1];
        Arrays.fill(floatNaNs, Float.NaN);
        MembershipIndex.Floats floats = MembershipIndex.Floats.of(floatNaNs);
        assertThat(floats.nan).isTrue();
        assertThat(floats.set).isNull();
        assertThat(DictionaryFilterSupport.absentAll(new Dictionary.FloatDictionary(new float[]{ 1f }), floats)).isTrue();
        assertThat(DictionaryFilterSupport.absentAll(new Dictionary.FloatDictionary(new float[]{ Float.NaN }), floats)).isFalse();

        double[] doubleNaNs = new double[SPARSE_PASS_LIMIT + 1];
        Arrays.fill(doubleNaNs, Double.NaN);
        MembershipIndex.Doubles doubles = MembershipIndex.Doubles.of(doubleNaNs);
        assertThat(doubles.nan).isTrue();
        assertThat(doubles.set).isNull();
    }

    @Test
    void float16ListsAlwaysTakeABitmaskOverTheHalvesTheyRepresent() {
        assertThat(MembershipIndex.Float16s.of(new float[]{ 1.5f }).bitmask).isNotNull();
        // A literal binary16 cannot represent, and a NaN, leave no bitmask to probe.
        MembershipIndex.Float16s unrepresentable = MembershipIndex.Float16s.of(new float[]{ Math.nextUp(1.0f), Float.NaN });
        assertThat(unrepresentable.bitmask).isNull();
        assertThat(unrepresentable.nan).isTrue();
    }

    @Test
    void byteStringListsTakePassesThenASet() {
        Dictionary dictionary = byteArrayDictionary("value_" + BINARY_PASS_LIMIT);
        MembershipIndex.Binaries passes = MembershipIndex.Binaries.of(binaries(BINARY_PASS_LIMIT, "value_"));
        assertThat(passes.passes).isNotNull();
        assertThat(passes.set).isNull();
        assertThat(DictionaryFilterSupport.absentAll(dictionary, passes)).isTrue();
        MembershipIndex.Binaries set = MembershipIndex.Binaries.of(binaries(BINARY_PASS_LIMIT + 1, "value_"));
        assertThat(set.passes).isNull();
        assertThat(set.set).isNotNull();
        assertThat(DictionaryFilterSupport.absentAll(dictionary, set)).isFalse();
    }

    @Test
    void anEmptyListMatchesNothing() {
        // Resolution can leave a list empty, for instance when no literal fits the column; such a
        // list proves every dictionary absent.
        MembershipIndex.Ints ints = MembershipIndex.Ints.of(new int[0]);
        assertPasses(ints);
        assertThat(DictionaryFilterSupport.absentAll(new Dictionary.IntDictionary(new int[]{ 0 }), ints)).isTrue();
        MembershipIndex.Float16s halves = MembershipIndex.Float16s.of(new float[0]);
        assertThat(halves.bitmask).isNull();
        assertThat(halves.nan).isFalse();
        assertThat(DictionaryFilterSupport.absentAll(byteArrayDictionary(new byte[]{ 0, 0x3c }), halves)).isTrue();
    }

    @Test
    void oneIndexDecidesEveryDictionaryItIsProbedAgainst() {
        // Built once, the index is probed against the dictionaries of several row groups.
        MembershipIndex.Longs index = MembershipIndex.Longs.of(longs(SPARSE_PASS_LIMIT + 1, 0, RangeBitmask.SPAN_LIMIT));
        assertThat(DictionaryFilterSupport.absentAll(new Dictionary.LongDictionary(new long[]{ 1, 2, 3 }), index)).isTrue();
        assertThat(DictionaryFilterSupport.absentAll(new Dictionary.LongDictionary(new long[]{ 7, RangeBitmask.SPAN_LIMIT }), index)).isFalse();
        assertThat(DictionaryFilterSupport.absentAll(new Dictionary.LongDictionary(new long[]{ 5 }), index)).isTrue();
        MembershipIndex.Binaries binaries = MembershipIndex.Binaries.of(binaries(BINARY_PASS_LIMIT + 1, "value_"));
        assertThat(DictionaryFilterSupport.absentAll(byteArrayDictionary("other"), binaries)).isTrue();
        assertThat(DictionaryFilterSupport.absentAll(byteArrayDictionary("other", "value_0"), binaries)).isFalse();
    }

    private static Dictionary byteArrayDictionary(String... entries) {
        byte[][] values = new byte[entries.length][];
        for (int i = 0; i < entries.length; i++) {
            values[i] = entries[i].getBytes(StandardCharsets.UTF_8);
        }
        return byteArrayDictionary(values);
    }
}
