/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding.simd;

import java.util.BitSet;
import java.util.Random;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for SIMD operations, validating both scalar and SIMD implementations
 * produce identical results.
 */
class SimdOperationsTest {

    private static final SimdOperations SCALAR = new ScalarOperations();
    private static SimdOperations SIMD;
    private static final Random RANDOM = new Random(42);

    @BeforeAll
    static void setup() {
        SIMD = VectorSupport.operations();
        System.out.println("SIMD implementation: " + VectorSupport.implementationName());
        System.out.println("SIMD available: " + VectorSupport.isAvailable());
    }

    // ==================== countNonNulls Tests ====================

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 1000, 8192})
    void countNonNullsMatchesScalar(int size) {
        int[] defLevels = generateDefLevels(size);
        int maxDef = 3;

        int scalarResult = SCALAR.countNonNulls(defLevels, maxDef);
        int simdResult = SIMD.countNonNulls(defLevels, maxDef);

        assertThat(simdResult).isEqualTo(scalarResult);
    }

    @Test
    void countNonNullsAllNulls() {
        int[] defLevels = new int[100];
        // All zeros, maxDef = 1 means all are null
        assertThat(SIMD.countNonNulls(defLevels, 1)).isEqualTo(0);
    }

    @Test
    void countNonNullsNoNulls() {
        int[] defLevels = IntStream.range(0, 100).map(i -> 3).toArray();
        assertThat(SIMD.countNonNulls(defLevels, 3)).isEqualTo(100);
    }

    @Test
    void countNonNullsAlternating() {
        int[] defLevels = new int[100];
        for (int i = 0; i < 100; i++) {
            defLevels[i] = i % 2 == 0 ? 3 : 0;
        }
        assertThat(SIMD.countNonNulls(defLevels, 3)).isEqualTo(50);
    }

    // ==================== markNulls Tests ====================

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 1000})
    void markNullsMatchesScalar(int count) {
        int[] defLevels = generateDefLevels(count + 10);
        int srcPos = 5;
        int destPos = 3;
        int maxDefLevel = 3;

        BitSet scalarNulls = new BitSet(count + destPos);
        BitSet simdNulls = new BitSet(count + destPos);

        SCALAR.markNulls(scalarNulls, defLevels, srcPos, destPos, count, maxDefLevel);
        SIMD.markNulls(simdNulls, defLevels, srcPos, destPos, count, maxDefLevel);

        assertThat(simdNulls).isEqualTo(scalarNulls);
    }

    @Test
    void markNullsWithNullBitSet() {
        int[] defLevels = generateDefLevels(100);
        // Should not throw
        SIMD.markNulls(null, defLevels, 0, 0, 100, 3);
    }

    @Test
    void markNullsAllNull() {
        int[] defLevels = new int[64]; // All zeros
        BitSet nulls = new BitSet(64);
        SIMD.markNulls(nulls, defLevels, 0, 0, 64, 3);
        assertThat(nulls.cardinality()).isEqualTo(64);
    }

    @Test
    void markNullsNoneNull() {
        int[] defLevels = IntStream.range(0, 64).map(i -> 3).toArray();
        BitSet nulls = new BitSet(64);
        SIMD.markNulls(nulls, defLevels, 0, 0, 64, 3);
        assertThat(nulls.cardinality()).isEqualTo(0);
    }

    // ==================== unpackBitWidth1 Tests ====================

    @ParameterizedTest
    @ValueSource(ints = {8, 16, 24, 32, 64, 128, 256, 512})
    void unpackBitWidth1MatchesScalar(int count) {
        int byteCount = count / 8;
        byte[] data = new byte[byteCount + 5];
        RANDOM.nextBytes(data);
        int dataPos = 2;

        int[] scalarOutput = new int[count + 10];
        int[] simdOutput = new int[count + 10];
        int outPos = 3;

        int scalarBytes = SCALAR.unpackBitWidth1(data, dataPos, scalarOutput, outPos, count);
        int simdBytes = SIMD.unpackBitWidth1(data, dataPos, simdOutput, outPos, count);

        assertThat(simdBytes).isEqualTo(scalarBytes);
        assertThat(simdOutput).isEqualTo(scalarOutput);
    }

    @Test
    void unpackBitWidth1CorrectValues() {
        byte[] data = { (byte) 0b10101010, (byte) 0b01010101 };
        int[] output = new int[16];

        SIMD.unpackBitWidth1(data, 0, output, 0, 16);

        // First byte: 0b10101010 -> bits 0,2,4,6 are 0; bits 1,3,5,7 are 1
        assertThat(output[0]).isEqualTo(0);
        assertThat(output[1]).isEqualTo(1);
        assertThat(output[2]).isEqualTo(0);
        assertThat(output[3]).isEqualTo(1);
        assertThat(output[4]).isEqualTo(0);
        assertThat(output[5]).isEqualTo(1);
        assertThat(output[6]).isEqualTo(0);
        assertThat(output[7]).isEqualTo(1);

        // Second byte: 0b01010101 -> opposite pattern
        assertThat(output[8]).isEqualTo(1);
        assertThat(output[9]).isEqualTo(0);
        assertThat(output[10]).isEqualTo(1);
        assertThat(output[11]).isEqualTo(0);
    }

    // ==================== unpackBitWidthN Tests ====================

    @ParameterizedTest
    @ValueSource(ints = {2, 3, 4, 5, 6, 7, 8})
    void unpackBitWidthNMatchesScalar(int bitWidth) {
        int count = 64;
        byte[] data = new byte[bitWidth * (count / 8) + 10];
        RANDOM.nextBytes(data);
        int dataPos = 2;

        int[] scalarOutput = new int[count + 10];
        int[] simdOutput = new int[count + 10];
        int outPos = 3;

        int scalarBytes = SCALAR.unpackBitWidthN(data, dataPos, scalarOutput, outPos, count, bitWidth);
        int simdBytes = SIMD.unpackBitWidthN(data, dataPos, simdOutput, outPos, count, bitWidth);

        assertThat(simdBytes).isEqualTo(scalarBytes);
        assertThat(simdOutput).isEqualTo(scalarOutput);
    }

    @Test
    void unpackBitWidth2CorrectValues() {
        // 2 bytes = 8 values for bit-width 2
        byte[] data = { (byte) 0b11100100, (byte) 0b00011011 };
        int[] output = new int[8];

        SIMD.unpackBitWidthN(data, 0, output, 0, 8, 2);

        // First byte: 0b11100100 -> values 0,1,2,3 (reading 2 bits at a time)
        // Bits 0-1: 00 = 0
        // Bits 2-3: 01 = 1
        // Bits 4-5: 10 = 2
        // Bits 6-7: 11 = 3
        assertThat(output[0]).isEqualTo(0);
        assertThat(output[1]).isEqualTo(1);
        assertThat(output[2]).isEqualTo(2);
        assertThat(output[3]).isEqualTo(3);

        // Second byte: 0b00011011 -> values 3,2,1,0
        assertThat(output[4]).isEqualTo(3);
        assertThat(output[5]).isEqualTo(2);
        assertThat(output[6]).isEqualTo(1);
        assertThat(output[7]).isEqualTo(0);
    }

    // ==================== Dictionary Application Tests ====================

    @ParameterizedTest
    @ValueSource(ints = {1, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 1000})
    void applyDictionaryIntsMatchesScalar(int count) {
        int[] dict = {100, 200, 300, 400, 500, 600, 700, 800};
        int[] indices = new int[count];
        for (int i = 0; i < count; i++) {
            indices[i] = RANDOM.nextInt(dict.length);
        }

        int[] scalarOutput = new int[count];
        int[] simdOutput = new int[count];

        SCALAR.applyDictionaryInts(scalarOutput, dict, indices, count);
        SIMD.applyDictionaryInts(simdOutput, dict, indices, count);

        assertThat(simdOutput).isEqualTo(scalarOutput);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 1000})
    void applyDictionaryLongsMatchesScalar(int count) {
        long[] dict = {100L, 200L, 300L, 400L, 500L};
        int[] indices = new int[count];
        for (int i = 0; i < count; i++) {
            indices[i] = RANDOM.nextInt(dict.length);
        }

        long[] scalarOutput = new long[count];
        long[] simdOutput = new long[count];

        SCALAR.applyDictionaryLongs(scalarOutput, dict, indices, count);
        SIMD.applyDictionaryLongs(simdOutput, dict, indices, count);

        assertThat(simdOutput).isEqualTo(scalarOutput);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 1000})
    void applyDictionaryDoublesMatchesScalar(int count) {
        double[] dict = {1.1, 2.2, 3.3, 4.4, 5.5};
        int[] indices = new int[count];
        for (int i = 0; i < count; i++) {
            indices[i] = RANDOM.nextInt(dict.length);
        }

        double[] scalarOutput = new double[count];
        double[] simdOutput = new double[count];

        SCALAR.applyDictionaryDoubles(scalarOutput, dict, indices, count);
        SIMD.applyDictionaryDoubles(simdOutput, dict, indices, count);

        assertThat(simdOutput).isEqualTo(scalarOutput);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 1000})
    void applyDictionaryFloatsMatchesScalar(int count) {
        float[] dict = {1.1f, 2.2f, 3.3f, 4.4f, 5.5f, 6.6f, 7.7f};
        int[] indices = new int[count];
        for (int i = 0; i < count; i++) {
            indices[i] = RANDOM.nextInt(dict.length);
        }

        float[] scalarOutput = new float[count];
        float[] simdOutput = new float[count];

        SCALAR.applyDictionaryFloats(scalarOutput, dict, indices, count);
        SIMD.applyDictionaryFloats(simdOutput, dict, indices, count);

        assertThat(simdOutput).isEqualTo(scalarOutput);
    }

    @Test
    void applyDictionaryCorrectValues() {
        int[] dict = {100, 200, 300, 400, 500};
        int[] indices = {0, 2, 4, 1, 3, 0, 2, 4, 1, 3, 0, 2, 4, 1, 3, 0};
        int[] output = new int[16];

        SIMD.applyDictionaryInts(output, dict, indices, 16);

        assertThat(output[0]).isEqualTo(100);
        assertThat(output[1]).isEqualTo(300);
        assertThat(output[2]).isEqualTo(500);
        assertThat(output[3]).isEqualTo(200);
        assertThat(output[4]).isEqualTo(400);
    }

    // ==================== Helper Methods ====================

    private int[] generateDefLevels(int size) {
        int[] defLevels = new int[size];
        for (int i = 0; i < size; i++) {
            // Generate def levels 0-3 with ~25% each
            defLevels[i] = RANDOM.nextInt(4);
        }
        return defLevels;
    }
}
