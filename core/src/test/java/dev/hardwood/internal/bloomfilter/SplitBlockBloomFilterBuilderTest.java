/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.bloomfilter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The write side of the split-block filter: sizing, insertion as [SplitBlockBloomFilter] probes
/// it, and folding to a target false-positive probability.
class SplitBlockBloomFilterBuilderTest {

    @Test
    void sizesForDistinctValuesAndProbabilityAsAPowerOfTwo() {
        // -8 * 1000 / ln(1 - 0.01^(1/8)) = 9,682 bits, 1,211 bytes, rounded up to 2 KiB.
        assertThat(SplitBlockBloomFilterBuilder.optimalNumBytes(1_000, 0.01)).isEqualTo(2_048);
        assertThat(SplitBlockBloomFilterBuilder.optimalNumBytes(1_000, 0.05)).isEqualTo(1_024);
        assertThat(SplitBlockBloomFilterBuilder.optimalNumBytes(0, 0.01)).isEqualTo(32);
        assertThat(SplitBlockBloomFilterBuilder.optimalNumBytes(Long.MAX_VALUE, 0.01))
                .isEqualTo(SplitBlockBloomFilterBuilder.MAX_BYTES);
    }

    @Test
    void rejectsAProbabilityOutsideTheOpenUnitInterval() {
        assertThatThrownBy(() -> SplitBlockBloomFilterBuilder.optimalNumBytes(1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fpp must be in (0, 1) but was 0.0");
        assertThatThrownBy(() -> SplitBlockBloomFilterBuilder.optimalNumBytes(1, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fpp must be in (0, 1) but was 1.0");
        assertThatThrownBy(() -> SplitBlockBloomFilterBuilder.optimalNumBytes(1, Double.NaN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fpp must be in (0, 1) but was NaN");
    }

    @Test
    void containsEveryInsertedValueAsTheReaderProbesIt() {
        SplitBlockBloomFilterBuilder builder = SplitBlockBloomFilterBuilder.forDistinctValues(10_000, 0.01);
        for (int i = 0; i < 10_000; i++) {
            builder.insert(XxHash64.hash(i));
        }
        ByteBuffer bitset = bitset(builder);
        for (int i = 0; i < 10_000; i++) {
            assertThat(SplitBlockBloomFilter.mightContain(bitset, XxHash64.hash(i))).isTrue();
        }
    }

    @Test
    void foldsAnOversizedFilterDownToTheTargetProbability() {
        // Sized for a hundred times the values it receives, as a chunk sized by its present
        // values is when they repeat.
        SplitBlockBloomFilterBuilder builder = SplitBlockBloomFilterBuilder.forDistinctValues(1_000_000, 0.01);
        int before = builder.numBytes();
        for (int i = 0; i < 10_000; i++) {
            builder.insert(XxHash64.hash(i));
        }
        builder.foldToTargetFpp(0.01);

        assertThat(before).isEqualTo(2 << 20);
        // What sizing for the true count would have given.
        assertThat(builder.numBytes()).isEqualTo(SplitBlockBloomFilterBuilder.optimalNumBytes(10_000, 0.01));
        ByteBuffer bitset = bitset(builder);
        for (int i = 0; i < 10_000; i++) {
            assertThat(SplitBlockBloomFilter.mightContain(bitset, XxHash64.hash(i))).isTrue();
        }
        assertThat(falsePositiveRate(bitset)).isLessThan(0.01);
    }

    @Test
    void foldsAnEmptyFilterToOneBlock() {
        SplitBlockBloomFilterBuilder builder = SplitBlockBloomFilterBuilder.forDistinctValues(100_000, 0.01);
        builder.foldToTargetFpp(0.01);
        assertThat(builder.numBytes()).isEqualTo(32);
        assertThat(builder.toBytes()).containsOnly(0);
    }

    @Test
    void keepsAFilterTheProbabilityNeedsWhole() {
        SplitBlockBloomFilterBuilder builder = SplitBlockBloomFilterBuilder.forDistinctValues(10_000, 0.01);
        for (int i = 0; i < 10_000; i++) {
            builder.insert(XxHash64.hash(i));
        }
        int sized = builder.numBytes();
        builder.foldToTargetFpp(0.01);
        assertThat(builder.numBytes()).isEqualTo(sized);
    }

    @Test
    void storesEachWordLittleEndian() {
        SplitBlockBloomFilterBuilder builder = SplitBlockBloomFilterBuilder.forDistinctValues(0, 0.01);
        // A hash whose low 32 bits are zero sets bit 0 of every word of the one block.
        builder.insert(0L);
        byte[] bytes = builder.toBytes();
        for (int word = 0; word < 8; word++) {
            assertThat(bytes[word * 4]).isEqualTo((byte) 1);
            assertThat(bytes[word * 4 + 1]).isZero();
            assertThat(bytes[word * 4 + 2]).isZero();
            assertThat(bytes[word * 4 + 3]).isZero();
        }
    }

    private static ByteBuffer bitset(SplitBlockBloomFilterBuilder builder) {
        return ByteBuffer.wrap(builder.toBytes()).order(ByteOrder.LITTLE_ENDIAN);
    }

    /// The share of 100,000 values never inserted that the filter passes.
    private static double falsePositiveRate(ByteBuffer bitset) {
        int passed = 0;
        int probes = 100_000;
        for (int i = 0; i < probes; i++) {
            if (SplitBlockBloomFilter.mightContain(bitset, XxHash64.hash(1_000_000 + i))) {
                passed++;
            }
        }
        return passed / (double) probes;
    }
}
