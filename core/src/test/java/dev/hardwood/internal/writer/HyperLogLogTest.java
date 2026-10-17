/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import dev.hardwood.internal.bloomfilter.XxHash64;

import static org.assertj.core.api.Assertions.assertThat;

class HyperLogLogTest {

    /// Each shape is offered `values` values drawn from `distinct`, in sequence and then in a
    /// scattered order of the same set, and the estimate holds within 3%, about four standard
    /// errors, both times.
    @ParameterizedTest
    @CsvSource({
            "100, 100",
            "4096, 3400",
            "16384, 10000",
            "24000, 24000",

            "131072, 10000",
            "131072, 70000",
            "1000000, 1000000" })
    void estimatesDistinctValuesWhateverTheirOrder(int values, int distinct) {
        HyperLogLog sequential = new HyperLogLog();
        HyperLogLog scattered = new HyperLogLog();
        for (int i = 0; i < values; i++) {
            sequential.offer(XxHash64.hash((long) (i % distinct)));
            scattered.offer(XxHash64.hash(i * 7_919L % distinct));
        }
        long tolerance = Math.max(1, distinct * 3L / 100);
        assertThat(sequential.estimatedDistinct()).isBetween(distinct - tolerance, distinct + tolerance);
        assertThat(scattered.estimatedDistinct()).isBetween(distinct - tolerance, distinct + tolerance);
    }

    /// Around 41,000 distinct values, two and a half times the registers, a sketch that switches
    /// from counting empty registers to the raw estimate reads about 2.5% high, more than the
    /// margin the recovery decision gives a dictionary; this estimator holds within 1% there.
    @ParameterizedTest
    @CsvSource({ "41000, 41000", "45000, 45000", "56000, 41800" })
    void holdsWhereEmptyRegistersRunOut(int values, int distinct) {
        HyperLogLog sketch = new HyperLogLog();
        for (int i = 0; i < values; i++) {
            sketch.offer(XxHash64.hash((long) (i % distinct)));
        }
        assertThat(sketch.estimatedDistinct()).isBetween(distinct * 99L / 100, distinct * 101L / 100);
    }

    /// Values repeat only near where they first appear, as in a spatially ordered column.
    @ParameterizedTest
    @CsvSource({ "120000, 500" })
    void estimatesLocalRepeats(int values, int window) {
        HyperLogLog sketch = new HyperLogLog();
        for (int i = 0; i < values; i++) {
            // Each value appears twice, the second time within the next `window` positions.
            int block = i / (2 * window);
            sketch.offer(XxHash64.hash((long) (block * window + i % window)));
        }
        long distinct = values / 2;
        assertThat(sketch.estimatedDistinct()).isBetween(distinct * 97 / 100, distinct * 103 / 100);
    }
}
