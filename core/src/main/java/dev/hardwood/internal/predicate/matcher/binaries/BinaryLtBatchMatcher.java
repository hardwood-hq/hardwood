/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.matcher.binaries;

import dev.hardwood.internal.predicate.BinaryBatchMatcher;
import dev.hardwood.internal.predicate.BinaryComparator;
import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate.Comparison;
import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.internal.reader.BinaryBatchValues;

/// `value < literal` over a byte-array column, in the order the column's values sort in.
public final class BinaryLtBatchMatcher implements BinaryBatchMatcher {

    private final byte[] literal;
    private final boolean signed;

    public BinaryLtBatchMatcher(byte[] literal, Comparison comparison) {
        this.literal = literal;
        this.signed = BinaryComparator.signedSliceOrder(comparison);
    }

    @Override
    public void test(BatchExchange.Batch batch, long[] outWords) {
        BinaryBatchValues vals = (BinaryBatchValues) batch.values;
        byte[] bytes = vals.bytes;
        int[] offsets = vals.offsets;
        long[] validity = batch.validity;
        int n = batch.recordCount;
        int activeWords = (n + 63) >>> 6;

        for (int w = 0; w < activeWords; w++) {
            int base = w << 6;
            int rows = Math.min(64, n - base);
            long present = validity != null ? validity[w] : -1L;
            long word = 0L;
            for (int b = 0; b < rows; b++) {
                int i = base + b;
                if ((present & (1L << b)) != 0L
                        && BinaryComparator.compare(bytes, offsets[i], offsets[i + 1], literal, signed) < 0) {
                    word |= 1L << b;
                }
            }
            outWords[w] = word;
        }
    }
}
