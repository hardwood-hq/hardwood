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

/// `value = literal`, or `value != literal` when negated, over a byte-array column whose equality is
/// byte equality, for a literal of at most eight bytes. A row of at most eight bytes is compared as one
/// big-endian `long`, as [ShortValueEquality] compares it against `IN` members; a longer row cannot
/// equal the literal. A short row that ends within eight bytes of the array's end is compared byte by
/// byte.
///
/// Like [BinaryShortInBatchMatcher], this compares every slot, nulls included, and clears the null
/// rows' bits afterwards. Only a byte-exact [Comparison] is accepted.
///
/// A class of its own rather than the one-member case of [BinaryShortInBatchMatcher], so that equality
/// never runs [ShortValueEquality]'s loop over the members. C2 compiles that loop from the member
/// counts it has profiled, and equality seen first would leave every later `IN` with a loop compiled
/// for a single member.
public final class BinaryShortEqBatchMatcher implements BinaryBatchMatcher {

    private final byte[] literal;
    private final int literalLength;
    private final long literalValue;
    private final boolean negated;

    public BinaryShortEqBatchMatcher(byte[] literal, Comparison comparison, boolean negated) {
        if (!supports(comparison, literal)) {
            throw new IllegalArgumentException("Short-value equality needs a byte-exact comparison and a literal of"
                    + " at most eight bytes, got " + comparison + " over a literal of " + literal.length + " bytes");
        }
        this.literal = literal;
        this.literalLength = literal.length;
        this.literalValue = ShortValueEquality.asLong(literal);
        this.negated = negated;
    }

    /// Whether this matcher can decide equality with `literal` under `comparison`.
    public static boolean supports(Comparison comparison, byte[] literal) {
        return comparison.byteExact() && literal.length <= Long.BYTES;
    }

    @Override
    public void test(BatchExchange.Batch batch, long[] outWords) {
        BinaryBatchValues vals = (BinaryBatchValues) batch.values;
        byte[] bytes = vals.bytes;
        int[] offsets = vals.offsets;
        int n = batch.recordCount;
        int activeWords = (n + 63) >>> 6;

        for (int w = 0; w < activeWords; w++) {
            int base = w << 6;
            int rows = Math.min(64, n - base);
            long word = 0L;
            for (int b = 0; b < rows; b++) {
                int i = base + b;
                word |= (matches(bytes, offsets[i], offsets[i + 1]) ? 1L : 0L) << b;
            }
            outWords[w] = word;
        }
        if (negated) {
            ShortValueEquality.invert(outWords, n);
        }
        ShortValueEquality.keepPresent(outWords, batch.validity, n);
    }

    @Override
    public boolean testValue(byte[] bytes, int from, int to) {
        return matches(bytes, from, to) != negated;
    }

    private boolean matches(byte[] bytes, int from, int to) {
        int length = to - from;
        if (length > Long.BYTES) {
            return false;
        }
        if (from <= bytes.length - Long.BYTES) {
            return (length == literalLength) & (ShortValueEquality.prefixAt(bytes, from, length) == literalValue);
        }
        return BinaryComparator.sliceEquals(bytes, from, to, literal);
    }
}
