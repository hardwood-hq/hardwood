/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.matcher.binaries;

import dev.hardwood.internal.predicate.BinaryBatchMatcher;
import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate.Comparison;
import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.internal.reader.BinaryBatchValues;

/// `value IN (members)`, or `value NOT IN (members)` when negated, over a byte-array column whose
/// equality is byte equality, where at least one member is at most eight bytes long. Rows are
/// decided through [ShortValueEquality]. Equality and inequality against one literal are the
/// one-member case.
///
/// Unlike the byte-wise matchers, this one compares every slot, nulls included, and clears the null
/// rows' bits afterwards.
///
/// Members must be spelled as the column holds them: for a `FIXED_LEN_BYTE_ARRAY` decimal that means
/// padded to the column width, which the resolver does. Only a byte-exact [Comparison] is accepted,
/// since a value with another spelling would otherwise be missed.
///
/// A class of its own rather than a branch in the byte-wise matchers, because C2 profiles a method
/// per class, not per instance: calls that return early here would make the byte-wise matchers'
/// per-row comparison look cold, and C2 inlines a call site it counts as cold only up to
/// `MaxInlineSize` bytes.
public final class BinaryShortInBatchMatcher implements BinaryBatchMatcher {

    private final boolean negated;
    private final ShortValueEquality equality;

    public BinaryShortInBatchMatcher(byte[][] members, Comparison comparison, boolean negated) {
        if (!supports(comparison, members)) {
            throw new IllegalArgumentException("Short-value equality needs a byte-exact comparison and a member of"
                    + " at most eight bytes, got " + comparison + " over " + members.length + " members");
        }
        this.negated = negated;
        this.equality = ShortValueEquality.of(members);
    }

    /// Whether this matcher can decide `members` under `comparison`.
    public static boolean supports(Comparison comparison, byte[]... members) {
        return comparison.byteExact() && ShortValueEquality.hasShortMember(members);
    }

    @Override
    public void test(BatchExchange.Batch batch, long[] outWords) {
        int n = batch.recordCount;
        equality.test((BinaryBatchValues) batch.values, n, outWords);
        if (negated) {
            ShortValueEquality.invert(outWords, n);
        }
        ShortValueEquality.keepPresent(outWords, batch.validity, n);
    }

    @Override
    public boolean testValue(byte[] bytes, int from, int to) {
        return equality.testValue(bytes, from, to) != negated;
    }
}
