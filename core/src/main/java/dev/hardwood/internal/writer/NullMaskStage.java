/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.util.Arrays;

import dev.hardwood.Validity;

/// Accumulates the per-instance nulls of one `OPTIONAL` group — a struct's "this instance is
/// absent" bit, or a list's "this list is absent" bit — while the row-oriented layer stages a
/// batch.
///
/// The mask is reused across batches. [#truncate] and [#reset()] clear what they drop, so the
/// entries beyond [#count()] are always `false` and it can be handed to [Validity#ofNulls]
/// untrimmed.
final class NullMaskStage {

    private static final int INITIAL_CAPACITY = 16;

    private boolean[] nulls = new boolean[INITIAL_CAPACITY];
    private int count;
    private boolean hasNulls;

    void append(boolean isNull) {
        if (count == nulls.length) {
            nulls = Arrays.copyOf(nulls, nulls.length * 2);
        }
        nulls[count++] = isNull;
        hasNulls |= isNull;
    }

    int count() {
        return count;
    }

    void truncate(int mark) {
        if (mark > count) {
            throw StagingRollback.shortOf(mark, count);
        }
        for (int i = mark; i < count; i++) {
            nulls[i] = false;
        }
        count = mark;
        // `hasNulls` is not recomputed: a spurious `true` costs an all-false mask, never a
        // wrong bit, and rescanning on every rolled-back record would not pay.
    }

    void reset() {
        // Only an entry below the count can be set, and only while `hasNulls` is.
        if (hasNulls) {
            Arrays.fill(nulls, 0, count, false);
        }
        count = 0;
        hasNulls = false;
    }

    /// The validity to hand to the batch, or `null` when every instance is present — which
    /// the columnar API takes as the all-present form.
    Validity validity() {
        return hasNulls ? Validity.ofNulls(nulls) : null;
    }
}
