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

/// A `LIST` or `MAP` group in the row-oriented layer's plan. Both stage the same thing — one
/// entry-offset per instance of the enclosing scope, plus a null bit when the group is
/// `OPTIONAL` — and differ only in the verbs their builder exposes and the batch setter their
/// offsets end up in.
///
/// `offsets[i]` is the running total of entries before instance `i`, so `offsets[count]` is
/// always the number of entries staged so far and the running total needs no separate field.
abstract class RowRepeatedNode extends RowNode {

    private static final int INITIAL_CAPACITY = 17;

    /// Per-instance nulls of an `OPTIONAL` list or map, or `null` when it is `REQUIRED`.
    private final NullMaskStage nulls;

    private int[] offsets = new int[INITIAL_CAPACITY];

    /// Instances of the enclosing scope staged so far; the offsets array holds `count + 1`
    /// meaningful entries.
    private int count;

    /// Entries appended in the scope currently open.
    private int pending;

    private boolean active;

    RowRepeatedNode(String path, boolean optional) {
        super(path);
        this.nulls = optional ? new NullMaskStage() : null;
    }

    NullMaskStage nulls() {
        return nulls;
    }

    int instanceCount() {
        return count;
    }

    /// Opens this group's scope for one present instance.
    void beginScope() {
        if (active) {
            throw reenteredScope();
        }
        active = true;
        pending = 0;
        if (nulls != null) {
            nulls.append(false);
        }
    }

    /// Closes the scope, recording how many entries it appended.
    void endScope() {
        active = false;
        appendOffset(pending);
    }

    void clearActive() {
        active = false;
        pending = 0;
    }

    /// Counts one appended entry. Called after the entry has been staged, so a rejected value
    /// leaves the offsets untouched.
    final void countEntry() {
        pending++;
    }

    final void ensureActive() {
        if (!active) {
            throw new IllegalStateException("This builder's scope has ended; a builder is only valid "
                    + "inside the lambda it was passed to");
        }
    }

    @Override
    void appendNullInstance() {
        if (nulls == null) {
            throw requiredField();
        }
        nulls.append(true);
        appendOffset(0);
    }

    @Override
    void appendAbsentInstance() {
        if (nulls != null) {
            nulls.append(true);
        }
        appendOffset(0);
    }

    private void appendOffset(int entries) {
        if (count + 2 > offsets.length) {
            offsets = Arrays.copyOf(offsets, offsets.length * 2);
        }
        offsets[count + 1] = offsets[count] + entries;
        count++;
    }

    /// The offsets to hand to the batch, trimmed to the staged instance count.
    final int[] offsets() {
        return Arrays.copyOf(offsets, count + 1);
    }

    final Validity validity() {
        return nulls == null ? null : nulls.validity();
    }

    void truncate(int mark) {
        if (mark > count) {
            throw StagingRollback.shortOf(mark, count);
        }
        count = mark;
        pending = 0;
    }

    void reset() {
        count = 0;
        pending = 0;
        offsets[0] = 0;
    }
}
