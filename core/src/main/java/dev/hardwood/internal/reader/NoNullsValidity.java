/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.reader.Validity;

/// [Validity] for the case where every item at a scope is non-null in the
/// current batch. Stateless; use the [#INSTANCE] singleton (exposed to
/// users as [Validity#NO_NULLS]) so identity comparison is stable.
public final class NoNullsValidity implements Validity {

    /// Identity-stable singleton. Surfaced publicly as [Validity#NO_NULLS].
    public static final NoNullsValidity INSTANCE = new NoNullsValidity();

    private NoNullsValidity() {
    }

    @Override
    public boolean hasNulls() {
        return false;
    }

    @Override
    public boolean isNull(int i) {
        return false;
    }

    @Override
    public boolean isNotNull(int i) {
        return true;
    }

    @Override
    public int nullCount(int count) {
        return 0;
    }

    @Override
    public int nextNull(int from, int count) {
        return -1;
    }

    @Override
    public int nextNotNull(int from, int count) {
        return from < count ? from : -1;
    }

    @Override
    public long[] words() {
        return null;
    }
}
