/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

/// A [BooleanColumnSource] backed by a plain `boolean[]`. The array is referenced, not
/// copied, so the caller must not mutate it until the batch has been written.
public final class BooleanArrayColumnSource implements BooleanColumnSource {

    private final boolean[] values;

    public BooleanArrayColumnSource(boolean[] values) {
        this.values = values;
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public void copyInto(int srcPos, boolean[] dest, int destPos, int length) {
        System.arraycopy(values, srcPos, dest, destPos, length);
    }
}
