/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

/// An [IntColumnSource] backed by a plain `int[]`. The array is referenced, not copied,
/// so the caller must not mutate it until the batch has been written.
public final class IntArrayColumnSource implements IntColumnSource {

    private final int[] values;

    public IntArrayColumnSource(int[] values) {
        this.values = values;
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public void copyInto(int srcPos, int[] dest, int destPos, int length) {
        System.arraycopy(values, srcPos, dest, destPos, length);
    }
}
