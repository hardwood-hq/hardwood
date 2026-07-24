/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

/// A [DoubleColumnSource] backed by a plain `double[]`. The array is referenced, not copied,
/// so the caller must not mutate it until the batch has been written.
public final class DoubleArrayColumnSource implements DoubleColumnSource {

    private final double[] values;

    public DoubleArrayColumnSource(double[] values) {
        this.values = values;
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public void copyInto(int srcPos, double[] dest, int destPos, int length) {
        System.arraycopy(values, srcPos, dest, destPos, length);
    }
}
