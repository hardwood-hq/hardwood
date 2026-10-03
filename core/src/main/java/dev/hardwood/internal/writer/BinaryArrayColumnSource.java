/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

/// A [BinaryColumnSource] backed by a plain `byte[][]`. The outer array and its element arrays
/// are referenced, not copied, so the caller must not mutate them until the batch has been
/// written.
public final class BinaryArrayColumnSource implements BinaryColumnSource {

    private final byte[][] values;

    public BinaryArrayColumnSource(byte[][] values) {
        this.values = values;
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public int valueBytesAt(int index) {
        byte[] value = values[index];
        return value == null ? 0 : value.length;
    }

    @Override
    public byte[] arrayAt(int index) {
        return values[index];
    }

    @Override
    public int offsetAt(int index) {
        return 0;
    }
}
