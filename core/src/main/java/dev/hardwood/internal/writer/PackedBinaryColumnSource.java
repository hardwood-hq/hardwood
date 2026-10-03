/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

/// A [BinaryColumnSource] over one packed buffer: value `i` is
/// `values[offsets[i], offsets[i + 1])`, the shape `ColumnReader.getBinaryValues()` and
/// `getBinaryOffsets()` return. Both arrays are referenced, not copied, so the caller must not
/// mutate them until the batch has been written.
public final class PackedBinaryColumnSource implements BinaryColumnSource {

    private final byte[] values;
    private final int[] offsets;

    public PackedBinaryColumnSource(byte[] values, int[] offsets) {
        this.values = values;
        this.offsets = offsets;
    }

    @Override
    public int size() {
        return offsets.length - 1;
    }

    @Override
    public int valueBytesAt(int index) {
        return offsets[index + 1] - offsets[index];
    }

    @Override
    public byte[] arrayAt(int index) {
        return values;
    }

    @Override
    public int offsetAt(int index) {
        return offsets[index];
    }
}
