/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.variant;

import dev.hardwood.internal.variant.VariantValueDecoder.ArrayLayout;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.PqVariantArray;

/// [PqVariantArray] implementation wrapping a parsed [ArrayLayout]. Elements are
/// produced lazily on [#get(int)] — no per-element allocation until requested.
final class PqVariantArrayImpl implements PqVariantArray {

    private final VariantMetadata metadata;
    private final byte[] valueBuf;
    private final ArrayLayout layout;

    PqVariantArrayImpl(VariantMetadata metadata, byte[] valueBuf, int arrayHeaderOffset) {
        this.metadata = metadata;
        this.valueBuf = valueBuf;
        this.layout = VariantValueDecoder.parseArray(valueBuf, arrayHeaderOffset);
    }

    @Override
    public int size() {
        return layout.numElements();
    }

    @Override
    public PqVariant get(int index) {
        if (index < 0 || index >= layout.numElements()) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds for size " + layout.numElements());
        }
        int off = VariantValueDecoder.arrayElementOffset(valueBuf, layout, index);
        return new PqVariantImpl(metadata, valueBuf, off);
    }
}
