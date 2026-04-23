/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.row;

import java.util.Iterator;

/// Indexed view of a Variant `ARRAY` value. Elements are heterogeneous Variants
/// — callers inspect each element's [VariantType] and unwrap appropriately.
///
/// ```java
/// PqVariantArray arr = v.asArray();
/// for (PqVariant e : arr) {
///     if (e.type() == VariantType.INT32) {
///         int x = e.asInt();
///     }
/// }
/// ```
public interface PqVariantArray extends Iterable<PqVariant> {

    /// Number of elements in the array.
    int size();

    /// Element at the given zero-based index, decoded lazily.
    ///
    /// @throws IndexOutOfBoundsException if `index` is not in `[0, size())`
    PqVariant get(int index);

    @Override
    default Iterator<PqVariant> iterator() {
        return new Iterator<>() {
            private int next;

            @Override
            public boolean hasNext() {
                return next < size();
            }

            @Override
            public PqVariant next() {
                if (next >= size()) {
                    throw new java.util.NoSuchElementException();
                }
                return get(next++);
            }
        };
    }
}
