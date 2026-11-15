/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import dev.hardwood.Experimental;

/// The dictionary of an `INT32` column chunk, from [ColumnReader#getIntDictionary()]. Its entries
/// never change, and it may be read from any thread. Every batch drawn from the same dictionary
/// returns the same instance, so comparing it with the previous batch's (`!=`) tells when the
/// dictionary changed.
///
/// **This API is [Experimental]:** it may change in future releases without prior
/// deprecation.
@Experimental
public final class IntDictionary implements ColumnDictionary {

    private final int[] values;

    IntDictionary(int[] values) {
        this.values = values;
    }

    @Override
    public int size() {
        return values.length;
    }

    /// Entry `entry`.
    ///
    /// @throws IndexOutOfBoundsException if `entry` is not in `[0, size())`
    public int getInt(int entry) {
        if (entry < 0 || entry >= values.length) {
            throw new IndexOutOfBoundsException("Dictionary entry " + entry + " out of bounds for "
                    + values.length + " entries");
        }
        return values[entry];
    }
}
