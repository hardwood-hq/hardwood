/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import dev.hardwood.Experimental;
import dev.hardwood.internal.reader.Dictionary;
import dev.hardwood.internal.reader.LogicalAccessorKind;
import dev.hardwood.schema.ColumnSchema;

/// The dictionary of a `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` or `INT96` column chunk, from
/// [ColumnReader#getBinaryDictionary()]. Its entries never change, and it may be read from any
/// thread. Every batch drawn from the same dictionary returns the same instance, so comparing
/// it with the previous batch's (`!=`) tells when the dictionary changed.
///
/// **This API is [Experimental]:** it may change in future releases without prior
/// deprecation.
@Experimental
public final class BinaryDictionary implements ColumnDictionary {

    private final Dictionary.ByteArrayDictionary dictionary;
    private final ColumnSchema column;
    private final String fileName;

    BinaryDictionary(Dictionary.ByteArrayDictionary dictionary, ColumnSchema column, String fileName) {
        this.dictionary = dictionary;
        this.column = column;
        this.fileName = fileName;
    }

    @Override
    public int size() {
        return dictionary.size();
    }

    /// A copy of entry `entry`.
    ///
    /// @throws IndexOutOfBoundsException if `entry` is not in `[0, size())`
    public byte[] getBinary(int entry) {
        checkEntry(entry);
        return dictionary.entry(entry);
    }

    /// Entry `entry` decoded as UTF-8. The result is cached for later calls, but compare it with
    /// `equals`: calls from several threads may each decode the entry and return distinct, equal
    /// strings. The column has to hold text, as for [ColumnReader#getStrings()].
    ///
    /// @throws IllegalArgumentException if the column does not hold text
    /// @throws IndexOutOfBoundsException if `entry` is not in `[0, size())`
    public String getString(int entry) {
        LogicalAccessorKind.requireText(fileName, column.name(), column.type(), column.logicalType());
        checkEntry(entry);
        return dictionary.internedString(entry);
    }

    /// Whether this object describes `dictionary`, for [ColumnReader] to reuse it across batches.
    boolean wraps(Dictionary.ByteArrayDictionary dictionary) {
        return this.dictionary == dictionary;
    }

    private void checkEntry(int entry) {
        if (entry < 0 || entry >= dictionary.size()) {
            throw new IndexOutOfBoundsException("Dictionary entry " + entry + " out of bounds for "
                    + dictionary.size() + " entries");
        }
    }
}
