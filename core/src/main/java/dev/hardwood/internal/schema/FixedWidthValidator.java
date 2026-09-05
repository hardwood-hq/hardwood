/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.SchemaIncompatibleException;
import dev.hardwood.schema.ColumnSchema;

/// Checks that a `FIXED_LEN_BYTE_ARRAY` column carries the width every decode of its
/// bytes needs.
///
/// `type_length` is optional in `parquet.thrift`, so a footer can declare a fixed-width
/// column without stating how wide its values are. The width sizes the value buffer,
/// spaces the offsets and advances the decoder's read position, so a column missing it
/// cannot be decoded at all — and one declaring a non-positive width decodes to
/// something that is not the column's data.
///
/// The check runs over the columns a read touches, before it starts: see
/// [dev.hardwood.internal.reader.RowGroupIterator#initialize]. Columns that are neither
/// projected nor filtered on are never checked, so a file remains readable through the
/// columns that are well-formed, and the metadata tools keep reporting the schema as the
/// footer states it.
///
/// A `type_length` on a column of any other physical type carries its second
/// `parquet.thrift` meaning — the maximum bit length of a value — and is left alone.
public final class FixedWidthValidator {

    private FixedWidthValidator() {
    }

    /// Validates `column` if it is a `FIXED_LEN_BYTE_ARRAY`, and does nothing otherwise.
    ///
    /// @param fileName the file the column was read from, for the message; may be `null`
    /// @param column the column to check
    /// @throws SchemaIncompatibleException if the column is fixed-width and its declared
    ///         width is absent or not positive
    public static void validate(String fileName, ColumnSchema column) {
        if (column.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            requireWidth(fileName, column);
        }
    }

    /// Returns the byte width of a `FIXED_LEN_BYTE_ARRAY` column.
    ///
    /// @param fileName the file the column was read from, for the message; may be `null`
    /// @param column the fixed-width column whose width is needed
    /// @return the column's positive byte width
    /// @throws SchemaIncompatibleException if the declared width is absent or not positive
    public static int requireWidth(String fileName, ColumnSchema column) {
        Integer width = column.typeLength();
        if (width == null) {
            throw new SchemaIncompatibleException(ExceptionContext.filePrefix(fileName)
                    + "Column '" + column.fieldPath() + "' is a FIXED_LEN_BYTE_ARRAY that declares no type length");
        }
        if (width <= 0) {
            throw new SchemaIncompatibleException(ExceptionContext.filePrefix(fileName)
                    + "Column '" + column.fieldPath() + "' declares a FIXED_LEN_BYTE_ARRAY type length of "
                    + width + ", which must be positive");
        }
        return width;
    }
}
