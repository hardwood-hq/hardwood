/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import dev.hardwood.row.StructAccessor;

/// Provides row-oriented iteration over a Parquet file.
///
/// A `RowReader` is a stateful, mutable view providing access to the current row
/// in the iterator. The values returned by its accessors change between calls of [#next()].
///
/// Usage example:
/// ```java
/// try (RowReader rowReader = fileReader.rowReader()) {
///     while (rowReader.hasNext()) {
///         rowReader.next();
///         long id = rowReader.getLong("id");
///         PqStruct address = rowReader.getStruct("address");
///         String city = address.getString("city");
///     }
/// }
/// ```
public interface RowReader extends StructAccessor, AutoCloseable {

    /// Check if there are more rows to read.
    ///
    /// @return true if there are more rows available
    boolean hasNext();

    /// Advance to the next row. Must be called before accessing row data.
    ///
    /// @throws java.util.NoSuchElementException if no more rows are available
    void next();

    /// Releases the resources held by this reader. Idempotent: calling it more
    /// than once has no further effect.
    @Override
    void close();
}
