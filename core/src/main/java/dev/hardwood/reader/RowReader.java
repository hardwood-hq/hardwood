/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.FlatRowReader;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.NestedRowReader;
import dev.hardwood.internal.reader.RowGroupIterator;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

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

    /// Creates a [RowReader] for the given pipeline components.
    ///
    /// Selects [dev.hardwood.internal.reader.FlatRowReader] for flat schemas and
    /// [dev.hardwood.internal.reader.NestedRowReader] for nested schemas.
    /// Wraps with [dev.hardwood.internal.reader.FilteredRowReader] when a filter is present.
    ///
    /// @param rowGroupIterator initialized iterator over row groups
    /// @param schema file schema
    /// @param projectedSchema column projection
    /// @param context hardwood context
    /// @param filter resolved predicate, or `null` for no filtering
    /// @param maxRows maximum rows (0 = unlimited)
    static RowReader create(RowGroupIterator rowGroupIterator,
                            FileSchema schema,
                            ProjectedSchema projectedSchema,
                            HardwoodContextImpl context,
                            ResolvedPredicate filter,
                            long maxRows) {
        if (schema.isFlatSchema()) {
            return FlatRowReader.create(rowGroupIterator, schema, projectedSchema, context, filter, maxRows);
        }
        else {
            return NestedRowReader.create(rowGroupIterator, schema, projectedSchema, context, filter, maxRows);
        }
    }

    /// Check if there are more rows to read.
    ///
    /// @return true if there are more rows available
    boolean hasNext();

    /// Advance to the next row. Must be called before accessing row data.
    ///
    /// @throws java.util.NoSuchElementException if no more rows are available
    void next();

    @Override
    void close();
}
