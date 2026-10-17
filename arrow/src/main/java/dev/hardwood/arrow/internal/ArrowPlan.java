/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.arrow.internal;

import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import dev.hardwood.schema.ColumnSchema;

/// The decode plan produced by [ArrowSchemaConverter#plan]: the Arrow schema and, in the
/// schema's field order, how each field is filled from the reader that serves it.
///
/// @param schema the converted schema, one field per entry
/// @param entries one entry per output field, in field order
public record ArrowPlan(Schema schema, List<Entry> entries) {

    /// One output field. Its index among the entries is its position among the requested
    /// columns, the index [dev.hardwood.reader.ColumnReaders#getColumnReader(int)] serves it
    /// at; a column requested more than once has one entry per position.
    ///
    /// @param source the Parquet column the field is read from
    /// @param field the Arrow field
    /// @param kind how the column's batch values reach the field's buffers
    public record Entry(ColumnSchema source, Field field, ScalarKind kind) {
    }
}
