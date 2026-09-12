/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.List;

import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Whether a column's recorded `min` / `max` are in an order this reader can read.
///
/// Pruning compares a literal against those bounds, which is sound only in the order they were
/// written in. Two shapes arrive where that order is not knowable, neither of them produced by
/// this writer:
///
/// - the annotation names no order — parquet-format defines none for `INTERVAL`, `GEOMETRY`,
///   `GEOGRAPHY`, `VARIANT`, `UNKNOWN`, `LIST` and `MAP`, and asks that `INTERVAL` record no
///   bounds at all;
/// - the file names an order this build does not recognize, which `parquet.thrift` says to treat
///   as a column whose `min` / `max` are to be ignored;
/// - the column is a legacy `INT96` timestamp. Its values are compared as the instants they
///   encode, and no order a writer records bounds in is that order on every value:
///   `parquet.thrift` says to ignore `INT96` bounds under the type-defined order, parquet-java
///   writes them in the byte order of a big-endian integer, and `Int96TimestampOrder` compares
///   the day before the nanoseconds of the day, which the format does not bound by one day.
///
/// Bloom filters and dictionaries are unaffected: both test exact stored values, which does not
/// depend on how those values order.
@FunctionalInterface
public interface BoundsReadability {

    /// Every column readable, for a caller whose leaves were already checked against their file —
    /// [PageDropPredicates#canDropPage], whose leaves are withheld per file before they reach
    /// it — and for the pruning helpers' own tests.
    BoundsReadability ALL = columnIndex -> true;

    /// Whether the leaf column at `columnIndex` has bounds worth reading.
    boolean readable(int columnIndex);

    /// The readability of every leaf of one file, indexed by that file's own leaf ordinals.
    /// Readability is a property of the file that wrote the bounds, so each file of a
    /// multi-file read has its own.
    ///
    /// Asking about an ordinal outside `schema` is a wiring error, and throws
    /// [IllegalStateException] rather than answering either way.
    ///
    /// @param schema the file's schema
    /// @param columnOrders the file's decoded `column_orders`, empty where the file omitted them,
    ///        which means the type-defined order throughout
    static BoundsReadability of(FileSchema schema, List<ColumnOrder> columnOrders) {
        boolean[] readable = new boolean[schema.getColumnCount()];
        for (int i = 0; i < readable.length; i++) {
            ColumnSchema column = schema.getColumn(i);
            boolean orderRecognized = columnOrders.size() <= i
                    || columnOrders.get(i) != ColumnOrder.UNKNOWN;
            readable[i] = orderRecognized && column.type() != PhysicalType.INT96
                    && namesAnOrder(column.logicalType());
        }
        return columnIndex -> {
            if (columnIndex < 0 || columnIndex >= readable.length) {
                throw new IllegalStateException("Column " + columnIndex + " is not one of the "
                        + readable.length + " leaf columns of this file");
            }
            return readable[columnIndex];
        };
    }

    /// Whether the annotation names an order for the values beneath it.
    ///
    /// Two callers ask: this one, to decide whether bounds already recorded can be trusted, and
    /// [FilterPredicateResolver], to decide whether a column takes `lt`, `ltEq`, `gt` and `gtEq`
    /// at all. Both questions are the one the format answers, so they share an answer and cannot
    /// drift apart.
    ///
    /// The switch is exhaustive rather than a list of the types without one, so an annotation
    /// added later has to say which side it falls on. It mirrors
    /// `StatisticsOrder#supportsBounds` on the write side without delegating to it: that asks
    /// whether to record bounds, this whether the values themselves have an order.
    static boolean namesAnOrder(LogicalType logicalType) {
        if (logicalType == null) {
            return true; // the physical type's own order
        }
        return switch (logicalType) {
            case LogicalType.StringType ignored -> true;
            case LogicalType.EnumType ignored -> true;
            case LogicalType.JsonType ignored -> true;
            case LogicalType.BsonType ignored -> true;
            case LogicalType.UuidType ignored -> true;
            case LogicalType.DateType ignored -> true;
            case LogicalType.TimeType ignored -> true;
            case LogicalType.TimestampType ignored -> true;
            case LogicalType.IntType ignored -> true;
            case LogicalType.DecimalType ignored -> true;
            case LogicalType.Float16Type ignored -> true;
            // parquet-format leaves these unordered.
            case LogicalType.IntervalType ignored -> false;
            case LogicalType.NullType ignored -> false;
            case LogicalType.VariantType ignored -> false;
            case LogicalType.GeometryType ignored -> false;
            case LogicalType.GeographyType ignored -> false;
            case LogicalType.ListType ignored -> false;
            case LogicalType.MapType ignored -> false;
        };
    }
}
