/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.List;
import java.util.function.IntPredicate;

import dev.hardwood.internal.schema.LeafAnnotation;
import dev.hardwood.internal.thrift.FileMetaDataReader.ReadFooter;
import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Whether a column's recorded `min` / `max` are in an order this reader can read.
///
/// Pruning compares a literal against those bounds, which is sound only in the order they were
/// written in. Three shapes arrive where that order is not knowable, none of them produced by
/// this writer:
///
/// - the annotation names no order — parquet-format defines none for `INTERVAL`, `GEOMETRY`,
///   `GEOGRAPHY`, `VARIANT`, `LIST` and `MAP`, and asks that `INTERVAL` record no bounds at all,
///   and a `NULL` column stores no values to order;
/// - the file names an order this build does not recognize, which `parquet.thrift` says to treat
///   as a column whose `min` / `max` are to be ignored;
/// - the reader dropped the column's annotation, either because this build does not recognize
///   it or because the column's physical type cannot carry it. The column reads as its physical
///   type, but its writer recorded the bounds in the order of the annotation, which the
///   physical type's order need not agree with. The "Unsupported Logical Types" section of
///   `LogicalTypes.md` has readers ignore the column order of such a column.
///
/// Bloom filters and dictionaries are unaffected: both test exact stored values, which does not
/// depend on how those values order.
public final class BoundsReadability {

    /// Every column readable, for a caller whose leaves were already checked against their file —
    /// [PageDropPredicates#canDropPage], whose leaves are withheld per file before they reach
    /// it — and for the pruning helpers' own tests.
    public static final BoundsReadability ALL = new BoundsReadability(null);

    /// Per leaf ordinal, or `null` for every column readable.
    private final boolean[] readable;
    /// Per leaf ordinal, whether the discard of the column's bounds has been reported; `null`
    /// where every column is readable and so nothing is discarded.
    private final boolean[] reported;

    private BoundsReadability(boolean[] readable) {
        this.readable = readable;
        this.reported = readable == null ? null : new boolean[readable.length];
    }

    /// Whether the leaf column at `columnIndex` has bounds worth reading.
    ///
    /// @throws IllegalStateException if `columnIndex` is not one of the file's leaf columns, which
    ///         is a wiring error rather than a question with an answer
    public boolean readable(int columnIndex) {
        if (readable == null) {
            return true;
        }
        if (columnIndex < 0 || columnIndex >= readable.length) {
            throw new IllegalStateException("Column " + columnIndex + " is not one of the "
                    + readable.length + " leaf columns of this file");
        }
        return readable[columnIndex];
    }

    /// Whether the discard of the unreadable column at `columnIndex` is yet to be reported:
    /// `true` on the first call per column and `false` on every later one.
    ///
    /// Readability holds for a column throughout the file, so a file pruned on every row group
    /// and page has one thing to report per column, not one per unit. The instance belongs to
    /// one opened file and is shared by every read of it, so each column is reported once for
    /// the file. Row groups are pruned while a read is planned and pages as it reaches each row
    /// group, not necessarily on the same thread, so the claim is synchronized; once a column is
    /// reported, the unsynchronized read answers every later call, and a stale `false` only
    /// falls through to the synchronized check.
    ///
    /// @throws IllegalStateException if every column is readable, so no discard can be reported
    boolean claimReport(int columnIndex) {
        if (reported == null) {
            throw new IllegalStateException("Every column's bounds are readable; there is no discard to report");
        }
        if (reported[columnIndex]) {
            return false;
        }
        synchronized (reported) {
            if (reported[columnIndex]) {
                return false;
            }
            reported[columnIndex] = true;
            return true;
        }
    }

    /// The readability of every leaf of one file, indexed by that file's own leaf ordinals.
    /// Readability is a property of the file that wrote the bounds, so each file of a
    /// multi-file read has its own.
    ///
    /// @param schema the schema built from `footer`
    /// @param footer the file's footer, whose schema elements still carry every annotation
    ///        `schema` dropped
    public static BoundsReadability of(FileSchema schema, ReadFooter footer) {
        boolean[] dropped = new boolean[schema.getColumnCount()];
        int leafOrdinal = 0;
        for (SchemaElement element : footer.metaData().schema()) {
            if (!element.isPrimitive()) {
                continue;
            }
            dropped[leafOrdinal] = footer.logicalTypeUnread(leafOrdinal)
                    || LeafAnnotation.dropFault(element) != null;
            leafOrdinal++;
        }
        return of(schema, footer.metaData().columnOrders(), ordinal -> dropped[ordinal]);
    }

    /// The readability of every leaf of one file, indexed by that file's own leaf ordinals.
    ///
    /// @param schema the file's schema
    /// @param columnOrders the file's decoded `column_orders`, empty where the file omitted them,
    ///        which means the type-defined order throughout
    /// @param annotationDropped whether the reader dropped the annotation the footer gave the
    ///        leaf at an ordinal
    static BoundsReadability of(FileSchema schema, List<ColumnOrder> columnOrders,
            IntPredicate annotationDropped) {
        boolean[] readable = new boolean[schema.getColumnCount()];
        for (int i = 0; i < readable.length; i++) {
            ColumnSchema column = schema.getColumn(i);
            boolean orderRecognized = columnOrders.size() <= i
                    || columnOrders.get(i) != ColumnOrder.UNKNOWN;
            readable[i] = orderRecognized && !annotationDropped.test(i)
                    && namesAnOrder(column.logicalType());
        }
        return new BoundsReadability(readable);
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
            // parquet-format leaves these unordered, and a NULL column stores no values.
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
