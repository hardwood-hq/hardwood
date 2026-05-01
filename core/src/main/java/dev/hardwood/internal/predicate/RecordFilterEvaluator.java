/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.Arrays;

import dev.hardwood.reader.FilterPredicate;

/// Evaluates a [ResolvedPredicate] against a row reader's current row.
///
/// Used by [dev.hardwood.internal.reader.FilteredRowReader] for record-level
/// filtering after page decoding, so that only rows matching the predicate
/// are returned to the caller.
public class RecordFilterEvaluator {

    /// Evaluates a predicate against the current row of a [RowReader].
    ///
    /// Navigates nested struct paths automatically using the [FileSchema] field paths,
    /// so predicates on nested leaf columns (e.g. `address.zip`) are supported.
    ///
    /// @param predicate  the resolved predicate to evaluate
    /// @param reader     the row reader positioned on the current row
    /// @param schema     the file schema (used to look up field paths for nested navigation)
    /// @return true if the current row matches the predicate
    public static boolean matchesRow(ResolvedPredicate predicate,
            dev.hardwood.row.StructAccessor reader, dev.hardwood.schema.FileSchema schema) {
        return switch (predicate) {
            case ResolvedPredicate.IntPredicate p -> {
                dev.hardwood.row.StructAccessor acc = resolve(reader, schema, p.columnIndex());
                String name = schema.getColumn(p.columnIndex()).fieldPath().leafName();
                if (acc == null || acc.isNull(name)) yield false;
                yield compareInt(p.op(), acc.getInt(name), p.value());
            }
            case ResolvedPredicate.LongPredicate p -> {
                dev.hardwood.row.StructAccessor acc = resolve(reader, schema, p.columnIndex());
                String name = schema.getColumn(p.columnIndex()).fieldPath().leafName();
                if (acc == null || acc.isNull(name)) yield false;
                yield compareLong(p.op(), acc.getLong(name), p.value());
            }
            case ResolvedPredicate.FloatPredicate p -> {
                dev.hardwood.row.StructAccessor acc = resolve(reader, schema, p.columnIndex());
                String name = schema.getColumn(p.columnIndex()).fieldPath().leafName();
                if (acc == null || acc.isNull(name)) yield false;
                yield compareFloat(p.op(), acc.getFloat(name), p.value());
            }
            case ResolvedPredicate.DoublePredicate p -> {
                dev.hardwood.row.StructAccessor acc = resolve(reader, schema, p.columnIndex());
                String name = schema.getColumn(p.columnIndex()).fieldPath().leafName();
                if (acc == null || acc.isNull(name)) yield false;
                yield compareDouble(p.op(), acc.getDouble(name), p.value());
            }
            case ResolvedPredicate.BooleanPredicate p -> {
                dev.hardwood.row.StructAccessor acc = resolve(reader, schema, p.columnIndex());
                String name = schema.getColumn(p.columnIndex()).fieldPath().leafName();
                if (acc == null || acc.isNull(name)) yield false;
                boolean val = acc.getBoolean(name);
                yield switch (p.op()) {
                    case EQ -> val == p.value();
                    case NOT_EQ -> val != p.value();
                    default -> true;
                };
            }
            case ResolvedPredicate.BinaryPredicate p -> {
                dev.hardwood.row.StructAccessor acc = resolve(reader, schema, p.columnIndex());
                String name = schema.getColumn(p.columnIndex()).fieldPath().leafName();
                if (acc == null || acc.isNull(name)) yield false;
                byte[] val = acc.getBinary(name);
                int cmp = p.signed()
                        ? BinaryComparator.compareSigned(val, p.value())
                        : BinaryComparator.compareUnsigned(val, p.value());
                yield switch (p.op()) {
                    case EQ -> cmp == 0;
                    case NOT_EQ -> cmp != 0;
                    case LT -> cmp < 0;
                    case LT_EQ -> cmp <= 0;
                    case GT -> cmp > 0;
                    case GT_EQ -> cmp >= 0;
                };
            }
            case ResolvedPredicate.IntInPredicate p -> {
                dev.hardwood.row.StructAccessor acc = resolve(reader, schema, p.columnIndex());
                String name = schema.getColumn(p.columnIndex()).fieldPath().leafName();
                if (acc == null || acc.isNull(name)) yield false;
                int val = acc.getInt(name);
                boolean found = false;
                for (int v : p.values()) {
                    if (val == v) { found = true; break; }
                }
                yield found;
            }
            case ResolvedPredicate.LongInPredicate p -> {
                dev.hardwood.row.StructAccessor acc = resolve(reader, schema, p.columnIndex());
                String name = schema.getColumn(p.columnIndex()).fieldPath().leafName();
                if (acc == null || acc.isNull(name)) yield false;
                long val = acc.getLong(name);
                boolean found = false;
                for (long v : p.values()) {
                    if (val == v) { found = true; break; }
                }
                yield found;
            }
            case ResolvedPredicate.BinaryInPredicate p -> {
                dev.hardwood.row.StructAccessor acc = resolve(reader, schema, p.columnIndex());
                String name = schema.getColumn(p.columnIndex()).fieldPath().leafName();
                if (acc == null || acc.isNull(name)) yield false;
                byte[] val = acc.getBinary(name);
                boolean found = false;
                for (byte[] v : p.values()) {
                    if (Arrays.equals(val, v)) { found = true; break; }
                }
                yield found;
            }
            case ResolvedPredicate.IsNullPredicate p -> {
                dev.hardwood.row.StructAccessor acc = resolve(reader, schema, p.columnIndex());
                String name = schema.getColumn(p.columnIndex()).fieldPath().leafName();
                yield acc == null || acc.isNull(name);
            }
            case ResolvedPredicate.IsNotNullPredicate p -> {
                dev.hardwood.row.StructAccessor acc = resolve(reader, schema, p.columnIndex());
                String name = schema.getColumn(p.columnIndex()).fieldPath().leafName();
                yield acc != null && !acc.isNull(name);
            }
            case ResolvedPredicate.And and -> {
                for (ResolvedPredicate child : and.children()) {
                    if (!matchesRow(child, reader, schema)) {
                        yield false;
                    }
                }
                yield true;
            }
            case ResolvedPredicate.Or or -> {
                for (ResolvedPredicate child : or.children()) {
                    if (matchesRow(child, reader, schema)) {
                        yield true;
                    }
                }
                yield false;
            }
            // Spatial intersects is a coarse-grained, bbox-only filter applied at the row group
            // and page level. Per-row WKB decoding is left to the caller, so every surviving row passes here.
            case ResolvedPredicate.GeospatialPredicate p -> true;
        };
    }

    /// Navigates the struct path from the reader to the parent accessor of the leaf column.
    /// For top-level columns, returns the reader itself.
    /// For nested columns (e.g. `address.zip`), navigates through intermediate structs.
    /// Returns null if any intermediate struct is null.
    private static dev.hardwood.row.StructAccessor resolve(dev.hardwood.row.StructAccessor reader,
            dev.hardwood.schema.FileSchema schema, int columnIndex) {
        java.util.List<String> elements = schema.getColumn(columnIndex).fieldPath().elements();
        dev.hardwood.row.StructAccessor current = reader;
        // Navigate all path elements except the last (the leaf name)
        for (int i = 0; i < elements.size() - 1; i++) {
            String segment = elements.get(i);
            if (current.isNull(segment)) {
                return null;
            }
            current = current.getStruct(segment);
        }
        return current;
    }

    private static boolean compareInt(FilterPredicate.Operator op, int recordValue, int predicateValue) {
        return switch (op) {
            case EQ -> recordValue == predicateValue;
            case NOT_EQ -> recordValue != predicateValue;
            case LT -> recordValue < predicateValue;
            case LT_EQ -> recordValue <= predicateValue;
            case GT -> recordValue > predicateValue;
            case GT_EQ -> recordValue >= predicateValue;
        };
    }

    private static boolean compareLong(FilterPredicate.Operator op, long recordValue, long predicateValue) {
        return switch (op) {
            case EQ -> recordValue == predicateValue;
            case NOT_EQ -> recordValue != predicateValue;
            case LT -> recordValue < predicateValue;
            case LT_EQ -> recordValue <= predicateValue;
            case GT -> recordValue > predicateValue;
            case GT_EQ -> recordValue >= predicateValue;
        };
    }

    private static boolean compareFloat(FilterPredicate.Operator op, float recordValue, float predicateValue) {
        int cmp = Float.compare(recordValue, predicateValue);
        return switch (op) {
            case EQ -> cmp == 0;
            case NOT_EQ -> cmp != 0;
            case LT -> cmp < 0;
            case LT_EQ -> cmp <= 0;
            case GT -> cmp > 0;
            case GT_EQ -> cmp >= 0;
        };
    }

    private static boolean compareDouble(FilterPredicate.Operator op, double recordValue, double predicateValue) {
        int cmp = Double.compare(recordValue, predicateValue);
        return switch (op) {
            case EQ -> cmp == 0;
            case NOT_EQ -> cmp != 0;
            case LT -> cmp < 0;
            case LT_EQ -> cmp <= 0;
            case GT -> cmp > 0;
            case GT_EQ -> cmp >= 0;
        };
    }
}
