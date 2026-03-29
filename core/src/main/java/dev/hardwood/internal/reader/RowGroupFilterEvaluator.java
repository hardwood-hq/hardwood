/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.List;

import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.FilterPredicate.And;
import dev.hardwood.reader.FilterPredicate.DateColumnPredicate;
import dev.hardwood.reader.FilterPredicate.DecimalColumnPredicate;
import dev.hardwood.reader.FilterPredicate.InstantColumnPredicate;
import dev.hardwood.reader.FilterPredicate.Not;
import dev.hardwood.reader.FilterPredicate.Or;
import dev.hardwood.reader.FilterPredicate.TimeColumnPredicate;
import dev.hardwood.schema.FileSchema;

/// Evaluates filter predicates against row group statistics to determine
/// whether a row group can be skipped.
///
/// Uses a conservative approach: if statistics are absent for a column,
/// the row group is never dropped (it may contain matching rows).
public class RowGroupFilterEvaluator {

    /// Determines whether a row group can be skipped based on the given filter predicate.
    ///
    /// @param predicate the filter predicate to evaluate
    /// @param rowGroup the row group to check
    /// @param schema the file schema
    /// @return `true` if the row group can be safely skipped (no matching rows),
    ///         `false` if it may contain matching rows
    public static boolean canDropRowGroup(FilterPredicate predicate, RowGroup rowGroup, FileSchema schema) {
        return switch (predicate) {
            case DateColumnPredicate p -> throw FilterPredicateResolver.unresolvedPredicate(p);
            case InstantColumnPredicate p -> throw FilterPredicateResolver.unresolvedPredicate(p);
            case TimeColumnPredicate p -> throw FilterPredicateResolver.unresolvedPredicate(p);
            case DecimalColumnPredicate p -> throw FilterPredicateResolver.unresolvedPredicate(p);
            case And a -> {
                for (FilterPredicate f : a.filters()) {
                    if (canDropRowGroup(f, rowGroup, schema)) {
                        yield true;
                    }
                }
                yield false;
            }
            case Or o -> {
                for (FilterPredicate f : o.filters()) {
                    if (!canDropRowGroup(f, rowGroup, schema)) {
                        yield false;
                    }
                }
                yield true;
            }
            case Not ignored -> false;
            default -> {
                String columnName = StatisticsFilterSupport.columnOf(predicate);
                Statistics stats = findStatistics(columnName,
                        StatisticsFilterSupport.expectedPhysicalType(predicate), rowGroup, schema);
                yield stats != null && StatisticsFilterSupport.canDropLeaf(predicate, MinMaxStats.of(stats));
            }
        };
    }

    private static Statistics findStatistics(String columnName, PhysicalType expectedType,
            RowGroup rowGroup, FileSchema schema) {
        int columnIndex = resolveColumnIndex(columnName, rowGroup, schema);
        if (columnIndex < 0) {
            return null;
        }
        ColumnChunk chunk = rowGroup.columns().get(columnIndex);
        PhysicalType actualType = chunk.metaData().type();
        if (actualType != expectedType
                && !StatisticsFilterSupport.isBinaryCompatible(actualType, expectedType)) {
            throw new IllegalArgumentException(
                    "Column '" + columnName + "' has physical type " + actualType
                            + "; given filter predicate type " + expectedType + " is incompatible");
        }
        return chunk.metaData().statistics();
    }

    /// Resolves a column name to its index in the row group, trying exact leaf-name
    /// lookup first, then falling back to path-based matching for nested/repeated columns.
    ///
    /// @return the column index, or -1 if not found
    static int resolveColumnIndex(String columnName, RowGroup rowGroup, FileSchema schema) {
        int columnIndex = findColumnIndex(columnName, schema);
        if (columnIndex < 0) {
            columnIndex = findColumnIndexByPath(columnName, rowGroup);
        }
        if (columnIndex < 0 || columnIndex >= rowGroup.columns().size()) {
            return -1;
        }
        return columnIndex;
    }

    private static int findColumnIndex(String columnName, FileSchema schema) {
        try {
            return schema.getColumn(columnName).columnIndex();
        }
        catch (IllegalArgumentException e) {
            return -1;
        }
    }

    private static int findColumnIndexByPath(String columnName, RowGroup rowGroup) {
        List<ColumnChunk> columns = rowGroup.columns();
        for (int i = 0; i < columns.size(); i++) {
            var path = columns.get(i).metaData().pathInSchema();
            if (path.isEmpty()) {
                continue;
            }
            if (path.matchesDottedName(columnName)) {
                return i;
            }
            // Match top-level name for repeated columns (e.g. "scores" matches ["scores", "list", "element"])
            if (path.topLevelName().equals(columnName)) {
                return i;
            }
        }
        return -1;
    }

}
