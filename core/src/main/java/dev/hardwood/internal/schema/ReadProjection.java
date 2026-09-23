/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;

import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

/// The columns a read exposes and the columns it decodes.
///
/// A filtered read decodes the columns its predicate references whether or not the caller
/// projected them. `payload` is the caller's projection, the only columns a reader's accessors
/// reach. `decoded` is the payload plus the predicate columns it does not cover, which every
/// worker, exchange and fetch plan spans. The payload's columns and top-level fields lead
/// `decoded`, at the indices they hold in `payload`, so a decoded index below
/// [#payloadColumnCount()] names the same column in both. A decoded column past it is
/// **filter-only**: it serves the predicate and nothing else. See
/// `_designs/ROW_READER_AUGMENTED_PROJECTION.md` and `_designs/FILTER_ONLY_COLUMN_SKIP.md`.
///
/// @param payload the columns a reader exposes
/// @param decoded the columns a reader decodes, `payload`'s first
public record ReadProjection(ProjectedSchema payload, ProjectedSchema decoded) {

    public ReadProjection {
        int payloadColumns = payload.getProjectedColumnCount();
        int[] payloadFields = payload.getProjectedFieldIndices();
        int[] decodedFields = decoded.getProjectedFieldIndices();
        if (payloadColumns > decoded.getProjectedColumnCount()
                || payloadFields.length > decodedFields.length
                || !Arrays.equals(payloadFields, 0, payloadFields.length, decodedFields, 0, payloadFields.length)) {
            throw new IllegalArgumentException("The payload projection does not lead the decoded one");
        }
        for (int i = 0; i < payloadColumns; i++) {
            if (payload.toOriginalIndex(i) != decoded.toOriginalIndex(i)) {
                throw new IllegalArgumentException("The payload projection does not lead the decoded one");
            }
        }
    }

    /// A read that decodes exactly what it exposes.
    public static ReadProjection of(ProjectedSchema projected) {
        return new ReadProjection(projected, projected);
    }

    /// Resolves `projection` as the payload and appends the leaves of `predicateColumns` it does
    /// not already cover to form the decoded columns.
    ///
    /// @param schema the file schema
    /// @param projection the columns to expose
    /// @param predicateColumns names of the predicate's leaf columns
    /// @param completeContainers as for [ProjectedSchema#create(FileSchema, ColumnProjection, boolean)]
    public static ReadProjection withPredicateColumns(FileSchema schema, ColumnProjection projection,
            Collection<String> predicateColumns, boolean completeContainers) {
        ProjectedSchema payload = ProjectedSchema.create(schema, projection, completeContainers);
        if (projection.projectsAll() || predicateColumns.isEmpty()) {
            return of(payload);
        }
        LinkedHashSet<String> names = new LinkedHashSet<>(projection.getProjectedColumnNames());
        if (!names.addAll(predicateColumns)) {
            return of(payload);
        }
        ProjectedSchema all = ProjectedSchema.create(schema, ColumnProjection.columns(names.toArray(new String[0])),
                completeContainers);
        if (all.getProjectedColumnCount() == payload.getProjectedColumnCount()) {
            return of(payload);
        }
        return new ReadProjection(payload, ProjectedSchema.leadingThenRest(payload, all));
    }

    /// The number of payload columns; the decoded columns at and past it are filter-only.
    public int payloadColumnCount() {
        return payload.getProjectedColumnCount();
    }

    /// Whether the decoded column at `decodedIndex` serves the predicate alone.
    public boolean isFilterOnly(int decodedIndex) {
        return decodedIndex >= payloadColumnCount();
    }
}
