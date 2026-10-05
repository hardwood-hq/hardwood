/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.util.Arrays;
import java.util.List;

import dev.hardwood.schema.ColumnProjection;

/// The columns a read requests, in request order: the names of a [ColumnProjection], followed by
/// leaf columns requested by index.
///
/// A column the reader already knows by index is requested by that index rather than by its
/// path, since the paths of two fields can join to the same name when a field name contains a
/// dot.
///
/// @param names the requested names, or `null` when every column is requested
/// @param columns the leaf columns requested by index, after the names
public record ColumnRequests(List<String> names, int[] columns) {

    private static final int[] NO_COLUMNS = new int[0];

    /// The requests of `projection`.
    public static ColumnRequests of(ColumnProjection projection) {
        return new ColumnRequests(projection.projectsAll() ? null : projection.getProjectedColumnNames(), NO_COLUMNS);
    }

    /// Requests the leaf columns at `columns`, in that order.
    public static ColumnRequests ofColumns(int... columns) {
        return new ColumnRequests(List.of(), columns);
    }

    /// Whether every column is requested.
    public boolean requestsAll() {
        return names == null;
    }

    /// These requests followed by the leaf columns at `more`.
    public ColumnRequests plusColumns(int[] more) {
        int[] combined = Arrays.copyOf(columns, columns.length + more.length);
        System.arraycopy(more, 0, combined, columns.length, more.length);
        return new ColumnRequests(names, combined);
    }
}
