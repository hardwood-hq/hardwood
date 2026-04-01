/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// Callback for lazily obtaining a [PageScanner] for the next row group within a file.
///
/// This is the row-group analogue of [FileManager] for cross-file transitions.
/// [PageCursor] calls this when pages from the current row group are exhausted,
/// obtaining a scanner that yields pages incrementally from the next row group.
@FunctionalInterface
public interface RowGroupPageSource {

    /// Creates a [PageScanner] for the given row group and projected column.
    /// The scanner yields pages on demand via [PageScanner#hasNextPage()] / [PageScanner#nextPage()],
    /// enabling lazy column chunk fetching on remote backends.
    ///
    /// @param rowGroupIndex the row group index within the file
    /// @param projectedColumnIndex the projected column index
    /// @return a PageScanner for the column in that row group, or `null` if no more row groups
    PageScanner getScanner(int rowGroupIndex, int projectedColumnIndex);
}
