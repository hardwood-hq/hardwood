/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.metadata.LogicalType;

/// Shared dispatch guards for the TIMESTAMP accessor pair (#568).
///
/// The Parquet TIMESTAMP logical type carries an `isAdjustedToUTC` flag that
/// picks between two distinct semantic kinds (UTC-adjusted instant vs.
/// wall-clock local timestamp). The row-level accessors are split along the
/// same line: `getTimestamp` requires UTC-adjusted, `getLocalTimestamp`
/// requires local. Each impl funnels through [#require] so the rejection
/// message is identical everywhere and the INT96 fallthrough is handled in
/// one place.
final class TimestampAccessorKind {

    private TimestampAccessorKind() {
    }

    /// Verify that a column is the right TIMESTAMP kind for the accessor being
    /// called. A `null` logical type means a legacy INT96 column (no
    /// `isAdjustedToUTC` field); the convention is to treat it as UTC-adjusted,
    /// so it is accepted by [#requireUtcAdjusted] and rejected by [#requireLocal].
    /// Non-TIMESTAMP logical types pass through without action — the caller's
    /// subsequent typed read will fail with its own type-mismatch exception.
    ///
    /// @throws IllegalStateException if the column's kind doesn't match the request
    static void require(String columnName, LogicalType lt, boolean wantUtcAdjusted) {
        if (lt == null) {
            // INT96: only the UTC-adjusted accessor accepts it.
            if (!wantUtcAdjusted) {
                throw new IllegalStateException("Column '" + columnName
                        + "' is a legacy INT96 TIMESTAMP (no isAdjustedToUTC field); "
                        + "use getTimestamp instead");
            }
            return;
        }
        if (lt instanceof LogicalType.TimestampType tt) {
            if (wantUtcAdjusted) {
                requireUtcAdjusted(columnName, tt);
            }
            else {
                requireLocal(columnName, tt);
            }
        }
    }

    static void requireUtcAdjusted(String columnName, LogicalType.TimestampType tt) {
        if (!tt.isAdjustedToUTC()) {
            throw new IllegalStateException("Column '" + columnName
                    + "' is a local-wall-clock TIMESTAMP (isAdjustedToUTC=false); "
                    + "use getLocalTimestamp instead");
        }
    }

    static void requireLocal(String columnName, LogicalType.TimestampType tt) {
        if (tt.isAdjustedToUTC()) {
            throw new IllegalStateException("Column '" + columnName
                    + "' is a UTC-adjusted TIMESTAMP (isAdjustedToUTC=true); "
                    + "use getTimestamp instead");
        }
    }
}
