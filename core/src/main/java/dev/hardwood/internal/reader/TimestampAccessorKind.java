/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.schema.SchemaNode;

/// Shared dispatch guards for the TIMESTAMP accessor pair (#568).
///
/// The Parquet TIMESTAMP logical type carries an `isAdjustedToUTC` flag that
/// picks between two distinct semantic kinds (UTC-adjusted instant vs.
/// wall-clock local timestamp). The row-level accessors are split along the
/// same line: `getTimestamp` requires UTC-adjusted, `getLocalTimestamp`
/// requires local. Each impl funnels through [#require] so the rejection
/// message is identical everywhere and the INT96 fallthrough is handled in
/// one place.
public final class TimestampAccessorKind {

    private TimestampAccessorKind() {
    }

    /// Verify that a leaf is the right TIMESTAMP kind for the accessor being called, for
    /// a caller holding the leaf's schema node. A group node is not a timestamp leaf and
    /// passes through, as a non-TIMESTAMP annotation does.
    static void require(SchemaNode schema, boolean wantUtcAdjusted) {
        if (schema instanceof SchemaNode.PrimitiveNode primitive) {
            require(schema.name(), primitive.logicalType(), wantUtcAdjusted);
        }
    }

    /// Verify that a column is the kind of TIMESTAMP the accessor being called reads.
    ///
    /// A `null` annotation is a legacy INT96 column, which carries no `isAdjustedToUTC`
    /// field and is conventionally UTC-adjusted. A non-TIMESTAMP annotation passes through
    /// without action: the caller's subsequent typed read fails with its own type-mismatch
    /// exception.
    ///
    /// @throws IllegalStateException if the column is the other kind
    static void require(String columnName, LogicalType lt, boolean wantUtcAdjusted) {
        if (lt != null && !(lt instanceof LogicalType.TimestampType)) {
            return;
        }
        boolean utcAdjusted = lt == null || ((LogicalType.TimestampType) lt).isAdjustedToUTC();
        if (utcAdjusted != wantUtcAdjusted) {
            throw new IllegalStateException(
                    "Column '" + columnName + "' is " + describe(lt, utcAdjusted));
        }
    }

    private static String describe(LogicalType lt, boolean utcAdjusted) {
        if (lt == null) {
            return describeLegacyInt96();
        }
        return describe(utcAdjusted);
    }

    /// The kind of an unannotated `INT96` column, as a rejection names it.
    public static String describeLegacyInt96() {
        return "a legacy INT96 TIMESTAMP (no isAdjustedToUTC field)";
    }

    /// The kind of a column annotated TIMESTAMP, as a rejection names it.
    public static String describe(boolean utcAdjusted) {
        return utcAdjusted
                ? "a UTC-adjusted TIMESTAMP (isAdjustedToUTC=true)"
                : "a local-wall-clock TIMESTAMP (isAdjustedToUTC=false)";
    }
}
