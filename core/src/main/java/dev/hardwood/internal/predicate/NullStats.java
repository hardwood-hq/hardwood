/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

/// One unit's null count, and what it proves about a predicate on the leaf column itself.
///
/// [FilterPredicateResolver] refuses a repeated column to a directly named leaf, so such a leaf
/// writes exactly one entry per row, and its null count is a count of rows. That is what lets
/// the count be held against the unit's row count at all. A null predicate on an enclosing group
/// is answered from a leaf that may be repeated, where it does not hold, and goes to
/// [DefinitionLevelStats] instead; [UnitStats#decide] is what keeps it from reaching here.
///
/// @param nullCount the number of null entries in the unit, or [#UNKNOWN_NULL_COUNT]
/// @param rowCount the number of rows the unit covers, or [UnitStats#UNKNOWN_ROW_COUNT]
record NullStats(long nullCount, long rowCount) {

    /// The null count of a unit whose source does not carry one.
    static final long UNKNOWN_NULL_COUNT = -1;

    /// Whether the unit is proven to hold no nulls.
    boolean noNulls() {
        return nullCount == 0;
    }

    /// Whether the unit is proven to be null on every row.
    boolean allNull() {
        return rowCount >= 0 && nullCount == rowCount;
    }

    /// `IS NULL` on the leaf, which no row matches where no entry is null.
    FilterDecision decideIsNull() {
        return noNulls() ? FilterDecision.CANNOT_MATCH : FilterDecision.MIGHT_MATCH;
    }

    /// `IS NOT NULL` on the leaf, which no row matches where every row is null, and every row
    /// matches where none is.
    FilterDecision decideIsNotNull() {
        if (allNull()) {
            return FilterDecision.CANNOT_MATCH;
        }
        return noNulls() ? FilterDecision.ALWAYS_MATCHES : FilterDecision.MIGHT_MATCH;
    }
}
