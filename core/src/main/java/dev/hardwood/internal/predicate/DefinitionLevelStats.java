/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

/// One unit's definition level histogram, and what it proves about a null predicate on a node in
/// the leaf's path — the only statistic that separates an absent group from a present one whose
/// children are null.
///
/// The histogram holds one bucket per definition level up to the leaf's maximum, counting the
/// entries written at that level. A null predicate on a node splits the buckets at the node's own
/// level: everything below was written with the node absent, everything at or above with it
/// present.
///
/// @param histogram the unit's histogram, or `null` where the file omits it
/// @param rowCount the number of rows the unit covers, or [UnitStats#UNKNOWN_ROW_COUNT]
record DefinitionLevelStats(long[] histogram, long rowCount) {

    /// `IS NULL` on the node present at or above `definitionLevel`, which the entries below it
    /// match.
    FilterDecision decideIsNull(int definitionLevel, int leafDefinitionLevel) {
        if (!sizedFor(leafDefinitionLevel)) {
            return FilterDecision.MIGHT_MATCH;
        }
        return decide(hasEntryIn(0, definitionLevel), hasEntryIn(definitionLevel, histogram.length));
    }

    /// `IS NOT NULL` on the node present at or above `definitionLevel`, which the entries at or
    /// above it match.
    FilterDecision decideIsNotNull(int definitionLevel, int leafDefinitionLevel) {
        if (!sizedFor(leafDefinitionLevel)) {
            return FilterDecision.MIGHT_MATCH;
        }
        return decide(hasEntryIn(definitionLevel, histogram.length), hasEntryIn(0, definitionLevel));
    }

    /// Whether the file wrote a histogram for a leaf at `leafDefinitionLevel`, one bucket per
    /// level up to it. A histogram it omits, or wrote at another length, proves nothing.
    boolean sizedFor(int leafDefinitionLevel) {
        return histogram != null && histogram.length == leafDefinitionLevel + 1;
    }

    /// The decision, given whether the histogram counts an entry on the predicate's side of the
    /// split and on the other side.
    ///
    /// A unit with no entry on the predicate's side cannot match: a row with the node absent
    /// writes one entry below the split into every leaf beneath it, so no such entry means no
    /// such row.
    ///
    /// One whose every entry is on the predicate's side matches throughout — but only where the
    /// histogram accounts for exactly one entry per row, since otherwise "every entry" is not
    /// "every row". A non-repeated leaf writes one entry per row; a leaf below a `LIST` or a
    /// `MAP` writes one per element, so the row count is what separates the two rather than the
    /// schema.
    private FilterDecision decide(boolean matchingEntry, boolean otherEntry) {
        if (!matchingEntry) {
            return FilterDecision.CANNOT_MATCH;
        }

        return !otherEntry && rowCount >= 0 && totalEntries() == rowCount
                ? FilterDecision.ALWAYS_MATCHES
                : FilterDecision.MIGHT_MATCH;
    }

    /// Whether any bucket from level `from` up to, but excluding, level `to` counts an entry.
    private boolean hasEntryIn(int from, int to) {
        for (int level = from; level < to; level++) {
            if (histogram[level] > 0) {
                return true;
            }
        }

        return false;
    }

    /// The number of leaf entries the histogram accounts for.
    private long totalEntries() {
        long total = 0;

        for (long count : histogram) {
            total += count;
        }

        return total;
    }
}
