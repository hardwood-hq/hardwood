/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

/// Three-valued outcome of evaluating a filter predicate against statistics.
///
/// Extends the boolean "can this unit be dropped?" question with its dual:
/// statistics can also prove that **every** row of a unit matches, in which
/// case per-row predicate evaluation over the unit is redundant.
///
/// A decision is always conservative: when statistics are absent, partial, or
/// untrusted, the decision is [#MIGHT_MATCH], which preserves today's behavior
/// exactly (evaluate rows individually).
public enum StatsDecision {

    /// Statistics prove no row matches; the unit can be skipped entirely.
    /// Equivalent to `canDrop == true`.
    CANNOT_MATCH,

    /// Statistics cannot decide; rows must be evaluated individually.
    /// Equivalent to `canDrop == false`.
    MIGHT_MATCH,

    /// Statistics prove every row matches; the unit can be read with
    /// per-row predicate evaluation skipped.
    ALWAYS_MATCHES;

    /// Combines two decisions under logical AND.
    static StatsDecision and(StatsDecision a, StatsDecision b) {
        if (a == CANNOT_MATCH || b == CANNOT_MATCH) {
            return CANNOT_MATCH;
        }
        if (a == ALWAYS_MATCHES && b == ALWAYS_MATCHES) {
            return ALWAYS_MATCHES;
        }
        return MIGHT_MATCH;
    }

    /// Combines two decisions under logical OR.
    static StatsDecision or(StatsDecision a, StatsDecision b) {
        if (a == ALWAYS_MATCHES || b == ALWAYS_MATCHES) {
            return ALWAYS_MATCHES;
        }
        if (a == CANNOT_MATCH && b == CANNOT_MATCH) {
            return CANNOT_MATCH;
        }
        return MIGHT_MATCH;
    }
}
