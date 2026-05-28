/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

/// Plan describing how to merge per-column
/// [BatchExchange.Batch#matches][dev.hardwood.internal.reader.BatchExchange.Batch#matches]
/// bitmaps into a single per-batch combined survivor mask.
///
/// Produced by [BatchFilterCompiler] alongside the per-column
/// [ColumnBatchMatcher] array. A [Column] node references the projected index
/// of a column whose worker emitted a `matches` array; [And] and [Or] combine
/// children word-wise.
///
/// Each leaf column appears in at most one [Column] node — same-column leaves
/// are pre-composed into [AndBatchMatcher] / [OrBatchMatcher] inside the
/// per-column matcher, so the plan only encodes cross-column structure.
public sealed interface MergePlan {

    /// Common parent for [And] and [Or]: a compound with [MergePlan] children
    /// that may themselves be compounds.
    sealed interface Compound extends MergePlan {
        MergePlan[] children();
    }

    record Column(int projectedIndex) implements MergePlan {}

    record And(MergePlan[] children) implements Compound {
        public And {
            if (children.length < 2) {
                throw new IllegalArgumentException("And requires at least two children");
            }
        }
    }

    record Or(MergePlan[] children) implements Compound {
        public Or {
            if (children.length < 2) {
                throw new IllegalArgumentException("Or requires at least two children");
            }
        }
    }
}
