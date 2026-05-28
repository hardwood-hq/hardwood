/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

/// Result of compiling an eligible [ResolvedPredicate] for the drain-side
/// filter path.
///
/// - `columnMatchers[i]` is the [ColumnBatchMatcher] installed on projected
///   column `i`, or `null` if the column has no filter contribution. Workers
///   run this matcher against every published batch.
/// - `mergePlan` is the consumer-side [MergePlan] walked by
///   [dev.hardwood.internal.reader.FlatRowReader] to combine the per-column
///   bitmaps into one per-batch survivor mask.
public record CompiledBatchFilter(ColumnBatchMatcher[] columnMatchers, MergePlan mergePlan) {
}
