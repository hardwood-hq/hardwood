/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

/// Marker for `int` typed [ColumnBatchMatcher]s. Implementations cast `batch.values` to `int[]`.
public non-sealed interface IntBatchMatcher extends ColumnBatchMatcher {
}