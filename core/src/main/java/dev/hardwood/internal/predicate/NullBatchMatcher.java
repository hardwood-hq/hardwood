/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

/// Marker for [ColumnBatchMatcher]s that answer from the batch's null tracking alone — the
/// IS NULL and IS NOT NULL tests, and the constant that matches no row. Implementations do not
/// touch `batch.values`.
public non-sealed interface NullBatchMatcher extends ColumnBatchMatcher {
}
