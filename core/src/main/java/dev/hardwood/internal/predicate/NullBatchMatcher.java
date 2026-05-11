/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

/// Marker for IS NULL / IS NOT NULL [ColumnBatchMatcher]s. Implementations only inspect
/// the batch's null tracking — they do not touch `batch.values`.
public non-sealed interface NullBatchMatcher extends ColumnBatchMatcher {
}