/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

/// Marker for `boolean` typed [BatchMatcher]s. Implementations cast `batch.values` to `boolean[]`.
public non-sealed interface BooleanBatchMatcher extends BatchMatcher {
}