/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

/// Marker for `double` typed [BatchMatcher]s. Implementations cast `batch.values` to `double[]`.
public non-sealed interface DoubleBatchMatcher extends BatchMatcher {
}