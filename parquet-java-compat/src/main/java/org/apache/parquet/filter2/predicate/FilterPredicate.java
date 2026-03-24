/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.filter2.predicate;

/// Shim for parquet-java's `FilterPredicate` interface.
///
/// Base type for all filter predicate nodes. Concrete implementations
/// are in [Operators].
public interface FilterPredicate {
}
