/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

/// Marker for byte-array typed [ColumnBatchMatcher]s (`BYTE_ARRAY` /
/// `FIXED_LEN_BYTE_ARRAY`). Implementations cast `batch.values` to
/// [dev.hardwood.internal.reader.BinaryBatchValues].
///
/// Being byte-array typed does not by itself imply a need for retained
/// dictionary-entry indices; a matcher that consumes them declares so by
/// overriding [ColumnBatchMatcher#requiresDictionaryIndices()].
public non-sealed interface BinaryBatchMatcher extends ColumnBatchMatcher {
}
