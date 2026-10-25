/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import dev.hardwood.internal.predicate.dictionary.RowGroupDictionaryFilterSource;
import dev.hardwood.internal.reader.select.RowRanges;

/// Shared metadata for one row group, reused across columns.
///
/// `matchingRows` is filter-derived only: the tail-read fast path's synthesized
/// `[tailSkip, numRows)` range is applied to the plans, so this record can be planned before
/// `tailSkip` is known.
///
/// `dictionaries` holds the dictionaries pruning read for this row group, for the fetch plans
/// to decode with rather than read them again; `null` when pruning read none. A row group its
/// bloom filters or dictionaries prove holds no match is `dropped`, and nothing else is read
/// for it.
public record SharedRowGroupMetadata(
        RowGroupIndexBuffers indexBuffers,
        RowRanges matchingRows,
        RowGroupDictionaryFilterSource dictionaries,
        boolean dropped
) {}
