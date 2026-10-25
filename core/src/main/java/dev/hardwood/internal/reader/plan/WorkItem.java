/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.util.BitSet;

import dev.hardwood.InputFile;
import dev.hardwood.internal.predicate.RowGroupFilterEvaluator;
import dev.hardwood.internal.reader.footer.FileColumnOrdinals;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.FileSchema;

/// A single unit of work: one row group in one file.
///
/// `rowsConsumedBefore` is the cumulative row count of all work items
/// preceding this one in the work list — used to convert the read-wide
/// `maxRows` budget into a per-row-group remainder when computing fetch
/// plans. (Filter predicates invalidate this correlation, so callers must
/// ignore it when a filter is active.)
///
/// `leafDecisions` holds what statistics decided for each leaf of the filter,
/// for the row group's bloom filters and dictionaries to sharpen once the read
/// reaches it; `null` when no metadata filtering applies. `bloomFilterColumns`
/// holds the file ordinals of the columns whose bloom filter a leaf would probe.
///
/// `indexWindow` holds the row group's page-index and bloom-filter slices together with those
/// of the work items planned beside it; `null` only while its file is planned.
public record WorkItem(
        InputFile inputFile,
        RowGroup rowGroup,
        FileSchema fileSchema,
        FileColumnOrdinals columnOrdinals,
        int fileIndex,
        int rowGroupIndex,
        int workItemIndex,
        long rowsConsumedBefore,
        boolean filterAlwaysMatches,
        RowGroupFilterEvaluator.LeafDecisions leafDecisions,
        BitSet bloomFilterColumns,
        IndexWindow indexWindow
) {

    WorkItem withIndexWindow(IndexWindow window) {
        return new WorkItem(inputFile, rowGroup, fileSchema, columnOrdinals, fileIndex,
                rowGroupIndex, workItemIndex, rowsConsumedBefore, filterAlwaysMatches,
                leafDecisions, bloomFilterColumns, window);
    }
}
