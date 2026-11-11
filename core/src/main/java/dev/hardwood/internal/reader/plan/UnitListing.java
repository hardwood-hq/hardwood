/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.util.BitSet;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.predicate.RowGroupFilterEvaluator;
import dev.hardwood.internal.reader.footer.FileColumnOrdinals;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.FileSchema;

/// A row group of a read, as the listing of its file decided it: its file and its position in
/// the read and in the file, the key of its first row, what statistics decided of it, each decoded
/// column's chunk and the rows the read's row limit reaches in it.
///
/// @param index its position among the read's units
/// @param rowGroup its index in its file, which pruning makes differ from its position
/// @param base the key of its first row: the rows of the units before it
/// @param alwaysMatches whether statistics proved every row of it matches the filter
/// @param leafDecisions what statistics decided for each leaf of the filter, for its Bloom filters
///        and dictionaries to sharpen once the read reaches it; `null` without metadata filtering
/// @param bloomFilterColumns the file ordinals of the columns whose Bloom filter a leaf would probe
/// @param chunks per decoded column, its chunk in the file; not written once listed
/// @param limit the rows of the row group the read's row limit reaches, its leading skipped rows
///        included; all of them without a limit
public record UnitListing(InputFile file, int fileIndex, FileSchema fileSchema, FileColumnOrdinals columnOrdinals,
                   RowGroup metaData, int rowGroup, int index, long base, boolean alwaysMatches,
                   RowGroupFilterEvaluator.LeafDecisions leafDecisions, BitSet bloomFilterColumns,
                   ColumnChunk[] chunks, long limit) {

    /// Fails unless every chunk of the row group stores its data in the file being read. It
    /// precedes every read the row group drives, whose offsets would address the other file for
    /// such a chunk.
    ///
    /// @throws UnsupportedOperationException if any chunk names another file
    public void requireSameFile() {
        List<ColumnChunk> columns = metaData.columns();
        for (int i = 0; i < columns.size(); i++) {
            try {
                columns.get(i).requireSameFile();
            }
            catch (UnsupportedOperationException e) {
                throw new UnsupportedOperationException(ExceptionContext.filePrefix(file.name())
                        + "Cannot read column " + i + " in row group " + rowGroup + ": " + e.getMessage(), e);
            }
        }
    }

    /// Whether every chunk of the row group stores its data in the file being read.
    public boolean storedInThisFile() {
        for (ColumnChunk chunk : metaData.columns()) {
            if (!chunk.filePath().isEmpty()) {
                return false;
            }
        }
        return true;
    }
}
