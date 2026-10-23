/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.List;

import dev.hardwood.InputFile;

/// Builds the [RowGroupIterator]s of tests that drive an iterator without a reader.
final class RowGroupIterators {

    private RowGroupIterators() {
    }

    /// An iterator over `inputFiles` with no row limit that owns its files' metadata, so its close
    /// closes the files.
    static RowGroupIterator standalone(List<InputFile> inputFiles, HardwoodContextImpl context) {
        return new RowGroupIterator(new FileMetadataCache(inputFiles, context), true, iterator -> {
        }, context, 0, 0, 0);
    }
}
