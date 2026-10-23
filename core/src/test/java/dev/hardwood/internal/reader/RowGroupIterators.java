/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import dev.hardwood.InputFile;

/// Builds the [RowGroupIterator]s of tests that drive an iterator without a reader.
final class RowGroupIterators {

    private RowGroupIterators() {
    }

    /// An iterator over `inputFiles` with no row limit. Its close closes the files' metadata
    /// and the files, as the parent reader of a read does.
    static RowGroupIterator standalone(List<InputFile> inputFiles, HardwoodContextImpl context) {
        FileMetadataCache metadata = new FileMetadataCache(inputFiles, context);
        return new RowGroupIterator(metadata, iterator -> {
            try {
                metadata.close();
                InputFileCloser.rethrow(InputFileCloser.closeAll(inputFiles, null));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, context, 0, 0, 0);
    }
}
