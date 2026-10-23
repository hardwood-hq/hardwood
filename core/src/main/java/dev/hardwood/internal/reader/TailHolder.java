/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;

import dev.hardwood.InputFile;

/// An [InputFile] whose open fetches the end of the file in the same request that learns its
/// length, and holds those bytes, the tail, to serve every read that lies inside it: a file no
/// longer than the tail is held whole. The S3 backend is one.
///
/// A read opens such a file with the tail sized for where its footers come from
/// ([ParquetMetadataReader#open]), charges the tail of a file it opens ahead to the context's
/// [ReadAheadBuffer] ([FileOpenAhead]), and releases the tail once it is done with the file
/// ([RowGroupIterator]).
public interface TailHolder {

    /// The most bytes an open holds: the tail's size for a read whose footers are read from the
    /// file, or, when `footerSupplied`, for one whose footers a [dev.hardwood.MetadataSource]
    /// supplies.
    long tailReservation(boolean footerSupplied);

    /// Opens the file as [InputFile#open()] does, fetching the tail sized for `footerSupplied`
    /// ([#tailReservation]). Idempotent: a file already open is left as it is.
    void open(boolean footerSupplied) throws IOException;

    /// Drops the tail. Later reads it would have served issue requests; buffers it handed out stay
    /// valid. Thread-safe, and safe to call more than once.
    void releaseTail();

    /// Releases `file`'s tail if it holds one.
    static void releaseTail(InputFile file) {
        if (file instanceof TailHolder holder) {
            holder.releaseTail();
        }
    }
}
