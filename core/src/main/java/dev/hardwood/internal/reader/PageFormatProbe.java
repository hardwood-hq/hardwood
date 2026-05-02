/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import dev.hardwood.InputFile;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;

/// Reads just enough of a column chunk to identify its first data page's
/// format (v1 vs v2). Used by the per-page mask gate in [RowGroupIterator] to
/// distinguish nested-v1 columns — whose repetition levels live inside the
/// compressed area and would require decompression to count records — from
/// nested-v2 columns, whose repetition levels live in an uncompressed prefix
/// and can be walked without invoking the codec.
///
/// All data pages in a single column chunk share the same format in practice
/// (Parquet writers don't mix v1 and v2 within one chunk), so a single peek
/// at the first data page is authoritative.
final class PageFormatProbe {

    /// Initial peek size for the page header. One KiB covers a typical header
    /// without inline `min_value`/`max_value` binaries.
    private static final int INITIAL_PEEK_SIZE = 1024;

    /// Upper bound on the peek size. Headers carrying long inline statistics
    /// rarely exceed a few KiB; 1 MiB is comfortably beyond that and protects
    /// against runaway reads on a corrupt file.
    private static final int MAX_PEEK_SIZE = 1024 * 1024;

    private PageFormatProbe() {
    }

    /// Returns the page type of the first data page in `columnChunk`. Reads at
    /// most one bounded `readRange` from `inputFile` (with growth on EOF for
    /// oversize headers). The dictionary page, if any, is skipped over by
    /// reading at the column's `dataPageOffset` directly.
    static PageHeader.PageType firstDataPageType(InputFile inputFile,
                                                  ColumnChunk columnChunk) throws IOException {
        long offset = columnChunk.metaData().dataPageOffset();
        long maxLength = columnChunk.metaData().totalCompressedSize();
        int peek = (int) Math.min(INITIAL_PEEK_SIZE, maxLength);
        int peekCeiling = (int) Math.min(MAX_PEEK_SIZE, maxLength);
        while (true) {
            ByteBuffer buf = inputFile.readRange(offset, peek);
            try {
                PageHeader header = PageHeaderReader.read(new ThriftCompactReader(buf));
                return header.type();
            }
            catch (EOFException eof) {
                if (peek >= peekCeiling) {
                    throw new IOException("First data page header for column at offset "
                            + offset + " exceeds " + peekCeiling
                            + " bytes — the file is likely corrupt", eof);
                }
                peek = Math.min(peekCeiling, peek * 2);
            }
        }
    }
}
