/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.memory;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.metadata.Encoding;

/// What a decode charges before it allocates, from what it reads of a page: a dictionary page's
/// entries and buffers, a data page's batches and buffers, and the values a decode reconstructs
/// beyond a page's own bytes, which no header bounds. A read charges the first two from page
/// headers before the decode starts, and the third as the decode learns it; decoding outside a
/// read charges all three to a [WorkingSet].
public interface DecodeCharges {

    /// Charges a dictionary page of `numValues` entries before it is decompressed and parsed.
    ///
    /// @throws dev.hardwood.reader.ParquetReadException if it does not fit
    void dictionary(int numValues, int uncompressedSize);

    /// Charges a data page before it is decompressed and decoded.
    ///
    /// @param decodedBytes the bytes its task decompresses or copies the page into; `0` for a page
    ///        whose values are read where they lie
    /// @param kept whether the column keeps the page's bytes and levels past its task
    /// @throws dev.hardwood.reader.ParquetReadException if it does not fit
    void page(int numValues, int uncompressedSize, long decodedBytes, Encoding encoding, boolean kept);

    /// Charges `valueBytes` of values the decode of a data page, charged as
    /// [#page(int, int, long, Encoding, boolean)] gives, reconstructs beyond its own bytes, as
    /// `DELTA_BYTE_ARRAY` does, before it allocates them, on the decode's `buffers`.
    ///
    /// @throws dev.hardwood.reader.ParquetReadException if they do not fit
    void reconstructed(int numValues, int uncompressedSize, long decodedBytes, Encoding encoding, boolean kept,
                       long valueBytes, DecodeBuffers buffers);
}
