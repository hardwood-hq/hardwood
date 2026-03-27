/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.PageLocation;

/// Holds fetched page-range buffers for a single column chunk.
///
/// Each [FetchedRange] corresponds to one coalesced [PageRange] and contains
/// the bytes fetched via a single `readRange()` call. Pages are sliced from
/// the appropriate range buffer based on their absolute file offset.
public record PageRangeData(List<FetchedRange> ranges) {

    /// A single fetched byte range and its absolute file offset.
    record FetchedRange(long fileOffset, ByteBuffer data) {

        /// Returns true if this range contains the given absolute file offset.
        boolean contains(long offset, int length) {
            return offset >= fileOffset && offset + length <= fileOffset + data.capacity();
        }
    }

    /// Slices a page's data from the appropriate fetched range buffer.
    ///
    /// @param loc the page location with absolute file offset and compressed size
    /// @return a ByteBuffer slice covering the page data
    ByteBuffer slicePage(PageLocation loc) {
        for (FetchedRange range : ranges) {
            if (range.contains(loc.offset(), loc.compressedPageSize())) {
                int relOffset = Math.toIntExact(loc.offset() - range.fileOffset());
                return range.data().slice(relOffset, loc.compressedPageSize());
            }
        }
        throw new IllegalStateException("Page at offset " + loc.offset()
                + " (size " + loc.compressedPageSize() + ") not found in any fetched range");
    }

    /// Slices a region from the fetched ranges by absolute file offset and length.
    /// Used for dictionary page parsing.
    ByteBuffer sliceRegion(long offset, int length) {
        for (FetchedRange range : ranges) {
            if (range.contains(offset, length)) {
                int relOffset = Math.toIntExact(offset - range.fileOffset());
                return range.data().slice(relOffset, length);
            }
        }
        throw new IllegalStateException("Region at offset " + offset
                + " (length " + length + ") not found in any fetched range");
    }

    /// Fetches page ranges from an [InputFile], issuing one `readRange()` per [PageRange].
    public static PageRangeData fetch(InputFile inputFile, List<PageRange> pageRanges) throws IOException {
        List<FetchedRange> fetched = new ArrayList<>(pageRanges.size());
        for (PageRange range : pageRanges) {
            ByteBuffer data = inputFile.readRange(range.offset(), range.length());
            fetched.add(new FetchedRange(range.offset(), data));
        }
        return new PageRangeData(fetched);
    }
}
