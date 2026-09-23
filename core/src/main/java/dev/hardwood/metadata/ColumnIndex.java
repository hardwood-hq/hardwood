/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import dev.hardwood.reader.ParquetReadException;

/// Column index for a column chunk, providing per-page min/max statistics for page-level filtering.
///
/// The level histograms hold one histogram per page, concatenated into a single array in
/// page order: with `maxLevel + 1` entries per page, page `p` occupies the range from
/// `p * (maxLevel + 1)` up to `(p + 1) * (maxLevel + 1)`.
/// [#repetitionLevelHistogram(int)] and [#definitionLevelHistogram(int)] slice one page out.
///
/// The per-page arrays are the values as the file recorded them and are not copied
/// on the way in or out. An absent optional array is `null`, which stays distinct from a
/// zero-length one the writer recorded as empty.
///
/// @param nullPages one flag per page, set for a page that contains only null values
/// @param minValues per-page minimum values in the column's physical sort order
/// @param maxValues per-page maximum values in the column's physical sort order
/// @param boundaryOrder ordering of min/max values: UNORDERED, ASCENDING, or DESCENDING
/// @param nullCounts per-page null counts, or `null` if not available
/// @param repetitionLevelHistograms per-page repetition-level histograms concatenated
///     page-major, or `null` if not available. See the note on layout above
/// @param definitionLevelHistograms per-page definition-level histograms concatenated
///     page-major, or `null` if not available
/// @param nanCounts per-page NaN counts, or `null` if not available. Only meaningful for
///     `FLOAT`, `DOUBLE` and `FLOAT16` columns, whose NaN values sit outside the ordering
///     of `minValues`/`maxValues`
/// @see <a href="https://parquet.apache.org/docs/file-format/pageindex/">File Format – Page Index</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public record ColumnIndex(
        boolean[] nullPages,
        List<byte[]> minValues,
        List<byte[]> maxValues,
        BoundaryOrder boundaryOrder,
        long[] nullCounts,
        long[] repetitionLevelHistograms,
        long[] definitionLevelHistograms,
        long[] nanCounts) {

    /// Ordering of min/max values across pages.
    public enum BoundaryOrder {
        UNORDERED,
        ASCENDING,
        DESCENDING
    }

    /// Returns the number of pages described by this index.
    public int getPageCount() {
        return nullPages.length;
    }

    /// Returns one page's slice of [#repetitionLevelHistograms()], or `null` if the file
    /// records no repetition-level histogram for this chunk.
    ///
    /// @param pageIndex page to slice, in `[0, getPageCount())`
    /// @throws IndexOutOfBoundsException if `pageIndex` is outside that range
    /// @throws ParquetReadException if the histogram's length is not a whole number of
    ///     pages, so no per-page stride describes it
    public long[] repetitionLevelHistogram(int pageIndex) {
        return pageSlice(repetitionLevelHistograms, pageIndex, "repetition");
    }

    /// Returns one page's slice of [#definitionLevelHistograms()], or `null` if the file
    /// records no definition-level histogram for this chunk.
    ///
    /// @param pageIndex page to slice, in `[0, getPageCount())`
    /// @throws IndexOutOfBoundsException if `pageIndex` is outside that range
    /// @throws ParquetReadException if the histogram's length is not a whole number of
    ///     pages, so no per-page stride describes it
    public long[] definitionLevelHistogram(int pageIndex) {
        return pageSlice(definitionLevelHistograms, pageIndex, "definition");
    }

    /// The per-page stride is `histograms.length / getPageCount()`: the concatenation holds
    /// `maxLevel + 1` entries for each of `getPageCount()` pages, so the column's maximum
    /// level follows from the two lengths and no schema reference is needed.
    ///
    /// The returned slice is a copy, unlike the whole-chunk accessors, which hand out the
    /// array the file was read into.
    private long[] pageSlice(long[] histograms, int pageIndex, String level) {
        if (histograms == null) {
            return null;
        }
        int pageCount = getPageCount();
        Objects.checkIndex(pageIndex, pageCount);
        if (histograms.length % pageCount != 0) {
            throw new ParquetReadException("Malformed Parquet metadata: " + level
                    + "-level histogram holds " + histograms.length + " entries for " + pageCount
                    + " pages, which is not a whole number of entries per page");
        }
        int stride = histograms.length / pageCount;
        return Arrays.copyOfRange(histograms, pageIndex * stride, (pageIndex + 1) * stride);
    }
}
