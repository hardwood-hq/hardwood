/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.internal.encoding.RleBitPackingHybridDecoder;

/// Counts top-level records inside a single Parquet data page without invoking
/// the value decoder.
///
/// For flat columns (`maxRepetitionLevel == 0`) the page's record count is
/// `header.num_values` — there are no repetition levels and one value per
/// record. The caller can short-circuit and never invoke this helper.
///
/// For nested columns this helper decodes a repetition-level RLE/bit-packing
/// hybrid stream and counts `rep_level == 0` occurrences. The encoding is
/// identical between `DATA_PAGE` (v1) and `DATA_PAGE_V2`; only the *position*
/// of the rep-level bytes differs. In v2 they live in an **uncompressed**
/// prefix at the start of the page (sized by
/// `DataPageHeaderV2.repetition_levels_byte_length`) and can be walked
/// without touching the codec — the basis for the skip-without-decompress
/// optimisation in [SequentialFetchPlan]. In v1 the rep-levels live inside
/// the compressed area, so reaching them requires decompressing the whole
/// page body, which defeats the purpose; callers are responsible for gating
/// v1-nested pages out before invoking this helper.
final class PageRecordCounter {

    private PageRecordCounter() {
    }

    /// Counts top-level records (`rep_level == 0` occurrences) in an
    /// RLE/bit-packing-hybrid encoded repetition-level stream.
    ///
    /// @param levels byte array containing the encoded rep-level stream; the
    ///        relevant slice starts at `offset`
    /// @param offset starting offset into `levels`
    /// @param length number of rep-level bytes (typically
    ///        `DataPageHeaderV2.repetition_levels_byte_length`)
    /// @param numValues total values in the page (`header.num_values`); the
    ///        decoder reads exactly this many levels
    /// @param maxRepetitionLevel column's maximum repetition level; used to
    ///        derive the level bit width
    /// @return number of top-level records in the page
    /// @throws IllegalArgumentException if `maxRepetitionLevel` is `0` (flat
    ///         columns must not call this — use `header.num_values` directly)
    static int countTopLevelRecords(byte[] levels, int offset, int length,
                                     int numValues, int maxRepetitionLevel) {
        if (maxRepetitionLevel <= 0) {
            throw new IllegalArgumentException(
                    "countTopLevelRecords called on a flat column "
                            + "(maxRepetitionLevel=" + maxRepetitionLevel
                            + "); use header.num_values directly");
        }
        if (numValues <= 0) {
            return 0;
        }
        int bitWidth = bitWidthFor(maxRepetitionLevel);
        int[] decoded = new int[numValues];
        RleBitPackingHybridDecoder decoder =
                new RleBitPackingHybridDecoder(levels, offset, length, bitWidth);
        decoder.readInts(decoded, 0, numValues);
        int records = 0;
        for (int i = 0; i < numValues; i++) {
            if (decoded[i] == 0) {
                records++;
            }
        }
        return records;
    }

    private static int bitWidthFor(int maxLevel) {
        if (maxLevel <= 0) {
            return 0;
        }
        return 32 - Integer.numberOfLeadingZeros(maxLevel);
    }
}
