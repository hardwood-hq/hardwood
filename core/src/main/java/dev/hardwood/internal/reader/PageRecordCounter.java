/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// Counts top-level records inside a single Parquet data page without invoking
/// the value decoder.
///
/// For flat columns (`maxRepetitionLevel == 0`) the page's record count is
/// `header.num_values` — there are no repetition levels and one value per
/// record. The caller can short-circuit and never invoke this helper.
///
/// For nested columns this helper walks the repetition-level RLE/bit-packing
/// hybrid stream and counts `rep_level == 0` occurrences without materialising
/// the decoded levels into a temporary `int[numValues]` buffer. The encoding
/// is identical between `DATA_PAGE` (v1) and `DATA_PAGE_V2`; only the
/// *position* of the rep-level bytes differs. In v2 they live in an
/// **uncompressed** prefix at the start of the page (sized by
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
    /// Walks runs in place and never allocates a per-page level buffer; for a
    /// 1 GiB nested-v2 row group with ~1000 pages of 1M values this saves on
    /// the order of hundreds of megabytes of transient `int[]` allocations
    /// versus decoding into a materialised buffer just to discard it.
    ///
    /// @param levels byte array containing the encoded rep-level stream; the
    ///        relevant slice starts at `offset`
    /// @param offset starting offset into `levels`
    /// @param length number of rep-level bytes (typically
    ///        `DataPageHeaderV2.repetition_levels_byte_length`)
    /// @param numValues total values in the page (`header.num_values`); the
    ///        walker reads exactly this many levels
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
        // bitWidth == 0 only happens when maxRepetitionLevel == 0, already rejected above.
        return walkAndCountZeros(levels, offset, length, numValues, bitWidth);
    }

    /// Walks an RLE/bit-packing-hybrid stream counting values that decode to `0`.
    ///
    /// Mirrors the run-dispatch logic of [dev.hardwood.internal.encoding.RleBitPackingHybridDecoder]
    /// but accumulates a count instead of writing into an output buffer. Carries
    /// leftover bits across bit-packed boundaries via `bitBuffer`/`bitsInBuffer`
    /// for the same reason the decoder does — Parquet's bit-packed groups are
    /// byte-aligned per group of 8, but a partial group at the end of a run
    /// shares the trailing byte with whatever follows.
    private static int walkAndCountZeros(byte[] data, int offset, int length,
                                          int numValues, int bitWidth) {
        final int dataEnd = offset + length;
        final int bitMask = (1 << bitWidth) - 1;
        int pos = offset;
        long bitBuffer = 0;
        int bitsInBuffer = 0;
        int remaining = numValues;
        int records = 0;

        while (remaining > 0) {
            if (pos >= dataEnd) {
                break;
            }

            // Read run header (unsigned varint).
            long header = 0;
            int shift = 0;
            while (pos < dataEnd) {
                int b = data[pos++] & 0xFF;
                header |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    break;
                }
                shift += 7;
            }

            if ((header & 1) == 0) {
                // RLE run: a single value repeated `repeatCount` times.
                int repeatCount = (int) (header >> 1);
                int value = 0;
                int bytesNeeded = (bitWidth + 7) / 8;
                for (int i = 0; i < bytesNeeded && pos < dataEnd; i++) {
                    value |= (data[pos++] & 0xFF) << (i * 8);
                }
                value &= bitMask;

                int toRead = Math.min(remaining, repeatCount);
                if (value == 0) {
                    records += toRead;
                }
                remaining -= toRead;
                continue;
            }

            // Bit-packed run: `groups` 8-value groups; each group consumes
            // exactly `bitWidth` bytes. We may stop mid-run when `remaining`
            // is exhausted; any unread bytes are discarded with the walker.
            int groups = (int) (header >> 1);
            int runLength = groups * 8;
            int toRead = Math.min(remaining, runLength);
            int read = 0;

            // Drain any leftover bits buffered from a prior partial extraction.
            while (bitsInBuffer >= bitWidth && read < toRead) {
                if ((bitBuffer & bitMask) == 0) {
                    records++;
                }
                bitBuffer >>>= bitWidth;
                bitsInBuffer -= bitWidth;
                read++;
            }

            // Fast path bw=1 — count zero bits per byte. Each byte holds
            // exactly 8 rep-level bits, so popcount tells us how many of those
            // 8 are non-zero (continuations); the rest are records.
            if (bitWidth == 1) {
                while (read + 8 <= toRead && pos < dataEnd) {
                    int b = data[pos++] & 0xFF;
                    records += 8 - Integer.bitCount(b);
                    read += 8;
                }
            }
            // For other widths up to 8: pull `bitWidth` bytes covering 8 values
            // and unpack inline.
            else if (bitWidth <= 8) {
                while (read + 8 <= toRead && pos + bitWidth <= dataEnd) {
                    long bits = 0;
                    for (int i = 0; i < bitWidth; i++) {
                        bits |= ((long) (data[pos++] & 0xFF)) << (i * 8);
                    }
                    for (int i = 0; i < 8; i++) {
                        if ((bits & bitMask) == 0) {
                            records++;
                        }
                        bits >>>= bitWidth;
                    }
                    read += 8;
                }
            }

            // Tail: pull individual values via the bit buffer until `toRead`
            // is met. Handles bit widths > 8 and the final partial group.
            while (read < toRead) {
                while (bitsInBuffer < bitWidth && pos < dataEnd) {
                    bitBuffer |= ((long) (data[pos++] & 0xFF)) << bitsInBuffer;
                    bitsInBuffer += 8;
                }
                if (bitsInBuffer < bitWidth) {
                    break;
                }
                if ((bitBuffer & bitMask) == 0) {
                    records++;
                }
                bitBuffer >>>= bitWidth;
                bitsInBuffer -= bitWidth;
                read++;
            }

            remaining -= read;
        }

        if (remaining > 0) {
            throw new IllegalStateException("Insufficient RLE/Bit-Packing data: walked "
                    + (numValues - remaining) + " of " + numValues + " requested values");
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
