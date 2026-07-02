/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import dev.hardwood.internal.encoding.RleBitPackingHybridDecoder;

/// Recognises the clean fixed-size-list shape from a `DataPageV2` page's
/// repetition/definition level streams alone, so the reader can skip the
/// per-element list-reconstruction state machine.
///
/// The detector is a pure predicate: it reads no vector interiors and mutates
/// nothing. It targets the single-level `LIST` shape
/// `optional group <name> (LIST) { repeated <primitive> element }`
/// (`maxRepetitionLevel == 1`, `maxDefinitionLevel == 2`); any other level
/// geometry yields [FixedSizeListShape#NOT_APPLICABLE].
///
/// ## Definition-level gate (O(1))
///
/// Every present, dense vector puts every leaf at `def == maxDef`, which the
/// RLE/bit-packing hybrid stores as a single RLE run of `maxDef` covering all
/// values. The gate accepts only that exact shape; nulls or empty lists produce
/// a lower def value or a bit-packed run and are rejected.
///
/// ## Repetition-level verification (O(rows))
///
/// The rep stream over `{0, 1}` describes list boundaries: a `0` starts a row,
/// a `1` continues it. The detector walks the hybrid runs and locates the `0`s,
/// deriving `k` from the first inter-boundary gap and requiring every subsequent
/// gap — including the final row, closed against the end of the stream — to
/// equal it. Long interior runs of `1`s arrive as RLE runs whose length is read
/// from the run header, so a vector interior is never scanned for decode or for
/// verification. `k` is verified, never assumed: a writer emitting variable
/// inner lengths that happen to sum to a multiple of the row count is rejected
/// by the per-gap check rather than silently mis-decoded.
public final class FixedSizeListDetector {

    private static final int MAX_REPETITION_LEVEL = 1;
    private static final int MAX_DEFINITION_LEVEL = 2;

    /// Little-endian `byte[]`-to-`long` view: reads 8 packed levels as one
    /// unaligned 64-bit load (a single bounds check), rather than assembling the
    /// word byte by byte, so the tiled compare stays a word-at-a-time equality.
    private static final VarHandle LONG_LE =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    /// Passed as `numRows` when the page header carries no row count
    /// (`DataPageV1`): the detector derives the row count from the values and
    /// skips the `num_rows` cross-check, relying on `numValues % k == 0` instead.
    public static final int ROWS_UNKNOWN = -1;

    private FixedSizeListDetector() {
    }

    /// Classifies a page from its already-sliced level regions.
    ///
    /// @param repData    buffer holding the repetition-level bytes
    /// @param repOffset  start of the repetition-level region within `repData`
    /// @param repLength  length in bytes of the repetition-level region
    /// @param defData    buffer holding the definition-level bytes
    /// @param defOffset  start of the definition-level region within `defData`
    /// @param defLength  length in bytes of the definition-level region
    /// @param numValues  number of leaf values in the page (rows × k when clean)
    /// @param numRows    number of top-level rows in the page (`DataPageV2.num_rows`),
    ///                   or [#ROWS_UNKNOWN] when the header carries no row count
    /// @param maxRepetitionLevel the column's maximum repetition level
    /// @param maxDefinitionLevel the column's maximum definition level
    /// @return [FixedSizeListShape.CleanFixedK] with the detected `k`, or
    ///         [FixedSizeListShape#NOT_APPLICABLE]
    public static FixedSizeListShape detect(
            byte[] repData, int repOffset, int repLength,
            byte[] defData, int defOffset, int defLength,
            int numValues, int numRows,
            int maxRepetitionLevel, int maxDefinitionLevel) {

        if (maxRepetitionLevel != MAX_REPETITION_LEVEL
                || maxDefinitionLevel != MAX_DEFINITION_LEVEL) {
            return FixedSizeListShape.NOT_APPLICABLE;
        }
        if (numValues <= 0 || repLength <= 0 || numRows == 0) {
            return FixedSizeListShape.NOT_APPLICABLE;
        }

        if (!definitionLevelsAllPresent(defData, defOffset, defLength, numValues)) {
            return FixedSizeListShape.NOT_APPLICABLE;
        }

        return repetitionShape(repData, repOffset, repLength, numValues, numRows);
    }

    /// The def-gate: accept iff the whole stream is a single RLE run of `maxDef`.
    private static boolean definitionLevelsAllPresent(
            byte[] defData, int defOffset, int defLength, int numValues) {
        int bitWidth = bitWidth(MAX_DEFINITION_LEVEL);
        RleBitPackingHybridDecoder decoder =
                new RleBitPackingHybridDecoder(defData, defOffset, defLength, bitWidth);
        return decoder.isSingleRleRunOf(MAX_DEFINITION_LEVEL, numValues);
    }

    /// Walks the width-1 repetition stream run by run, verifying uniform gaps
    /// between boundaries without expanding interior runs.
    private static FixedSizeListShape repetitionShape(
            byte[] data, int offset, int length, int numValues, int numRows) {
        // Small-k regime: the whole stream is one bit-packed run and cleanliness
        // reduces to a bulk compare against the tiled period. Decisive when it
        // applies; null defers the large-k / multi-run cases to the scalar walk.
        FixedSizeListShape tiled = tryTiledBitPacked(data, offset, length, numValues, numRows);
        if (tiled != null) {
            return tiled;
        }

        int pos = offset;
        int end = offset + length;

        int idx = 0;         // logical value index consumed so far
        int lastStart = 0;   // index at which the current row started (a boundary)
        int k = -1;          // established row length; -1 until the first row closes

        while (idx < numValues) {
            if (pos >= end) {
                return FixedSizeListShape.NOT_APPLICABLE; // stream truncated
            }

            long header = 0;
            int shift = 0;
            while (pos < end) {
                int b = data[pos++] & 0xFF;
                header |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    break;
                }
                shift += 7;
            }

            if ((header & 1) == 1) {
                // Bit-packed run: (header >> 1) groups of 8 values, one byte each.
                int groups = (int) (header >> 1);
                if (groups > end - pos) {
                    return FixedSizeListShape.NOT_APPLICABLE;
                }
                for (int g = 0; g < groups && idx < numValues; g++) {
                    int packed = data[pos++] & 0xFF;
                    for (int bit = 0; bit < 8 && idx < numValues; bit++) {
                        int value = (packed >> bit) & 1;
                        if (value == 0) {
                            k = closeAndCheck(idx, lastStart, k);
                            if (k == FAILED) {
                                return FixedSizeListShape.NOT_APPLICABLE;
                            }
                            lastStart = idx;
                        }
                        else if (idx == 0) {
                            return FixedSizeListShape.NOT_APPLICABLE; // must start on a boundary
                        }
                        idx++;
                    }
                }
            }
            else {
                // RLE run: (header >> 1) copies of a one-byte value.
                int runLen = (int) (header >> 1);
                if (pos >= end || runLen > numValues - idx) {
                    return FixedSizeListShape.NOT_APPLICABLE;
                }
                int value = data[pos++] & 0xFF;
                if (value > MAX_REPETITION_LEVEL) {
                    return FixedSizeListShape.NOT_APPLICABLE;
                }
                if (value == 1) {
                    if (idx == 0) {
                        return FixedSizeListShape.NOT_APPLICABLE; // must start on a boundary
                    }
                    idx += runLen; // interior skipped in O(1) — never scanned
                }
                else {
                    for (int j = 0; j < runLen; j++) {
                        k = closeAndCheck(idx, lastStart, k);
                        if (k == FAILED) {
                            return FixedSizeListShape.NOT_APPLICABLE;
                        }
                        lastStart = idx;
                        idx++;
                    }
                }
            }
        }

        // Close the final row against the end of the stream.
        k = closeAndCheck(numValues, lastStart, k);
        if (k == FAILED || !rowCountConsistent(numValues, numRows, k)) {
            return FixedSizeListShape.NOT_APPLICABLE;
        }
        return new FixedSizeListShape.CleanFixedK(k);
    }

    private static final int FAILED = -2;

    /// Records a boundary at `idx`: establishes `k` from the first closed gap,
    /// then requires each later gap to match. Returns the (possibly newly set)
    /// `k`, or [#FAILED] on a mismatch. The boundary at index 0 opens the first
    /// row and closes nothing.
    private static int closeAndCheck(int idx, int lastStart, int k) {
        if (idx == 0) {
            return k;
        }
        int gap = idx - lastStart;
        if (k == -1) {
            return gap;
        }
        return gap == k ? k : FAILED;
    }

    /// Fast path for a bit-packed repetition stream — the small-k regime, where
    /// no interior run of `1`s reaches the RLE threshold, so the stream is made
    /// entirely of bit-packed runs (writers chunk them, e.g. arrow-cpp emits
    /// 63-group runs, so there may be many).
    ///
    /// It handles only single-byte-period lists — `k` dividing 8, i.e. `k` in
    /// {2, 4, 8} — where the clean pattern is one repeating byte, so cleanliness
    /// is a bulk compare of each run against that constant word. Multi-byte-period
    /// small `k` (3/5/7) and the RLE-interior large-`k` shape return `null` and
    /// are handled by the scalar walk in [#repetitionShape].
    private static FixedSizeListShape tryTiledBitPacked(
            byte[] data, int offset, int length, int numValues, int numRows) {
        int pos = offset;
        int end = offset + length;
        int idx = 0;       // global value index (a multiple of 8 at each run start)
        int k = 0;         // established once the first bit-packed run is seen
        int refByte = 0;   // the one repeating pattern byte for this k
        long refWord = 0;  // eight copies of refByte, for the word-at-a-time compare

        while (idx < numValues) {
            if (pos >= end) {
                return FixedSizeListShape.NOT_APPLICABLE;
            }
            long header = 0;
            int shift = 0;
            while (pos < end) {
                int b = data[pos++] & 0xFF;
                header |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    break;
                }
                shift += 7;
            }
            if ((header & 1) == 0) {
                // An RLE run: at the very start this is k == 1 or the large-k
                // boundary-then-RLE shape (defer to the scalar walk); later it
                // means the stream is not a clean small-k list.
                return idx == 0 ? null : FixedSizeListShape.NOT_APPLICABLE;
            }
            int groups = (int) (header >> 1);
            if (groups > end - pos) {
                return FixedSizeListShape.NOT_APPLICABLE;
            }
            int payloadPos = pos;

            if (k == 0) {
                if (bitAt(data, payloadPos, 0) != 0) {
                    return FixedSizeListShape.NOT_APPLICABLE; // must open on a boundary
                }
                // The second boundary sits within the first run for k <= 8; look
                // only there. A large-k stream's first run is a tiny boundary
                // group with no second boundary, so k stays 0 and we defer.
                int scanLimit = Math.min(8, Math.min(groups * 8 - 1, numValues - 1));
                for (int p = 1; p <= scanLimit; p++) {
                    if (bitAt(data, payloadPos, p) == 0) {
                        k = p;
                        break;
                    }
                }
                if (k == 0 || 8 % k != 0) {
                    // No second boundary (large-k), or a multi-byte period
                    // (k in {3,5,6,7}): leave both to the scalar walk.
                    return null;
                }
                refByte = patternByte(k);
                refWord = (refByte & 0xFFL) * 0x0101010101010101L;
            }

            int compareBits = Math.min(groups * 8, numValues - idx);
            if (!matchesConstant(data, payloadPos, refByte, refWord, compareBits)) {
                return FixedSizeListShape.NOT_APPLICABLE;
            }
            pos += groups;
            idx += groups * 8;
        }

        if (k == 0 || !rowCountConsistent(numValues, numRows, k)) {
            return FixedSizeListShape.NOT_APPLICABLE;
        }
        return new FixedSizeListShape.CleanFixedK(k);
    }

    private static int bitAt(byte[] data, int byteBase, int bitPos) {
        return (data[byteBase + (bitPos >>> 3)] >> (bitPos & 7)) & 1;
    }

    /// The single repeating byte of the clean fixed-`k` pattern when `k` divides
    /// 8: bit `i` is `0` exactly at a row boundary (`i % k == 0`), `1` otherwise.
    private static int patternByte(int k) {
        int value = 0;
        for (int i = 0; i < 8; i++) {
            if (i % k != 0) {
                value |= 1 << i;
            }
        }
        return value & 0xFF;
    }

    /// Compares `compareBits` bits of a run's payload against a constant pattern
    /// byte tiled across it — a word (eight packed levels) at a time, with the
    /// sub-word remainder and the final padding-masked byte handled scalar. The
    /// constant word carries no loop dependency, so the loop can vectorize.
    private static boolean matchesConstant(byte[] data, int payloadPos, int refByte, long refWord,
                                           int compareBits) {
        int fullBytes = compareBits >>> 3;
        int b = 0;
        for (; b + 8 <= fullBytes; b += 8) {
            if (readLongLE(data, payloadPos + b) != refWord) {
                return false;
            }
        }
        for (; b < fullBytes; b++) {
            if ((data[payloadPos + b] & 0xFF) != refByte) {
                return false;
            }
        }
        int tailBits = compareBits & 7;
        if (tailBits != 0) {
            int mask = (1 << tailBits) - 1;
            if ((data[payloadPos + fullBytes] & mask) != (refByte & mask)) {
                return false;
            }
        }
        return true;
    }

    private static long readLongLE(byte[] data, int off) {
        return (long) LONG_LE.get(data, off);
    }

    /// Confirms the leaf count divides into whole rows of `k`, and — when the
    /// row count is known ([#ROWS_UNKNOWN] otherwise) — that it agrees with the
    /// header's `num_rows`.
    private static boolean rowCountConsistent(int numValues, int numRows, int k) {
        if (k < 1 || numValues % k != 0) {
            return false;
        }
        return numRows < 0 || (long) numRows * k == numValues;
    }

    private static int bitWidth(int maxValue) {
        return 32 - Integer.numberOfLeadingZeros(maxValue);
    }
}
