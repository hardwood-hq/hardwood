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
import java.util.Arrays;

import dev.hardwood.internal.encoding.RleBitPackingHybridDecoder;

/// Recognises the fixed-width fixed-size-list shape from a Parquet data page's
/// repetition/definition level streams alone — on both `DataPageV1` and
/// `DataPageV2` — so the reader can skip the per-element list-reconstruction
/// state machine.
///
/// The detector is a pure predicate: it reads no vector interiors and mutates
/// nothing. It recognises the level geometry of a present, fixed-width list —
/// `maxRepetitionLevel == 1` with `maxDefinitionLevel` of `2` (optional list) or
/// `1` (required list of required elements) — as produced by the 3-level compliant
/// `LIST` encoding
/// `<optional|required> group <name> (LIST) { repeated group list { <primitive> element } }`;
/// any other level geometry yields [FixedSizeListShape#NOT_APPLICABLE]. Being
/// levels-only, it cannot distinguish encodings that share this geometry (e.g. the
/// legacy 2-level list); which columns reach it is the caller's decision (see
/// [PageDecoder]). A `DataPageV1` page
/// carries no row count (passed as [#ROWS_UNKNOWN]): `k` is then taken from the
/// values and checked with `numValues % k == 0` rather than against `num_rows`.
///
/// ## Definition-level gate (O(1))
///
/// Every present, dense vector puts every leaf at `def == maxDef`, which the
/// RLE/bit-packing hybrid stores as a single RLE run of `maxDef` covering all
/// values. The gate accepts only that exact shape; nulls or empty lists produce
/// a lower def value or a bit-packed run and are rejected.
///
/// ## Repetition-level verification
///
/// The rep stream over `{0, 1}` describes list boundaries: a `0` starts a row
/// and a `1` continues it, so a clean fixed-width list is `0` followed by
/// `k - 1` ones, once per row. Verification derives `k` from the first
/// inter-boundary gap and requires every subsequent gap — including the final
/// row, closed against the end of the stream — to equal it. `k` is verified,
/// never assumed: a writer emitting variable inner lengths that happen to sum to
/// a multiple of the row count is rejected by the per-gap check rather than
/// silently mis-decoded.
///
/// Both level streams use Parquet's [RLE / bit-packing hybrid](https://parquet.apache.org/docs/file-format/data-pages/encodings/#RLE)
/// encoding: a sequence of runs, each introduced by a varint header whose low
/// bit selects the kind. An **RLE run** (low bit `0`) is `header >> 1` repeats
/// of one value that follows in `ceil(bitWidth / 8)` bytes; a **bit-packed run**
/// (low bit `1`) is `header >> 1` groups of eight values, each packed in
/// `bitWidth` bits, least-significant-bit first. The width is
/// `ceil(log2(maxLevel + 1))` — one bit for the rep stream (`maxRep == 1`), two
/// for the def stream (`maxDef == 2`) — so a width-1 run stores one byte per
/// bit-packed group and one byte per RLE value. The header bytes in the examples
/// below decode against exactly this layout.
///
/// How that stream is encoded falls into two regimes, each with a bulk fast
/// path, backed by a scalar walk for anything irregular:
///
/// - **Small `k` — one bit-packed run.** For `k <= 8` the interior run of `1`s
///   is at most seven long, below the writer's run-length-8 RLE threshold, so the
///   whole stream is bit-packed. The `0`/`1` pattern is then byte-periodic (period
///   `k / gcd(k, 8)`), and [#tryTiledBitPacked] verifies it with a word-at-a-time
///   compare against that tiled period. Example `k = 4`: header `0x21`
///   (bit-packed, sixteen groups) then payload bytes all `0xee`, which is
///   `0 1 1 1 0 1 1 1` read LSB-first — two rows per byte, a one-byte period.
///
/// - **Large `k` — a per-row RLE stride.** Once `k - 1` reaches the RLE
///   threshold the interior `1`s arrive as one RLE run, so each row is a
///   bit-packed boundary group followed by that run. When the row is byte-aligned
///   this is a single fixed byte stride repeated once per row, which
///   [#tryTiledRle] verifies with one shift-by-stride compare. Example `k = 16`,
///   a 4-byte stride per row: `03 fe 10 01` — `0x03` bit-packed (one group)
///   `0xfe` = `0` then seven `1`s, then RLE run `0x10 0x01` of eight `1`s, i.e.
///   `0` followed by fifteen `1`s.
///
/// - **Scalar fallback.** Whatever both bulk paths decline — an all-bit-packed
///   large `k` (e.g. `9..15`, whose rows are not byte-aligned), the degenerate
///   `k = 1` (an RLE run of `0`s), a non-uniform run split, or a mixed layout — is
///   walked run by run in [#scalarFallback], which assumes nothing about the
///   encoding. A bit-packed run inspects each level to locate the `0`s; an RLE run
///   of `1`s is skipped in O(1) with `idx += len`, since the header already
///   carries the count. Example `k = 9`: rows are nine bits, never byte-aligned,
///   so the layout only repeats every `lcm(8, 9) = 72` values —
///   `11 fe fd fb f7 ef df bf 7f 10 01` per 8-row block: a 64-value bit-packed run
///   (seven full rows plus the opening `0` of the eighth) then an RLE run
///   `0x10 0x01` of eight `1`s closing it.
public final class FixedSizeListDetector {

    private static final int MAX_REPETITION_LEVEL = 1;

    /// Accepted range for the column's max definition level. Both single-level
    /// `LIST` shapes qualify: `1` for a required list of required elements
    /// (`required group (LIST) { repeated element }`) and `2` for an optional
    /// (nullable) list. The actual value drives the def-gate's expected run value
    /// and bit width.
    private static final int MIN_DEFINITION_LEVEL = 1;
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

    /// Precomputed reference tiles for the small-`k` regime, indexed by `k` in
    /// `1..8`. `TILE_PERIOD[k]` is the pattern's byte period (`k / gcd(k, 8)`) and
    /// `TILE_WORDS[k]` holds one 64-bit reference word per phase (a byte offset
    /// modulo the period), each word being the pattern's next 8 bytes little-endian.
    /// The per-byte reference needed for a run's sub-word tail is the low byte of
    /// its phase word, so the byte pattern is not stored separately. There are only
    /// eight cases, so the tiled compare looks these up rather than rebuilding them
    /// per page.
    private static final int[] TILE_PERIOD = new int[9];
    private static final long[][] TILE_WORDS = new long[9][];

    static {
        for (int k = 1; k <= 8; k++) {
            int period = k / gcd(k, 8);
            byte[] pat = new byte[period];
            for (int j = 0; j < period; j++) {
                pat[j] = (byte) patternByte(k, j);
            }
            long[] words = new long[period];
            for (int phase = 0; phase < period; phase++) {
                long word = 0;
                for (int i = 0; i < 8; i++) {
                    word |= (pat[(phase + i) % period] & 0xFFL) << (8 * i);
                }
                words[phase] = word;
            }
            TILE_PERIOD[k] = period;
            TILE_WORDS[k] = words;
        }
    }

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
    /// @param numValues  number of leaf values in the page (rows × k when fixed-width)
    /// @param numRows    number of top-level rows in the page (`DataPageV2.num_rows`),
    ///                   or [#ROWS_UNKNOWN] when the header carries no row count
    /// @param maxRepetitionLevel the column's maximum repetition level
    /// @param maxDefinitionLevel the column's maximum definition level
    /// @return [FixedSizeListShape.FixedWidth] with the detected `k`, or
    ///         [FixedSizeListShape#NOT_APPLICABLE]
    public static FixedSizeListShape detect(
            byte[] repData, int repOffset, int repLength,
            byte[] defData, int defOffset, int defLength,
            int numValues, int numRows,
            int maxRepetitionLevel, int maxDefinitionLevel) {

        if (maxRepetitionLevel != MAX_REPETITION_LEVEL
                || maxDefinitionLevel < MIN_DEFINITION_LEVEL
                || maxDefinitionLevel > MAX_DEFINITION_LEVEL) {
            return FixedSizeListShape.NOT_APPLICABLE;
        }
        if (numValues <= 0 || repLength <= 0 || numRows == 0) {
            return FixedSizeListShape.NOT_APPLICABLE;
        }

        if (!definitionLevelsAllPresent(defData, defOffset, defLength, numValues, maxDefinitionLevel)) {
            return FixedSizeListShape.NOT_APPLICABLE;
        }

        return repetitionShape(repData, repOffset, repLength, numValues, numRows);
    }

    /// The def-gate: accept iff the whole stream is a single RLE run of `maxDef`
    /// covering all values — every leaf present. `maxDef` is `2` for an optional
    /// list and `1` for a required one, setting both the expected run value and
    /// the level bit width.
    private static boolean definitionLevelsAllPresent(
            byte[] defData, int defOffset, int defLength, int numValues, int maxDefinitionLevel) {
        int bitWidth = bitWidth(maxDefinitionLevel);
        RleBitPackingHybridDecoder decoder =
                new RleBitPackingHybridDecoder(defData, defOffset, defLength, bitWidth);
        return decoder.isSingleRleRunOf(maxDefinitionLevel, numValues);
    }

    /// Verifies the width-1 repetition stream, dispatching across the three
    /// encoding regimes: the small-`k` tiled bit-packed compare, the large-`k`
    /// tiled RLE stride, then the scalar run-by-run walk for anything neither bulk
    /// path accepts.
    private static FixedSizeListShape repetitionShape(
            byte[] data, int offset, int length, int numValues, int numRows) {
        // Small-k regime: the whole stream is bit-packed and the fixed-width check
        // reduces to a bulk compare against the tiled byte period. UNDETERMINED
        // defers the large-k / multi-run cases.
        FixedSizeListShape tiled = tryTiledBitPacked(data, offset, length, numValues, numRows);
        if (tiled != FixedSizeListShape.UNDETERMINED) {
            return tiled;
        }

        // Large-k regime: when every row is an identical byte stride (boundary
        // group + RLE interior), verify the stream is that stride repeated with a
        // single bulk compare. UNDETERMINED defers unusual splits to the scalar walk.
        FixedSizeListShape strided = tryTiledRle(data, offset, length, numValues, numRows);
        if (strided != FixedSizeListShape.UNDETERMINED) {
            return strided;
        }

        return scalarFallback(data, offset, length, numValues, numRows);
    }

    /// Fast path for a bit-packed repetition stream — the small-k regime, where
    /// no interior run of `1`s reaches the RLE threshold (`k <= 8`), so the
    /// stream is entirely bit-packed runs (writers chunk them, e.g. arrow-cpp
    /// emits 63-group runs, so there may be many).
    ///
    /// The fixed-width pattern is byte-periodic with period `k / gcd(k, 8)`: one
    /// byte for `k` dividing 8 (`k` in {1,2,4,8}), or 3/5/7 bytes for the
    /// multi-byte periods (`k` in {3,5,6,7}). Each run's payload is compared
    /// against that period tiled across it, a word (eight packed levels) at a
    /// time. For a single-byte period the reference word is a constant; for a
    /// multi-byte period the word is selected from a per-phase reference table,
    /// indexed by the run's byte offset in the stream. The RLE-interior large-`k`
    /// shape (no second boundary within the first run) returns
    /// [FixedSizeListShape#UNDETERMINED], deferring to [#tryTiledRle] and then
    /// [#scalarFallback] in [#repetitionShape].
    private static FixedSizeListShape tryTiledBitPacked(
            byte[] data, int offset, int length, int numValues, int numRows) {
        int pos = offset;
        int end = offset + length;
        int idx = 0;            // global value index (a multiple of 8 at each run start)
        int k = 0;              // established once the first bit-packed run is seen
        int period = 0;         // byte period of the pattern (1 for k | 8, else 3/5/7)
        long[] refWords = null; // reference word per phase (from TILE_WORDS)

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
                // boundary-then-RLE shape (defer to the large-k / scalar paths);
                // later it means the stream is not a fixed-width small-k list.
                return idx == 0 ? FixedSizeListShape.UNDETERMINED : FixedSizeListShape.NOT_APPLICABLE;
            }
            int groups = (int) (header >> 1);
            if (groups <= 0 || groups > end - pos) {
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
                if (k == 0) {
                    return FixedSizeListShape.UNDETERMINED; // no second boundary: large k, defer to the large-k / scalar paths
                }
                period = TILE_PERIOD[k];
                refWords = TILE_WORDS[k];
            }

            int compareBits = Math.min(groups * 8, numValues - idx);
            boolean matches = period == 1
                    ? matchesConstant(data, payloadPos, refWords[0], compareBits)
                    : matchesPattern(data, payloadPos, refWords, period,
                            (idx >> 3) % period, compareBits);
            if (!matches) {
                return FixedSizeListShape.NOT_APPLICABLE;
            }
            pos += groups;
            idx += groups * 8;
        }

        if (k == 0 || !rowCountConsistent(numValues, numRows, k)) {
            return FixedSizeListShape.NOT_APPLICABLE;
        }
        return new FixedSizeListShape.FixedWidth(k);
    }

    /// Fast path for the large-`k` RLE-interior regime, where each row is encoded
    /// as an identical byte stride — a bit-packed boundary group (the lone `0` plus
    /// some leading `1`s) followed by an RLE run carrying the rest of the row's
    /// `1`s — laid out on byte boundaries. The whole rep stream is then that stride
    /// repeated once per row, so after deriving the stride and `k` from the first
    /// row the rest is verified with one bulk byte compare, replacing the per-row
    /// header parsing of the scalar walk.
    ///
    /// Returns [FixedSizeListShape#UNDETERMINED] (defer to the scalar walk) for
    /// anything that is not this clean, byte-aligned per-row stride: `k == 1`, a row
    /// that is not a whole number of bytes (e.g. an all-bit-packed `k` in `9..15`),
    /// a single row, or an encoder that lays rows out non-uniformly.
    private static FixedSizeListShape tryTiledRle(
            byte[] data, int offset, int length, int numValues, int numRows) {
        int end = offset + length;
        int pos = offset;
        int idx = 0;
        int k = -1;
        int strideBytes = -1;

        while (pos < end && k < 0) {
            int runStart = pos;
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
                int groups = (int) (header >> 1);
                if (groups > end - pos) {
                    return FixedSizeListShape.UNDETERMINED;
                }
                for (int g = 0; g < groups && k < 0 && idx < numValues; g++) {
                    int packed = data[pos + g] & 0xFF;
                    for (int bit = 0; bit < 8 && idx < numValues; bit++) {
                        int value = (packed >> bit) & 1;
                        if (idx == 0) {
                            // The stream must open on a boundary at its first bit.
                            if (value != 0) {
                                return FixedSizeListShape.UNDETERMINED;
                            }
                        }
                        else if (value == 0) {
                            // Second boundary — row 1. Its byte offset is the stride,
                            // but only if it lands on a fresh run (byte boundary).
                            if (g != 0 || bit != 0) {
                                return FixedSizeListShape.UNDETERMINED;
                            }
                            k = idx;
                            strideBytes = runStart - offset;
                            break;
                        }
                        idx++;
                    }
                }
                if (k < 0) {
                    pos += groups;
                }
            }
            else {
                int runLen = (int) (header >> 1);
                if (pos >= end || runLen > numValues - idx) {
                    return FixedSizeListShape.UNDETERMINED;
                }
                int value = data[pos++] & 0xFF;
                if (value != 1 || idx == 0) {
                    return FixedSizeListShape.UNDETERMINED; // an opening or 0/>1 RLE run is not the large-k stride
                }
                idx += runLen; // interior 1s
            }
        }

        if (k < 1 || strideBytes < 1 || length % strideBytes != 0) {
            return FixedSizeListShape.UNDETERMINED;
        }
        if ((long) (length / strideBytes) * k != numValues
                || !rowCountConsistent(numValues, numRows, k)) {
            return FixedSizeListShape.UNDETERMINED;
        }
        // The stream must be the stride repeated: byte-periodic with period
        // `strideBytes`, i.e. the stream shifted by one stride equals itself.
        if (!Arrays.equals(data, offset + strideBytes, end, data, offset, end - strideBytes)) {
            return FixedSizeListShape.UNDETERMINED;
        }
        return new FixedSizeListShape.FixedWidth(k);
    }

    private static final int FAILED = -2;

    /// Scalar fallback: walks the repetition stream run by run, verifying uniform
    /// gaps between boundaries without expanding interior runs. Handles whatever
    /// the two bulk paths decline — an all-bit-packed large `k`, the degenerate
    /// `k == 1`, non-uniform run splits, mixed layouts — assuming nothing about
    /// the encoding. Being the terminal path, it returns
    /// [FixedSizeListShape#NOT_APPLICABLE] on failure rather than deferring with
    /// [FixedSizeListShape#UNDETERMINED].
    private static FixedSizeListShape scalarFallback(
            byte[] data, int offset, int length, int numValues, int numRows) {
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
                // The width-1 rep stream packs one bit per value, so a `0` bit is a
                // record boundary. Scan boundaries a 64-bit word (64 values) at a
                // time: the boundary mask is the word's zero bits (`~word`), which
                // `Long.numberOfTrailingZeros` walks in order — skipping the runs of
                // `1`s between boundaries rather than visiting every bit. Only full
                // words (`levelsLeft >= 64`) take this path, so no padding bit is ever
                // read; `levelsLeft`, clamped to `numValues`, is the padding-safe
                // count. The sub-word remainder (and any run shorter than a word)
                // falls to the scalar bit loop below.
                int groups = (int) (header >> 1);
                if (groups > end - pos) {
                    return FixedSizeListShape.NOT_APPLICABLE;
                }
                int levelsLeft = Math.min(groups * 8, numValues - idx);
                while (levelsLeft >= 64) {
                    long boundaries = ~readLongLE(data, pos);
                    if (idx == 0 && (boundaries & 1L) == 0) {
                        return FixedSizeListShape.NOT_APPLICABLE; // must start on a boundary
                    }
                    while (boundaries != 0) {
                        int b = idx + Long.numberOfTrailingZeros(boundaries);
                        k = closeAndCheck(b, lastStart, k);
                        if (k == FAILED) {
                            return FixedSizeListShape.NOT_APPLICABLE;
                        }
                        lastStart = b;
                        boundaries &= boundaries - 1;
                    }
                    idx += 64;
                    pos += 8;
                    levelsLeft -= 64;
                }
                while (levelsLeft > 0) {
                    int packed = data[pos++] & 0xFF;
                    int bits = Math.min(8, levelsLeft);
                    for (int bit = 0; bit < bits; bit++) {
                        if (((packed >> bit) & 1) == 0) {
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
                    levelsLeft -= bits;
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

        return new FixedSizeListShape.FixedWidth(k);
    }

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

    private static int bitAt(byte[] data, int byteBase, int bitPos) {
        return (data[byteBase + (bitPos >>> 3)] >> (bitPos & 7)) & 1;
    }

    /// Byte `byteIndex` of the fixed-width `k`-element pattern: bit `i` is `0`
    /// exactly at a row boundary (`(byteIndex * 8 + i) % k == 0`), `1` otherwise.
    /// The byte sequence is periodic with period `k / gcd(k, 8)`, so a single
    /// byte suffices for `k` dividing 8 and 3/5/7 bytes for the rest.
    private static int patternByte(int k, int byteIndex) {
        int base = byteIndex * 8;
        int value = 0;
        for (int i = 0; i < 8; i++) {
            if ((base + i) % k != 0) {
                value |= 1 << i;
            }
        }
        return value & 0xFF;
    }

    /// Compares `compareBits` bits of a run's payload against a constant pattern
    /// byte tiled across it — a word (eight packed levels) at a time, with the
    /// sub-word remainder and the final padding-masked byte handled scalar. The
    /// constant word carries no loop dependency, so the loop can vectorize. The
    /// single reference byte is `refWord`'s low byte (all eight lanes are equal).
    private static boolean matchesConstant(byte[] data, int payloadPos, long refWord,
                                           int compareBits) {
        int refByte = (int) (refWord & 0xFF);
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

    /// Multi-byte-period counterpart of [#matchesConstant]: compares `compareBits`
    /// bits of a run's payload against the `k`-periodic pattern tiled across it, a
    /// word at a time. `startPhase` is the run's byte offset in the stream modulo
    /// `period` — the phase of its first byte — selecting the first reference word
    /// from `refWords`; each full word advances the phase by 8 bytes. The sub-word
    /// remainder and the final padding-masked byte compare against the phase word's
    /// low byte, which is the pattern byte at that phase.
    private static boolean matchesPattern(byte[] data, int payloadPos, long[] refWords,
                                          int period, int startPhase, int compareBits) {
        int fullBytes = compareBits >>> 3;
        int b = 0;
        int phase = startPhase;
        for (; b + 8 <= fullBytes; b += 8) {
            if (readLongLE(data, payloadPos + b) != refWords[phase]) {
                return false;
            }
            phase = (phase + 8) % period;
        }
        for (; b < fullBytes; b++) {
            if ((data[payloadPos + b] & 0xFF) != (int) (refWords[phase] & 0xFF)) {
                return false;
            }
            phase = phase + 1 == period ? 0 : phase + 1;
        }
        int tailBits = compareBits & 7;
        if (tailBits != 0) {
            int mask = (1 << tailBits) - 1;
            if ((data[payloadPos + fullBytes] & mask) != ((int) (refWords[phase] & 0xFF) & mask)) {
                return false;
            }
        }
        return true;
    }

    private static int gcd(int a, int b) {
        while (b != 0) {
            int t = b;
            b = a % b;
            a = t;
        }
        return a;
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
