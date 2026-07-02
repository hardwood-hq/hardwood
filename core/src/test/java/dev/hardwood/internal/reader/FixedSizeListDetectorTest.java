/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.ByteArrayOutputStream;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.FixedSizeListShape.FixedWidth;

import static org.assertj.core.api.Assertions.assertThat;

/// Tests for [FixedSizeListDetector], the pure level-stream predicate behind the
/// fixed-size-list read fast path. `k` is the fixed list length. Level streams
/// are hand-constructed run by run so both encoding regimes — fully bit-packed
/// (small `k`) and RLE interior (large `k`) — and the boundary edge cases are
/// exercised directly.
class FixedSizeListDetectorTest {

    private static final int REP_BIT_WIDTH = 1;
    private static final int DEF_BIT_WIDTH = 2;

    // --- Small k: the whole repetition stream is a single bit-packed run -----

    @Test
    void detectsSmallKAllBitPacked() {
        // k = 4, 8 rows: pattern 0,1,1,1 repeated -> every byte is 0xEE.
        byte[] rep = bitPacked(repeatVector(4, 8));
        assertThat(detect(rep, 32, 8)).isEqualTo(new FixedWidth(4));
    }

    @Test
    void detectsSmallKWithPaddedFinalGroup() {
        // k = 4, 3 rows -> 12 values; the final bit-packed group is padded with
        // four trailing zeros that must not be read as extra boundaries.
        byte[] rep = bitPacked(repeatVector(4, 3));
        assertThat(detect(rep, 12, 3)).isEqualTo(new FixedWidth(4));
    }

    @Test
    void detectsThresholdKStillBitPacked() {
        // k = 8: the interior run of seven 1s is below the RLE threshold, so the
        // stream stays fully bit-packed.
        byte[] rep = bitPacked(repeatVector(8, 4));
        assertThat(detect(rep, 32, 4)).isEqualTo(new FixedWidth(8));
    }

    // --- Large k: bit-packed boundary group followed by an RLE run of 1s -----

    @Test
    void detectsLargeKWithRleInterior() {
        // k = 768, 2 rows. Boundary group holds the 0 and seven 1s; the RLE run
        // carries the remaining 760 ones whose length is read from the header —
        // the interior is never scanned.
        byte[] rep = concat(
                largeVector(768),
                largeVector(768));
        assertThat(detect(rep, 1536, 2)).isEqualTo(new FixedWidth(768));
    }

    @Test
    void detectsLargeKWithWiderBoundarySplit() {
        // Same k = 768 but a different, encoder-dependent split: two bit-packed
        // boundary groups (0 plus fifteen 1s) then an RLE run of 752 ones. The
        // detector must locate the boundary structurally, not assume a split.
        byte[] oneRow = concat(
                bitPacked(new int[] {0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
                rle(1, 768 - 16));
        byte[] rep = concat(oneRow, oneRow);
        assertThat(detect(rep, 1536, 2)).isEqualTo(new FixedWidth(768));
    }

    @Test
    void detectsLargeKWithNonUniformEncoderSplit() {
        // Row 0 uses an eight-value boundary group, row 1 a sixteen-value one, both
        // encoding a clean k=768 row. The stride tiling requires byte-identical rows
        // so it defers; the scalar fallback still recognises the shape.
        byte[] wideRow = concat(
                bitPacked(new int[] {0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
                rle(1, 768 - 16));
        byte[] rep = concat(largeVector(768), wideRow);
        assertThat(detect(rep, 1536, 2)).isEqualTo(new FixedWidth(768));
    }

    @Test
    void rejectsLargeKWithVaryingRowLength() {
        // Two large rows of different lengths (768 then 760): not fixed-width. The
        // stride derived from row 0 fails the value-count check, so the tiling
        // defers and the scalar fallback rejects the non-uniform gaps.
        byte[] rep = concat(largeVector(768), largeVector(760));
        assertThat(detect(rep, 768 + 760, 2)).isEqualTo(FixedSizeListShape.NOT_APPLICABLE);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {3, 5, 6, 7})
    void detectsMultiBytePeriodBitPacked(int k) {
        // k values whose tiled period is 3/5/7 bytes, with a row count chosen so
        // the payload spans a full word, trailing sub-word bytes, and a
        // padding-masked tail — exercising every branch of the tiled compare.
        int rows = 200 + k;
        int numValues = k * rows;
        byte[] rep = bitPacked(repeatVector(k, rows));
        assertThat(detect(rep, numValues, rows)).isEqualTo(new FixedWidth(k));
    }

    @Test
    void rejectsCorruptionInBitPackedInterior() {
        // A fixed-width k=4 stream with one interior element turned into a boundary:
        // the tiled compare must reject it rather than accept a wrong shape.
        int[] levels = repeatVector(4, 64);
        levels[130] = 0; // was an interior 1; now an extra row start
        byte[] rep = bitPacked(levels);
        assertThat(detect(rep, 256, 64)).isEqualTo(FixedSizeListShape.NOT_APPLICABLE);
    }

    @Test
    void detectsKOfOneAsSingleRleRunOfZeros() {
        // k = 1: every value starts its own row, so the rep stream is one RLE run
        // of zeros.
        byte[] rep = rle(0, 5);
        assertThat(detect(rep, 5, 5)).isEqualTo(new FixedWidth(1));
    }

    @Test
    void detectsSingleRowVector() {
        byte[] rep = largeVector(768);
        assertThat(detect(rep, 768, 1)).isEqualTo(new FixedWidth(768));
    }

    // --- Definition-level gate rejections -----------------------------------

    @Test
    void rejectsWhenDefinitionLevelsCarryNulls() {
        // A null element drops one def below maxDef, so the def stream is no
        // longer a single RLE run of 2 — fall back even though rep looks fixed-width.
        byte[] rep = bitPacked(repeatVector(4, 2));
        byte[] def = bitPacked(new int[] {2, 2, 2, 0, 2, 2, 2, 2});
        assertThat(FixedSizeListDetector.detect(
                rep, 0, rep.length, def, 0, def.length, 8, 2, 1, 2))
                .isEqualTo(FixedSizeListShape.NOT_APPLICABLE);
    }

    @Test
    void rejectsWhenDefinitionLevelsAreAllEmptyLists() {
        // def == 1 everywhere (present but empty list): not maxDef.
        byte[] rep = bitPacked(repeatVector(4, 2));
        byte[] def = rle(1, 8, DEF_BIT_WIDTH);
        assertThat(FixedSizeListDetector.detect(
                rep, 0, rep.length, def, 0, def.length, 8, 2, 1, 2))
                .isEqualTo(FixedSizeListShape.NOT_APPLICABLE);
    }

    // --- maxDef == 1: required list of required elements ---------------------

    @Test
    void detectsRequiredListMaxDefOne() {
        // `required group (LIST) { repeated element }`: maxDef == 1, every leaf
        // present as a single RLE run of def == 1 (bit width 1).
        byte[] rep = bitPacked(repeatVector(4, 8));
        byte[] def = rle(1, 32, 1);
        assertThat(FixedSizeListDetector.detect(
                rep, 0, rep.length, def, 0, def.length, 32, 8, 1, 1))
                .isEqualTo(new FixedWidth(4));
    }

    @Test
    void rejectsRequiredListWithEmptyList() {
        // maxDef == 1 but a def == 0 marks an empty list, so not every leaf is
        // present: the def gate rejects the bit-packed run.
        byte[] rep = bitPacked(repeatVector(4, 2));
        byte[] def = bitPacked(new int[] {1, 1, 1, 0, 1, 1, 1, 1});
        assertThat(FixedSizeListDetector.detect(
                rep, 0, rep.length, def, 0, def.length, 8, 2, 1, 1))
                .isEqualTo(FixedSizeListShape.NOT_APPLICABLE);
    }

    // --- Repetition-level rejections ----------------------------------------

    @Test
    void rejectsVariableInnerLengthsSummingToMultiple() {
        // Two rows of lengths 3 and 5 sum to 8 = 2 x 4, but are not uniform. The
        // per-gap check rejects them instead of trusting numValues / numRows.
        byte[] rep = bitPacked(new int[] {0, 1, 1, 0, 1, 1, 1, 1});
        assertThat(detect(rep, 8, 2)).isEqualTo(FixedSizeListShape.NOT_APPLICABLE);
    }

    @Test
    void rejectsStreamNotStartingOnBoundary() {
        byte[] rep = bitPacked(new int[] {1, 0, 1, 1, 0, 1, 1, 1});
        assertThat(detect(rep, 8, 2)).isEqualTo(FixedSizeListShape.NOT_APPLICABLE);
    }

    @Test
    void rejectsRowCountMismatch() {
        // Fixed-width k = 4 over 8 rows, but the header claims 7 rows.
        byte[] rep = bitPacked(repeatVector(4, 8));
        assertThat(detect(rep, 32, 7)).isEqualTo(FixedSizeListShape.NOT_APPLICABLE);
    }

    @Test
    void rejectsInvalidRepetitionValue() {
        // An RLE run of value 2 is impossible for maxRep = 1.
        byte[] rep = concat(rle(0, 1), rle(2, 3));
        assertThat(detect(rep, 4, 1)).isEqualTo(FixedSizeListShape.NOT_APPLICABLE);
    }

    // --- Shape / bounds guards ----------------------------------------------

    @Test
    void rejectsNonListLevelGeometry() {
        byte[] rep = bitPacked(repeatVector(4, 8));
        byte[] def = allPresentDef(32);
        assertThat(FixedSizeListDetector.detect(
                rep, 0, rep.length, def, 0, def.length, 32, 8, 2, 3))
                .isEqualTo(FixedSizeListShape.NOT_APPLICABLE);
    }

    @Test
    void rejectsEmptyPage() {
        assertThat(detect(new byte[0], 0, 0)).isEqualTo(FixedSizeListShape.NOT_APPLICABLE);
    }

    @Test
    void rejectsBitPackedRunClaimingZeroGroups() {
        // Malformed rep stream: a bit-packed run header of (0 groups << 1) | 1
        // with no payload. The guard rejects it instead of reading past the run.
        byte[] rep = new byte[] {0x01};
        assertThat(detect(rep, 8, 2)).isEqualTo(FixedSizeListShape.NOT_APPLICABLE);
    }

    // --- Helpers ------------------------------------------------------------

    /// Runs the detector with an all-present definition stream sized to
    /// `numValues`, so tests that vary only the repetition stream stay terse.
    private static FixedSizeListShape detect(byte[] rep, int numValues, int numRows) {
        byte[] def = allPresentDef(numValues);
        return FixedSizeListDetector.detect(
                rep, 0, rep.length, def, 0, def.length, numValues, numRows, 1, 2);
    }

    private static int[] repeatVector(int k, int rows) {
        int[] levels = new int[k * rows];
        for (int row = 0; row < rows; row++) {
            levels[row * k] = 0;
            for (int i = 1; i < k; i++) {
                levels[row * k + i] = 1;
            }
        }
        return levels;
    }

    /// A large-k vector as writers emit it: an eight-value bit-packed boundary
    /// group (the 0 and seven 1s) then an RLE run holding the remaining ones.
    private static byte[] largeVector(int k) {
        return concat(
                bitPacked(new int[] {0, 1, 1, 1, 1, 1, 1, 1}),
                rle(1, k - 8));
    }

    private static byte[] allPresentDef(int numValues) {
        return rle(2, numValues, DEF_BIT_WIDTH);
    }

    private static byte[] bitPacked(int[] values) {
        int groups = (values.length + 7) / 8;
        int padded = groups * 8;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeUnsignedVarInt(out, ((long) groups << 1) | 1L);

        long bits = 0;
        int bitsInBuffer = 0;
        for (int i = 0; i < padded; i++) {
            int value = i < values.length ? values[i] : 0;
            bits |= ((long) value) << bitsInBuffer;
            bitsInBuffer += REP_BIT_WIDTH;
            while (bitsInBuffer >= 8) {
                out.write((int) (bits & 0xFF));
                bits >>>= 8;
                bitsInBuffer -= 8;
            }
        }
        if (bitsInBuffer > 0) {
            out.write((int) (bits & 0xFF));
        }
        return out.toByteArray();
    }

    private static byte[] rle(int value, int count) {
        return rle(value, count, REP_BIT_WIDTH);
    }

    private static byte[] rle(int value, int count, int bitWidth) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeUnsignedVarInt(out, (long) count << 1);
        int bytesNeeded = (bitWidth + 7) / 8;
        for (int i = 0; i < bytesNeeded; i++) {
            out.write((value >> (i * 8)) & 0xFF);
        }
        return out.toByteArray();
    }

    private static byte[] concat(byte[]... parts) {
        int length = 0;
        for (byte[] part : parts) {
            length += part.length;
        }
        byte[] out = new byte[length];
        int pos = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, out, pos, part.length);
            pos += part.length;
        }
        return out;
    }

    private static void writeUnsignedVarInt(ByteArrayOutputStream out, long value) {
        while ((value & ~0x7FL) != 0) {
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((int) value);
    }
}
