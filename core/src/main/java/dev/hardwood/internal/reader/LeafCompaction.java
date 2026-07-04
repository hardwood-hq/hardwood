/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// Gathers a nested batch's real-items-only leaf values from its raw
/// (phantom-including) value array, using a real-leaf → raw-position map. Shared
/// by the drain — which pre-compacts on the [dev.hardwood.reader.ColumnReader]
/// path so the scan lands on an idle thread — and the consumer fallback for
/// batches derived by record selection.
public final class LeafCompaction {

    private LeafCompaction() {
    }

    /// Returns a fresh array holding `raw[map[i]]` for each real-leaf index `i`.
    /// The result is sized to `map.length`; the caller owns it outright.
    public static Object compact(Object raw, int[] map) {
        int n = map.length;
        return switch (raw) {
            case int[] a -> {
                int[] out = new int[n];
                for (int i = 0; i < n; i++) {
                    out[i] = a[map[i]];
                }
                yield out;
            }
            case long[] a -> {
                long[] out = new long[n];
                for (int i = 0; i < n; i++) {
                    out[i] = a[map[i]];
                }
                yield out;
            }
            case float[] a -> {
                float[] out = new float[n];
                for (int i = 0; i < n; i++) {
                    out[i] = a[map[i]];
                }
                yield out;
            }
            case double[] a -> {
                double[] out = new double[n];
                for (int i = 0; i < n; i++) {
                    out[i] = a[map[i]];
                }
                yield out;
            }
            case boolean[] a -> {
                boolean[] out = new boolean[n];
                for (int i = 0; i < n; i++) {
                    out[i] = a[map[i]];
                }
                yield out;
            }
            case BinaryBatchValues bbv -> compactBinary(bbv, map, n);
            default -> throw new IllegalStateException("Unexpected leaf array type: " + raw.getClass());
        };
    }

    /// Compacts a varlength leaf to the records at `map[0..count)`. `map` may be
    /// an oversized reusable buffer (flat in-place path) or an exact gather index
    /// (nested path); only its `[0, count)` prefix is read. A dictionary-encoded
    /// string leaf carries its chunk dictionary and gathered entry indices through,
    /// so `getStrings()` still reuses the interned instances.
    public static BinaryBatchValues compactBinary(BinaryBatchValues raw, int[] map, int count) {
        int totalBytes = 0;
        int[] outOffsets = new int[count + 1];
        for (int i = 0; i < count; i++) {
            int rawIdx = map[i];
            int len = raw.offsets[rawIdx + 1] - raw.offsets[rawIdx];
            outOffsets[i] = totalBytes;
            totalBytes += len;
        }
        outOffsets[count] = totalBytes;
        byte[] outBytes = new byte[totalBytes];
        boolean interned = raw.dictionary != null;
        int[] outDictIndices = interned ? new int[count] : null;
        for (int i = 0; i < count; i++) {
            int rawIdx = map[i];
            int rawStart = raw.offsets[rawIdx];
            int len = raw.offsets[rawIdx + 1] - rawStart;
            System.arraycopy(raw.bytes, rawStart, outBytes, outOffsets[i], len);
            if (interned) {
                outDictIndices[i] = raw.dictIndices[rawIdx];
            }
        }
        BinaryBatchValues out = new BinaryBatchValues(outBytes, outOffsets);
        if (interned) {
            out.dictionary = raw.dictionary;
            out.dictIndices = outDictIndices;
        }
        return out;
    }
}
