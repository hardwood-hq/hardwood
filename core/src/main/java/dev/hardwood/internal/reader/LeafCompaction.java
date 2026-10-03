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
    /// The result is sized to `map.length`; the caller owns it outright, except
    /// that a varlength leaf shares `raw`'s bytes (see [#compactBinary]).
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
    /// (nested path); only its `[0, count)` prefix is read. Only the views are
    /// gathered: the result takes over `raw`'s bytes, and `raw` must not be read
    /// afterwards (see [BinaryBatchValues#compact]). A dictionary-encoded leaf carries its chunk
    /// dictionary and gathered entry indices through, so `getStrings()` still reuses
    /// the interned instances and the column reader's dictionary ids describe the
    /// compacted values.
    public static BinaryBatchValues compactBinary(BinaryBatchValues raw, int[] map, int count) {
        return raw.compact(map, count);
    }
}
