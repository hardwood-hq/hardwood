/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.dictionary;

import dev.hardwood.internal.conversion.FixedWidths;
import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.internal.predicate.BinaryComparator;
import dev.hardwood.internal.reader.batch.Dictionary;

/// Shared utilities for evaluating equality / membership predicates against a row group's
/// dictionaries.
///
/// A single-value check scans the dictionary, which is unsorted, so it costs one pass. A
/// membership check runs the kernel its [MembershipIndex] chose when the list was resolved: one
/// pass per literal for a short list, or a single pass that probes the index per entry.
///
/// Each scan is a method of its own holding one loop, so the JIT compiles every kernel's loop on
/// its own terms rather than as one branch of a method holding three.
public final class DictionaryFilterSupport {

    private DictionaryFilterSupport() {
    }

    public static boolean valueAbsent(Dictionary dictionary, int value) {
        if (!(dictionary instanceof Dictionary.IntDictionary dict)) {
            return false;
        }
        return absent(dict.values(), value);
    }

    public static boolean valueAbsent(Dictionary dictionary, long value) {
        if (!(dictionary instanceof Dictionary.LongDictionary dict)) {
            return false;
        }
        return absent(dict.values(), value);
    }

    /// Single-value dictionary check for `FLOAT` values, under the equality `Float.compare`
    /// defines, which every `FLOAT` matcher applies: every `NaN` is one value, and `-0.0f` is apart
    /// from `+0.0f`, so a dictionary holding only `-0.0f` proves `+0.0f` absent.
    ///
    /// A value that is not `NaN` has one bit pattern, and no `NaN` shares it, so such a literal is
    /// found by comparing raw bits, with no per-entry canonicalisation; a `NaN` literal is found by
    /// any `NaN` entry.
    public static boolean valueAbsent(Dictionary dictionary, float value) {
        if (!(dictionary instanceof Dictionary.FloatDictionary dict)) {
            return false;
        }
        return absent(dict.values(), value);
    }

    /// Single-value dictionary check for `DOUBLE` values. See the `FLOAT` overload.
    public static boolean valueAbsent(Dictionary dictionary, double value) {
        if (!(dictionary instanceof Dictionary.DoubleDictionary dict)) {
            return false;
        }
        return absent(dict.values(), value);
    }

    /// Single-value dictionary check for a `FLOAT16` column, whose values are stored as
    /// `FIXED_LEN_BYTE_ARRAY(2)` and so reach the [Dictionary.ByteArrayDictionary] arm.
    ///
    /// Each entry is widened to `float` and compared with [Float#compare], the comparison the
    /// record matcher and the statistics path already apply to this type — so `±0` and `NaN` need
    /// no carve-out here either. Widening the *entries* rather than narrowing the probe is what
    /// makes this exact: narrowing would round a probe that binary16 cannot represent to a
    /// neighbouring value and prove the wrong one absent. A probe with no binary16 representation
    /// simply matches no entry, which is what a full scan would also find.
    ///
    /// Named for the type rather than joining the `valueAbsent` overloads because a `FLOAT16` probe
    /// is a `float` and would collide with the `FLOAT` overload's signature — the same reason
    /// `StatisticsDecoder` carries both `decodeFloat` and `decodeFloat16`. Keeping them apart also
    /// keeps each one's expected dictionary arm explicit (`FloatDictionary` there,
    /// `ByteArrayDictionary` here) instead of resting on `FilterPredicateResolver` never pairing a
    /// probe with the wrong arm — an invariant two layers away whose breach would otherwise show up
    /// as wrong results rather than a compile error.
    public static boolean valueAbsentFloat16(Dictionary dictionary, float value) {
        if (!(dictionary instanceof Dictionary.ByteArrayDictionary dict)) {
            return false;
        }
        return !dict.anyEntry((bytes, from, to) -> to - from == FixedWidths.FLOAT16
                && Float.compare(LogicalTypeConverter.float16At(bytes, from), value) == 0);
    }

    /// Whether every probe of a `FLOAT16` membership test is provably absent, the list form of
    /// [#valueAbsentFloat16] under the same equality. See [MembershipIndex.Float16s].
    public static boolean absentAll(Dictionary dictionary, MembershipIndex.Float16s index) {
        if (!(dictionary instanceof Dictionary.ByteArrayDictionary dict)) {
            return false;
        }
        if (index.nan && !absentNaNFloat16(dict)) {
            return false;
        }
        return index.bitmask == null || absentHalfBits(dict, index.bitmask);
    }

    public static boolean valueAbsent(Dictionary dictionary, byte[] value) {
        if (!(dictionary instanceof Dictionary.ByteArrayDictionary dict)) {
            return false;
        }
        return absent(dict, value);
    }

    public static boolean absentAll(Dictionary dictionary, MembershipIndex.Ints index) {
        if (!(dictionary instanceof Dictionary.IntDictionary dict)) {
            return false;
        }
        int[] entries = dict.values();
        if (index.passes != null) {
            return absentByPasses(entries, index.passes);
        }
        return index.bitmask != null ? absent(entries, index.bitmask) : absent(entries, index.set);
    }

    public static boolean absentAll(Dictionary dictionary, MembershipIndex.Longs index) {
        if (!(dictionary instanceof Dictionary.LongDictionary dict)) {
            return false;
        }
        long[] entries = dict.values();
        if (index.passes != null) {
            return absentByPasses(entries, index.passes);
        }
        return index.bitmask != null ? absent(entries, index.bitmask) : absent(entries, index.set);
    }

    public static boolean absentAll(Dictionary dictionary, MembershipIndex.Binaries index) {
        if (!(dictionary instanceof Dictionary.ByteArrayDictionary dict)) {
            return false;
        }
        return index.passes != null ? absentByPasses(dict, index.passes) : absent(dict, index.set);
    }

    /// `IN`-list dictionary check for `FLOAT` values, under the equality of the single-value check:
    /// a `NaN` literal is decided by one scan for any `NaN` entry, the other literals by their raw
    /// bits, in passes or as a [LongSet].
    public static boolean absentAll(Dictionary dictionary, MembershipIndex.Floats index) {
        if (!(dictionary instanceof Dictionary.FloatDictionary dict)) {
            return false;
        }
        float[] entries = dict.values();
        if (index.passes != null) {
            return absentByPasses(entries, index.passes);
        }
        if (index.nan && !absentNaN(entries)) {
            return false;
        }
        return index.set == null || absentRawBits(entries, index.set);
    }

    /// `IN`-list dictionary check for `DOUBLE` values. See the `FLOAT` overload.
    public static boolean absentAll(Dictionary dictionary, MembershipIndex.Doubles index) {
        if (!(dictionary instanceof Dictionary.DoubleDictionary dict)) {
            return false;
        }
        double[] entries = dict.values();
        if (index.passes != null) {
            return absentByPasses(entries, index.passes);
        }
        if (index.nan && !absentNaN(entries)) {
            return false;
        }
        return index.set == null || absentRawBits(entries, index.set);
    }

    /// The raw bits of the `FLOAT16` entry at `from`, stored little-endian.
    private static int halfBitsAt(byte[] bytes, int from) {
        return (bytes[from] & 0xFF) | (bytes[from + 1] & 0xFF) << 8;
    }

    // One pass per literal.

    private static boolean absentByPasses(int[] entries, int[] values) {
        for (int value : values) {
            if (!absent(entries, value)) {
                return false;
            }
        }
        return true;
    }

    private static boolean absentByPasses(long[] entries, long[] values) {
        for (long value : values) {
            if (!absent(entries, value)) {
                return false;
            }
        }
        return true;
    }

    private static boolean absentByPasses(float[] entries, float[] values) {
        for (float value : values) {
            if (!absent(entries, value)) {
                return false;
            }
        }
        return true;
    }

    private static boolean absentByPasses(double[] entries, double[] values) {
        for (double value : values) {
            if (!absent(entries, value)) {
                return false;
            }
        }
        return true;
    }

    private static boolean absentByPasses(Dictionary.ByteArrayDictionary dict, byte[][] values) {
        for (byte[] value : values) {
            if (!absent(dict, value)) {
                return false;
            }
        }
        return true;
    }

    // One pass, one value.

    private static boolean absent(int[] entries, int value) {
        for (int entry : entries) {
            if (entry == value) {
                return false;
            }
        }
        return true;
    }

    private static boolean absent(long[] entries, long value) {
        for (long entry : entries) {
            if (entry == value) {
                return false;
            }
        }
        return true;
    }

    private static boolean absent(float[] entries, float value) {
        return Float.isNaN(value) ? absentNaN(entries) : absentRawBits(entries, Float.floatToRawIntBits(value));
    }

    private static boolean absent(double[] entries, double value) {
        return Double.isNaN(value) ? absentNaN(entries) : absentRawBits(entries, Double.doubleToRawLongBits(value));
    }

    private static boolean absentNaN(float[] entries) {
        for (float entry : entries) {
            if (Float.isNaN(entry)) {
                return false;
            }
        }
        return true;
    }

    private static boolean absentNaN(double[] entries) {
        for (double entry : entries) {
            if (Double.isNaN(entry)) {
                return false;
            }
        }
        return true;
    }

    private static boolean absentRawBits(float[] entries, int bits) {
        for (float entry : entries) {
            if (Float.floatToRawIntBits(entry) == bits) {
                return false;
            }
        }
        return true;
    }

    private static boolean absentRawBits(double[] entries, long bits) {
        for (double entry : entries) {
            if (Double.doubleToRawLongBits(entry) == bits) {
                return false;
            }
        }
        return true;
    }

    private static boolean absentNaNFloat16(Dictionary.ByteArrayDictionary dict) {
        for (int c = 0; c < dict.chunkCount(); c++) {
            byte[] bytes = dict.chunkBytes(c);
            int[] offsets = dict.chunkOffsets(c);
            for (int k = 0; k < offsets.length - 1; k++) {
                // A half is NaN when its exponent bits are all set and its mantissa is not zero.
                if (offsets[k + 1] - offsets[k] == FixedWidths.FLOAT16 && (halfBitsAt(bytes, offsets[k]) & 0x7FFF) > 0x7C00) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean absent(Dictionary.ByteArrayDictionary dict, byte[] value) {
        for (int c = 0; c < dict.chunkCount(); c++) {
            byte[] bytes = dict.chunkBytes(c);
            int[] offsets = dict.chunkOffsets(c);
            for (int k = 0; k < offsets.length - 1; k++) {
                if (BinaryComparator.sliceEquals(bytes, offsets[k], offsets[k + 1], value)) {
                    return false;
                }
            }
        }
        return true;
    }

    // One pass, one probe per entry into an index over the literals.

    private static boolean absent(int[] entries, RangeBitmask bitmask) {
        for (int entry : entries) {
            if (bitmask.contains(entry)) {
                return false;
            }
        }
        return true;
    }

    private static boolean absent(long[] entries, RangeBitmask bitmask) {
        for (long entry : entries) {
            if (bitmask.contains(entry)) {
                return false;
            }
        }
        return true;
    }

    private static boolean absent(int[] entries, LongSet set) {
        for (int entry : entries) {
            if (set.contains(entry)) {
                return false;
            }
        }
        return true;
    }

    private static boolean absent(long[] entries, LongSet set) {
        for (long entry : entries) {
            if (set.contains(entry)) {
                return false;
            }
        }
        return true;
    }

    private static boolean absentRawBits(float[] entries, LongSet set) {
        for (float entry : entries) {
            if (set.contains(Float.floatToRawIntBits(entry))) {
                return false;
            }
        }
        return true;
    }

    private static boolean absentRawBits(double[] entries, LongSet set) {
        for (double entry : entries) {
            if (set.contains(Double.doubleToRawLongBits(entry))) {
                return false;
            }
        }
        return true;
    }

    private static boolean absentHalfBits(Dictionary.ByteArrayDictionary dict, RangeBitmask bitmask) {
        for (int c = 0; c < dict.chunkCount(); c++) {
            byte[] bytes = dict.chunkBytes(c);
            int[] offsets = dict.chunkOffsets(c);
            for (int k = 0; k < offsets.length - 1; k++) {
                if (offsets[k + 1] - offsets[k] == FixedWidths.FLOAT16 && bitmask.contains(halfBitsAt(bytes, offsets[k]))) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean absent(Dictionary.ByteArrayDictionary dict, BinarySet set) {
        for (int c = 0; c < dict.chunkCount(); c++) {
            byte[] bytes = dict.chunkBytes(c);
            int[] offsets = dict.chunkOffsets(c);
            for (int k = 0; k < offsets.length - 1; k++) {
                if (set.contains(bytes, offsets[k], offsets[k + 1])) {
                    return false;
                }
            }
        }
        return true;
    }
}
