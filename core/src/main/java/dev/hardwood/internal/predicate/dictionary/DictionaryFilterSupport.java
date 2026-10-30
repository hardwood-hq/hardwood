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
/// membership check over a short list repeats that pass per literal; every branch of such a pass
/// goes the same way until a match, so the passes stay cheap. A longer list is indexed once, and
/// the single pass over the dictionary probes the index per entry: a [RangeBitmask] where integer
/// literals lie close together, else a [LongSet] over the values' raw bits or a [BinarySet] over
/// byte strings. Integer literals too spread out for a bitmask stay on passes a while longer, since a
/// pass over primitives is cheap next to a hash probe.
///
/// Each scan is a method of its own holding one loop, so the JIT compiles every kernel's loop on
/// its own terms rather than as one branch of a method holding three.
public final class DictionaryFilterSupport {

    /// Literal count up to which a membership check runs one pass per literal. Tuning: #1594.
    static final int PASS_LIMIT = 8;

    /// Literal count up to which an integer membership check whose literals lie too far apart for
    /// a [RangeBitmask] still runs one pass per literal. A pass over primitive integers costs far
    /// less per entry than a hash probe, so the hash set only wins from this count. Tuning: #1594.
    static final int SPARSE_PASS_LIMIT = 16;

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
        byte[] bytes = dict.entryBytes();
        int[] offsets = dict.entryOffsets();
        for (int i = 0; i < dict.size(); i++) {
            int from = offsets[i];
            if (offsets[i + 1] - from == FixedWidths.FLOAT16
                    && Float.compare(LogicalTypeConverter.float16At(bytes, from), value) == 0) {
                return false;
            }
        }
        return true;
    }

    /// Whether every probe of a `FLOAT16` membership test is provably absent, each compared with
    /// the halves the dictionary holds — the list form of [#valueAbsentFloat16].
    public static boolean absentAllFloat16(Dictionary dictionary, float[] values) {
        if (!(dictionary instanceof Dictionary.ByteArrayDictionary dict)) {
            return false;
        }
        byte[] bytes = dict.entryBytes();
        int[] offsets = dict.entryOffsets();
        for (int i = 0; i < dict.size(); i++) {
            int from = offsets[i];
            if (offsets[i + 1] - from != FixedWidths.FLOAT16) {
                continue;
            }
            float stored = LogicalTypeConverter.float16At(bytes, from);
            for (float value : values) {
                if (Float.compare(stored, value) == 0) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean valueAbsent(Dictionary dictionary, byte[] value) {
        if (!(dictionary instanceof Dictionary.ByteArrayDictionary dict)) {
            return false;
        }
        return absent(dict, value);
    }

    public static boolean absentAll(Dictionary dictionary, int[] values) {
        if (!(dictionary instanceof Dictionary.IntDictionary dict)) {
            return false;
        }
        int[] entries = dict.values();
        if (values.length <= PASS_LIMIT) {
            return absentByPasses(entries, values);
        }
        long[] keys = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            keys[i] = values[i];
        }
        RangeBitmask bitmask = RangeBitmask.of(keys);
        if (bitmask != null) {
            return absent(entries, bitmask);
        }
        if (values.length <= SPARSE_PASS_LIMIT) {
            return absentByPasses(entries, values);
        }
        return absent(entries, new LongSet(keys));
    }

    public static boolean absentAll(Dictionary dictionary, long[] values) {
        if (!(dictionary instanceof Dictionary.LongDictionary dict)) {
            return false;
        }
        long[] entries = dict.values();
        if (values.length <= PASS_LIMIT) {
            return absentByPasses(entries, values);
        }
        RangeBitmask bitmask = RangeBitmask.of(values);
        if (bitmask != null) {
            return absent(entries, bitmask);
        }
        if (values.length <= SPARSE_PASS_LIMIT) {
            return absentByPasses(entries, values);
        }
        return absent(entries, new LongSet(values));
    }

    public static boolean absentAll(Dictionary dictionary, byte[][] values) {
        if (!(dictionary instanceof Dictionary.ByteArrayDictionary dict)) {
            return false;
        }
        if (values.length <= PASS_LIMIT) {
            return absentByPasses(dict, values);
        }
        return absent(dict, new BinarySet(values));
    }

    /// `IN`-list dictionary check for `FLOAT` values, under the equality of the single-value check:
    /// a `NaN` literal is decided by one scan for any `NaN` entry, the other literals by their raw
    /// bits, in passes or as a [LongSet].
    public static boolean absentAll(Dictionary dictionary, float[] values) {
        if (!(dictionary instanceof Dictionary.FloatDictionary dict)) {
            return false;
        }
        float[] entries = dict.values();
        if (values.length <= PASS_LIMIT) {
            return absentByPasses(entries, values);
        }
        if (containsNaN(values) && !absentNaN(entries)) {
            return false;
        }
        long[] keys = rawBits(values);
        return keys.length == 0 || absentRawBits(entries, new LongSet(keys));
    }

    /// `IN`-list dictionary check for `DOUBLE` values. See the `float[]` overload.
    public static boolean absentAll(Dictionary dictionary, double[] values) {
        if (!(dictionary instanceof Dictionary.DoubleDictionary dict)) {
            return false;
        }
        double[] entries = dict.values();
        if (values.length <= PASS_LIMIT) {
            return absentByPasses(entries, values);
        }
        if (containsNaN(values) && !absentNaN(entries)) {
            return false;
        }
        long[] keys = rawBits(values);
        return keys.length == 0 || absentRawBits(entries, new LongSet(keys));
    }

    private static boolean containsNaN(float[] values) {
        for (float value : values) {
            if (Float.isNaN(value)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsNaN(double[] values) {
        for (double value : values) {
            if (Double.isNaN(value)) {
                return true;
            }
        }
        return false;
    }

    /// The raw bits of the values that are not `NaN`.
    private static long[] rawBits(float[] values) {
        int count = 0;
        for (float value : values) {
            if (!Float.isNaN(value)) {
                count++;
            }
        }
        long[] keys = new long[count];
        int i = 0;
        for (float value : values) {
            if (!Float.isNaN(value)) {
                keys[i++] = Float.floatToRawIntBits(value);
            }
        }
        return keys;
    }

    /// The raw bits of the values that are not `NaN`.
    private static long[] rawBits(double[] values) {
        int count = 0;
        for (double value : values) {
            if (!Double.isNaN(value)) {
                count++;
            }
        }
        long[] keys = new long[count];
        int i = 0;
        for (double value : values) {
            if (!Double.isNaN(value)) {
                keys[i++] = Double.doubleToRawLongBits(value);
            }
        }
        return keys;
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

    private static boolean absent(Dictionary.ByteArrayDictionary dict, byte[] value) {
        byte[] bytes = dict.entryBytes();
        int[] offsets = dict.entryOffsets();
        for (int i = 0; i < dict.size(); i++) {
            if (BinaryComparator.sliceEquals(bytes, offsets[i], offsets[i + 1], value)) {
                return false;
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

    private static boolean absent(Dictionary.ByteArrayDictionary dict, BinarySet set) {
        byte[] bytes = dict.entryBytes();
        int[] offsets = dict.entryOffsets();
        for (int i = 0; i < dict.size(); i++) {
            if (set.contains(bytes, offsets[i], offsets[i + 1])) {
                return false;
            }
        }
        return true;
    }
}
