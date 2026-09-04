/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.dictionary;

import java.util.Arrays;

import dev.hardwood.internal.predicate.BinaryComparator;
import dev.hardwood.internal.predicate.StatisticsDecoder;
import dev.hardwood.internal.reader.Dictionary;

/// Shared utilities for evaluating equality / membership predicates against a row group's
/// dictionaries.
///
/// A single-value check scans the dictionary, which is unsorted, so it costs one pass. A
/// membership check indexes the predicate's values instead of pairing them off against every
/// entry: the value list is the smaller side, and sorting it once turns the pass over the
/// dictionary into a binary search per entry rather than a comparison per (entry, value) pair.
public final class DictionaryFilterSupport {

    /// Width of a `FLOAT16` value as stored: `FIXED_LEN_BYTE_ARRAY(2)`.
    private static final int FLOAT16_BYTES = 2;

    private DictionaryFilterSupport() {
    }

    public static boolean valueAbsent(RowGroupDictionaryFilterSource dictionaries, int columnIndex, int value) {
        if (dictionaries == null
                || !(dictionaries.forColumn(columnIndex) instanceof Dictionary.IntDictionary dict)) {
            return false;
        }

        for (int entry : dict.values()) {
            if (entry == value) {
                return false;
            }
        }

        return true;
    }

    public static boolean valueAbsent(RowGroupDictionaryFilterSource dictionaries, int columnIndex, long value) {
        if (dictionaries == null
                || !(dictionaries.forColumn(columnIndex) instanceof Dictionary.LongDictionary dict)) {
            return false;
        }
        for (long entry : dict.values()) {
            if (entry == value) {
                return false;
            }
        }
        return true;
    }

    /// Single-value dictionary check for `FLOAT` values.
    ///
    /// A dictionary holds the column chunk's exact stored values, so `Float.compare` — the same
    /// total order every `FLOAT` matcher applies — decides membership exactly. `±0` needs no
    /// carve-out: a dictionary holding only `-0.0f` proves `+0.0f` absent.
    public static boolean valueAbsent(RowGroupDictionaryFilterSource dictionaries, int columnIndex, float value) {
        if (dictionaries == null
                || !(dictionaries.forColumn(columnIndex) instanceof Dictionary.FloatDictionary dict)) {
            return false;
        }
        for (float entry : dict.values()) {
            if (Float.compare(entry, value) == 0) {
                return false;
            }
        }
        return true;
    }

    /// Single-value dictionary check for `DOUBLE` values. See the `FLOAT` overload for why `±0` and
    /// `NaN` need no special handling.
    public static boolean valueAbsent(RowGroupDictionaryFilterSource dictionaries, int columnIndex, double value) {
        if (dictionaries == null
                || !(dictionaries.forColumn(columnIndex) instanceof Dictionary.DoubleDictionary dict)) {
            return false;
        }
        for (double entry : dict.values()) {
            if (Double.compare(entry, value) == 0) {
                return false;
            }
        }
        return true;
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
    public static boolean valueAbsentFloat16(RowGroupDictionaryFilterSource dictionaries, int columnIndex,
                                             float value) {
        if (dictionaries == null
                || !(dictionaries.forColumn(columnIndex) instanceof Dictionary.ByteArrayDictionary dict)) {
            return false;
        }
        for (byte[] entry : dict.values()) {
            if (entry.length == FLOAT16_BYTES && Float.compare(StatisticsDecoder.decodeFloat16(entry), value) == 0) {
                return false;
            }
        }
        return true;
    }

    public static boolean valueAbsent(RowGroupDictionaryFilterSource dictionaries, int columnIndex, byte[] value) {
        if (dictionaries == null
                || !(dictionaries.forColumn(columnIndex) instanceof Dictionary.ByteArrayDictionary dict)) {
            return false;
        }
        for (byte[] entry : dict.values()) {
            if (Arrays.equals(entry, value)) {
                return false;
            }
        }
        return true;
    }

    public static boolean absentAll(RowGroupDictionaryFilterSource dictionaries, int columnIndex, int[] values) {
        if (dictionaries == null
                || !(dictionaries.forColumn(columnIndex) instanceof Dictionary.IntDictionary dict)) {
            return false;
        }
        // Sorted copy, not the predicate's own array: the resolved predicate is shared across
        // every row group and file of the read.
        int[] probes = values.clone();
        Arrays.sort(probes);
        for (int entry : dict.values()) {
            if (Arrays.binarySearch(probes, entry) >= 0) {
                return false;
            }
        }
        return true;
    }

    public static boolean absentAll(RowGroupDictionaryFilterSource dictionaries, int columnIndex, long[] values) {
        if (dictionaries == null
                || !(dictionaries.forColumn(columnIndex) instanceof Dictionary.LongDictionary dict)) {
            return false;
        }
        long[] probes = values.clone();
        Arrays.sort(probes);
        for (long entry : dict.values()) {
            if (Arrays.binarySearch(probes, entry) >= 0) {
                return false;
            }
        }
        return true;
    }

    public static boolean absentAll(RowGroupDictionaryFilterSource dictionaries, int columnIndex, byte[][] values) {
        if (dictionaries == null
                || !(dictionaries.forColumn(columnIndex) instanceof Dictionary.ByteArrayDictionary dict)) {
            return false;
        }
        // Unsigned lexicographic order compares equal exactly when the arrays are equal, so the
        // ordering that makes the search possible agrees with the equality the single-value check
        // applies.
        byte[][] probes = values.clone();
        Arrays.sort(probes, BinaryComparator::compareUnsigned);
        for (byte[] entry : dict.values()) {
            if (Arrays.binarySearch(probes, entry, BinaryComparator::compareUnsigned) >= 0) {
                return false;
            }
        }
        return true;
    }

    /// `IN`-list dictionary check for `double` values across DOUBLE and FLOAT columns.
    /// For FLOAT columns, entries from the [Dictionary.FloatDictionary] are widened to `double`
    /// and compared via [Double#compare], preserving total ordering without narrowing probes.
    public static boolean absentAll(RowGroupDictionaryFilterSource dictionaries, int columnIndex,
            double[] values, boolean floatColumn) {
        if (dictionaries == null) {
            return false;
        }
        if (floatColumn) {
            if (!(dictionaries.forColumn(columnIndex) instanceof Dictionary.FloatDictionary dict)) {
                return false;
            }
            for (float entry : dict.values()) {
                double widened = entry;
                for (double probe : values) {
                    if (Double.compare(widened, probe) == 0) {
                        return false;
                    }
                }
            }
            return true;
        }
        if (!(dictionaries.forColumn(columnIndex) instanceof Dictionary.DoubleDictionary dict)) {
            return false;
        }
        for (double entry : dict.values()) {
            for (double probe : values) {
                if (Double.compare(entry, probe) == 0) {
                    return false;
                }
            }
        }
        return true;
    }
}
