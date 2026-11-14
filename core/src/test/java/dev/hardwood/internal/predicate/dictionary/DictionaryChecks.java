/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.dictionary;

import dev.hardwood.internal.reader.batch.Dictionary;

/// The dictionary membership check for tests that hold literal arrays, building each list's
/// [MembershipIndex] for the one call as resolution would.
public final class DictionaryChecks {

    private DictionaryChecks() {
    }

    public static boolean absentAll(Dictionary dictionary, int[] values) {
        return DictionaryFilterSupport.absentAll(dictionary, MembershipIndex.Ints.of(values));
    }

    public static boolean absentAll(Dictionary dictionary, long[] values) {
        return DictionaryFilterSupport.absentAll(dictionary, MembershipIndex.Longs.of(values));
    }

    public static boolean absentAll(Dictionary dictionary, float[] values) {
        return DictionaryFilterSupport.absentAll(dictionary, MembershipIndex.Floats.of(values));
    }

    public static boolean absentAll(Dictionary dictionary, double[] values) {
        return DictionaryFilterSupport.absentAll(dictionary, MembershipIndex.Doubles.of(values));
    }

    public static boolean absentAll(Dictionary dictionary, byte[][] values) {
        return DictionaryFilterSupport.absentAll(dictionary, MembershipIndex.Binaries.of(values));
    }

    public static boolean absentAllFloat16(Dictionary dictionary, float[] values) {
        return DictionaryFilterSupport.absentAll(dictionary, MembershipIndex.Float16s.of(values));
    }
}
