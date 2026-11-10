/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;

/// Field names shaped like real column names (one to three lowercase words of two to nine
/// letters, joined by `_`), drawn from a seeded [Random] so every run and every machine sees
/// the same names. Their hash codes spread like those of real schemas, unlike numbered names
/// such as `col_000123`, whose hashes differ only in their low digits.
public final class FieldNames {

    private FieldNames() {
    }

    /// `count` distinct names, the same for the same `count` and `seed`. Each is interned, as a
    /// name written as a literal at the call site is.
    public static String[] random(int count, long seed) {
        Random random = new Random(seed);
        Set<String> names = new LinkedHashSet<>();
        while (names.size() < count) {
            names.add(name(random));
        }
        String[] out = new String[count];
        int i = 0;
        for (String name : names) {
            out[i++] = name.intern();
        }
        return out;
    }

    private static String name(Random random) {
        StringBuilder sb = new StringBuilder();
        int words = 1 + random.nextInt(3);
        for (int w = 0; w < words; w++) {
            if (w > 0) {
                sb.append('_');
            }
            int letters = 2 + random.nextInt(8);
            for (int l = 0; l < letters; l++) {
                sb.append((char) ('a' + random.nextInt(26)));
            }
        }
        return sb.toString();
    }
}
