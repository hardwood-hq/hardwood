/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Arrays;

/// Shared `toString` rendering for the flyweight types ([dev.hardwood.row.PqStruct] /
/// [dev.hardwood.row.PqList] / [dev.hardwood.row.PqMap]). Renders `byte[]` values via
/// [java.util.Arrays#toString(byte[])] so binary keys, BSON values, and un-annotated
/// `BYTE_ARRAY` columns print as `[1, 2, 3]` instead of `[B@deadbeef`. Other values
/// defer to their own `toString` — nested flyweights recurse naturally.
final class FlyweightFormatter {

    private FlyweightFormatter() {
    }

    static void appendValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof byte[] bytes) {
            sb.append(Arrays.toString(bytes));
        } else {
            sb.append(value);
        }
    }
}
