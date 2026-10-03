/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// Builds [BinaryBatchValues] for tests from the contiguous form: value `i` at
/// `[offsets[i], offsets[i + 1])` of `bytes`.
public final class BinaryBatchValuesFixtures {

    private BinaryBatchValuesFixtures() {
    }

    public static BinaryBatchValues contiguous(byte[] bytes, int[] offsets) {
        int count = offsets.length - 1;
        int[] starts = new int[count];
        int[] ends = new int[count];
        for (int i = 0; i < count; i++) {
            starts[i] = offsets[i];
            ends[i] = offsets[i + 1];
        }
        return new BinaryBatchValues(bytes, starts, ends, offsets[count]);
    }
}
