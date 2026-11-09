/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

/// Whether a floating-point `IN` list holds a `NaN` literal, which each pruning source treats
/// apart from the list's other literals.
public final class NaNLiterals {

    private NaNLiterals() {
    }

    public static boolean containsNaN(float[] values) {
        for (float value : values) {
            if (Float.isNaN(value)) {
                return true;
            }
        }
        return false;
    }

    public static boolean containsNaN(double[] values) {
        for (double value : values) {
            if (Double.isNaN(value)) {
                return true;
            }
        }
        return false;
    }
}
