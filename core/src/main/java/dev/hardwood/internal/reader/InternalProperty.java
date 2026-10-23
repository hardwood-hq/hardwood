/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// The numeric internal properties a read takes its limits from (`hardwood.internal.*`), each read
/// when the read starts rather than cached, so an override holds whenever it is set. A value that
/// is not a number of at least the property's minimum is rejected rather than replaced by the
/// default: a limit that does not take effect produces a plausible read with the wrong shape.
final class InternalProperty {

    private InternalProperty() {
    }

    /// The value of `property` as a `long`, `defaultValue` when it is not set.
    ///
    /// @param requirement what the value must be, as the rejection states it, such as
    ///        `"a positive byte count"`
    /// @throws IllegalArgumentException if the value is not a `long` of at least `min`
    static long longValue(String property, long defaultValue, long min, String requirement) {
        String override = System.getProperty(property);
        if (override == null) {
            return defaultValue;
        }
        long value;
        try {
            value = Long.parseLong(override);
        }
        catch (NumberFormatException e) {
            throw rejected(property, requirement, override, e);
        }
        if (value < min) {
            throw rejected(property, requirement, override, null);
        }
        return value;
    }

    /// As [#longValue], for a property whose value is an `int`.
    static int intValue(String property, int defaultValue, int min, String requirement) {
        String override = System.getProperty(property);
        if (override == null) {
            return defaultValue;
        }
        int value;
        try {
            value = Integer.parseInt(override);
        }
        catch (NumberFormatException e) {
            throw rejected(property, requirement, override, e);
        }
        if (value < min) {
            throw rejected(property, requirement, override, null);
        }
        return value;
    }

    private static IllegalArgumentException rejected(String property, String requirement, String override,
                                                      NumberFormatException cause) {
        return new IllegalArgumentException(property + " must be " + requirement + ", but was: " + override, cause);
    }
}
