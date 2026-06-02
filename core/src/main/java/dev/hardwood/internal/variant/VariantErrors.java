/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.variant;

import dev.hardwood.row.VariantType;
import dev.hardwood.row.VariantTypeException;

/// Builds [VariantTypeException]s with a consistent message for variant type
/// mismatches. Internal: the message-formatting helpers were factored out of the
/// public [VariantTypeException] so they do not freeze into the 1.0 surface.
final class VariantErrors {

    private VariantErrors() {
    }

    static VariantTypeException expected(VariantType expected, VariantType actual) {
        return new VariantTypeException("Expected Variant type " + expected + ", got " + actual);
    }

    static VariantTypeException expectedOneOf(String expected, VariantType actual) {
        return new VariantTypeException("Expected Variant type " + expected + ", got " + actual);
    }
}
