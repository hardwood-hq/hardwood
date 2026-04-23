/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.row;

/// Thrown when a Variant accessor is called on a value whose tagged type
/// does not match what the accessor expects — for example, calling
/// [PqVariant#asInt()] on a Variant holding a STRING.
public class VariantTypeException extends RuntimeException {

    public VariantTypeException(String message) {
        super(message);
    }

    public static VariantTypeException expected(VariantType expected, VariantType actual) {
        return new VariantTypeException("Expected Variant type " + expected + ", got " + actual);
    }

    public static VariantTypeException expectedOneOf(String expected, VariantType actual) {
        return new VariantTypeException("Expected Variant type " + expected + ", got " + actual);
    }
}
