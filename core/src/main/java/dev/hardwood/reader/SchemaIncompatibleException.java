/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

/// Thrown when a file's schema is incompatible with the reference schema during
/// multi-file reading. The reference schema is the schema of the first file
/// in the input list; subsequent files must match it for column projection,
/// physical type, logical type, and repetition type.
public class SchemaIncompatibleException extends RuntimeException {

    public SchemaIncompatibleException(String message) {
        super(message);
    }
}
