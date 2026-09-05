/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

/// Thrown when a file cannot be read under the schema the read was planned against.
///
/// In a multi-file read the reference schema is the schema of the first file in the
/// input list. Every column the read touches — the projected ones plus any a filter
/// predicate tests — must exist in each subsequent file, matched by field path, with
/// the same physical type, logical type and repetition type, the same fixed byte
/// length, and enclosing groups of the same nullability and repeatedness. Columns
/// that are neither projected nor filtered on are not checked.
///
/// Also thrown, for a read of any size, when a file's footer is internally
/// inconsistent: the column chunks a row group lists disagree with the schema about
/// which leaf they hold, or a touched `FIXED_LEN_BYTE_ARRAY` column declares no
/// `type_length` — which is optional in the format but required to decode the column —
/// or declares one that is not positive.
public class SchemaIncompatibleException extends RuntimeException {

    public SchemaIncompatibleException(String message) {
        super(message);
    }

    public SchemaIncompatibleException(String message, Throwable cause) {
        super(message, cause);
    }
}
