/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;

/// Which columns hold text: a `BYTE_ARRAY` annotated `STRING`, `ENUM` or `JSON`, and one
/// carrying no annotation at all, written before the annotation existed.
///
/// Two contracts rest on this one question and have to give the same answer. `getString`
/// reads exactly these columns, and a `String` is the filter literal for exactly these
/// columns: a literal is a value the column's accessors return, so a column a `String`
/// cannot be read out of is a column a `String` cannot filter either.
///
/// Each annotation's constant in [AnnotationKind] states whether it is text, so an annotation
/// added later has to state whether a `String` reads it.
public final class TextColumns {

    private TextColumns() {
    }

    /// Whether `getString` reads this column, and a `String` is its filter literal.
    ///
    /// An annotation only a wider physical type can carry never reaches here: `FileSchema`
    /// drops `STRING`, `ENUM` and `JSON` off anything but a `BYTE_ARRAY` when the footer is
    /// read, leaving the column unannotated.
    public static boolean holdsText(PhysicalType type, LogicalType logicalType) {
        return type == PhysicalType.BYTE_ARRAY && (logicalType == null || isText(logicalType));
    }

    /// Whether the column is annotated as text, which [#holdsText] widens by the unannotated
    /// `BYTE_ARRAY`. A reader that hands back a value of the column's own type decodes these
    /// columns to a `String`, and the unannotated one to its stored bytes.
    public static boolean isAnnotatedText(PhysicalType type, LogicalType logicalType) {
        return type == PhysicalType.BYTE_ARRAY && logicalType != null && isText(logicalType);
    }

    /// Whether the annotation says the stored bytes are the UTF-8 encoding of a string, as
    /// [AnnotationKind#holdsText] states it per annotation.
    private static boolean isText(LogicalType logicalType) {
        return AnnotationKind.of(logicalType).holdsText();
    }
}
