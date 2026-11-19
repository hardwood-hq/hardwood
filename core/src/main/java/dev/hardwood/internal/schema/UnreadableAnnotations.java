/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import dev.hardwood.internal.util.Lazy;
import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.schema.FileSchema;

/// Annotations a footer gives a field that cannot carry them, which the reader drops rather
/// than refusing the file: a primitive annotation whose physical type cannot hold it, an
/// annotation only a primitive carries on a group, and a group's `converted_type` that names
/// another structure than its `logicalType`.
///
/// `FLOAT16` is defined as a two-byte payload, so a column annotated `FLOAT16` that declares
/// twelve bytes is invalid, and no reading of it produces the value the annotation promises.
/// The format says to read past the annotation rather than refuse the file: see the
/// "Unsupported Logical Types" section of `LogicalTypes.md`, adopted in parquet-format PR 606,
/// which has readers "ignore both the logical type annotation and column order for that column.
/// Only the physical type information should be used to process the column's data." So the
/// field is reported and read as though the footer had not annotated it.
///
/// The column's statistics are the one consequence that does not follow from the annotation's
/// absence. The writer recorded them in the order of the annotation, which the physical type's
/// order need not agree with, so `BoundsReadability` reads no bounds for such a column. It asks
/// [LeafAnnotation#dropFault] of the footer's schema element, as [#fault] does.
///
/// The sibling case, an annotation this version does not recognize at all, is dropped where it
/// is parsed, in `LogicalTypeReader`. The two differ only in what the warning can tell the
/// reader, because one is a file from a newer writer and the other is a file from a broken one.
///
/// The drop applies to schemas read from a footer only. [FileSchema#fromSchemaElements] refuses
/// such an annotation, so a schema built from elements by hand is never written without one it
/// was given.
public final class UnreadableAnnotations {

    private static final Lazy<System.Logger> LOG = Lazy.of(() -> System.getLogger(UnreadableAnnotations.class.getName()));

    private UnreadableAnnotations() {
    }

    /// Returns `elements` with every annotation [#fault] names cleared, logging one warning that
    /// lists them; `elements` itself when there is none.
    ///
    /// @param elements a footer's schema elements, root first, whose child counts the footer
    ///        reader has checked
    public static List<SchemaElement> drop(List<SchemaElement> elements) {
        if (elements.isEmpty() || elements.get(0).numChildren() == null) {
            return elements;
        }
        List<SchemaElement> result = new ArrayList<>(elements);
        // One line per file rather than one per field: a file from a broken writer can carry many,
        // and they say the same thing about all of them.
        List<String> dropped = new ArrayList<>();
        int[] cursor = { 1 };
        walkChildren(result, cursor, elements.get(0).numChildren(), new ArrayDeque<>(), dropped);
        if (dropped.isEmpty()) {
            return elements;
        }
        LOG.get().log(System.Logger.Level.WARNING,
                "Ignoring {0} annotation(s) their field cannot carry; each such field is read"
                + " as though unannotated: {1}",
                dropped.size(), String.join("; ", dropped));
        return result;
    }

    /// Why the element's field cannot carry its annotation, which [#drop] then clears; `null`
    /// where it carries every annotation it has.
    public static String fault(SchemaElement element) {
        if (element.isPrimitive()) {
            return LeafAnnotation.dropFault(element);
        }
        LogicalType annotation = element.logicalType();
        LogicalType readable = AnnotationPairings.readableGroupAnnotation(annotation);
        if (readable != annotation) {
            return annotation + " annotates a primitive, but the field is a group";
        }
        ConvertedType converted = element.convertedType();
        if (AnnotationPairings.readableGroupConvertedType(readable, converted) != converted) {
            return converted + (AnnotationPairings.annotatesGroup(converted)
                    ? " contradicts the group's " + readable
                    : " annotates a primitive, but the field is a group");
        }
        return null;
    }

    private static void walkChildren(List<SchemaElement> elements, int[] cursor, int numChildren,
                                     Deque<String> path, List<String> dropped) {
        for (int i = 0; i < numChildren; i++) {
            int index = cursor[0]++;
            SchemaElement element = elements.get(index);
            path.addLast(element.name());
            String fault = fault(element);
            if (fault != null) {
                dropped.add(String.join(".", path) + " (" + fault + ")");
                element = readable(element);
                elements.set(index, element);
            }
            if (!element.isPrimitive()) {
                int groupChildren = element.numChildren() != null ? element.numChildren() : 0;
                walkChildren(elements, cursor, groupChildren, path, dropped);
            }
            path.removeLast();
        }
    }

    /// The element less the annotation [#fault] names: a primitive keeps no annotation, a group
    /// the annotation and converted type it can carry.
    private static SchemaElement readable(SchemaElement element) {
        if (element.isPrimitive()) {
            return new SchemaElement(element.name(), element.type(), element.typeLength(), element.repetitionType(),
                    element.numChildren(), null, element.scale(), element.precision(), element.fieldId(), null);
        }
        LogicalType annotation = AnnotationPairings.readableGroupAnnotation(element.logicalType());
        ConvertedType converted = AnnotationPairings.readableGroupConvertedType(annotation, element.convertedType());
        return new SchemaElement(element.name(), element.type(), element.typeLength(), element.repetitionType(),
                element.numChildren(), converted, element.scale(), element.precision(), element.fieldId(), annotation);
    }
}
