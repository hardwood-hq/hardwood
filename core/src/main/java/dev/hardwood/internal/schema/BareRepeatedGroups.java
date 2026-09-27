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
import java.util.BitSet;
import java.util.Deque;
import java.util.List;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// Drops the annotation of a repeated group that stands outside a `LIST` or `MAP` group, before
/// a footer's schema elements become a [FileSchema].
///
/// The format reads a repeated field that is neither inside a `LIST` or `MAP` group nor
/// annotated `LIST` or `MAP` as a required list whose element is the field itself
/// ([Nested Types](https://parquet.apache.org/docs/file-format/types/logicaltypes/#nested-types)).
/// A group standing there annotated `LIST` or `MAP` is excluded from that rule, and has no
/// reading of its own: a repeated two-level `LIST` is only ever the element of another `LIST`,
/// and a `MAP` group is `required` or `optional`. `MAP_KEY_VALUE` outside a `MAP` group stands
/// for `MAP`, and no other annotation applies to a group. As for a leaf annotation its physical
/// type cannot carry, the reader drops such an annotation with one warning per file, and the
/// group reads as the unannotated repeated group it is in structure: a list of its own fields.
///
/// `VARIANT` is the exception. The rule covers it, making the field a list of variants, which
/// the reader does not serve. The annotation stays in the schema, so the file opens and reports
/// the group as the footer states it, and [#refuseTouchedVariants] refuses a read that touches
/// one of the group's columns rather than reading it as something else.
///
/// Whether a group is inside a `LIST` or `MAP` group follows [FileSchema#fromSchemaElements]:
/// a group whose only child is a repeated `MAP_KEY_VALUE` group, and which carries no logical
/// type, reads as a legacy `MAP`, so that child is not bare. A bare repeated group of that shape
/// keeps its inferred `MAP`: it is a list whose element is a legacy map.
///
/// The drop applies to schemas read from a footer only. A schema declared for the writer
/// never reaches this class, and `WriterSchemaShape` refuses these groups there.
public final class BareRepeatedGroups {

    private static final System.Logger LOG = System.getLogger(BareRepeatedGroups.class.getName());

    private BareRepeatedGroups() {
    }

    /// Returns `elements` with the annotation of every repeated group outside a `LIST` or `MAP`
    /// group cleared, except a `VARIANT` annotation, logging one warning that lists them;
    /// `elements` itself when there is none.
    ///
    /// @param elements a footer's schema elements, root first
    public static List<SchemaElement> dropAnnotations(List<SchemaElement> elements) {
        if (elements.isEmpty() || elements.get(0).numChildren() == null) {
            return elements;
        }
        List<SchemaElement> result = new ArrayList<>(elements);
        List<String> dropped = new ArrayList<>();
        int[] cursor = { 1 };
        walkChildren(result, cursor, elements.get(0).numChildren(), false, new ArrayDeque<>(), dropped);
        if (dropped.isEmpty()) {
            return elements;
        }
        LOG.log(System.Logger.Level.WARNING,
                "Ignoring {0} annotation(s) on repeated groups outside a LIST or MAP group; those groups"
                + " are read as though unannotated: {1}",
                dropped.size(), String.join("; ", dropped));
        return result;
    }

    private static void walkChildren(List<SchemaElement> elements, int[] cursor, int numChildren,
                                      boolean parentIsListOrMap, Deque<String> path, List<String> dropped) {
        for (int i = 0; i < numChildren; i++) {
            int index = cursor[0]++;
            SchemaElement element = elements.get(index);
            if (element.isPrimitive()) {
                continue;
            }
            path.addLast(element.name());
            if (!parentIsListOrMap && element.repetitionType() == RepetitionType.REPEATED
                    && isAnnotated(element) && !(element.logicalType() instanceof LogicalType.VariantType)) {
                element = dropAnnotation(element, String.join(".", path), dropped);
                elements.set(index, element);
            }
            int groupChildren = element.numChildren() != null ? element.numChildren() : 0;
            walkChildren(elements, cursor, groupChildren, isListOrMap(elements, index), path, dropped);
            path.removeLast();
        }
    }

    private static SchemaElement dropAnnotation(SchemaElement group, String path, List<String> dropped) {
        Object annotation = group.logicalType() != null ? group.logicalType() : group.convertedType();
        dropped.add(path + " (" + annotation + ")");
        return new SchemaElement(group.name(), group.type(), group.typeLength(), group.repetitionType(),
                group.numChildren(), null, group.scale(), group.precision(), group.fieldId(), null);
    }

    /// Refuses a read that touches a column below a repeated `VARIANT` group outside a `LIST` or
    /// `MAP` group, a list of variants the reader does not serve.
    ///
    /// @param fileName the file `schema` was read from, for the message; may be `null`
    /// @param schema the file's schema
    /// @param touchedColumns the leaf ordinals, in `schema`, the read projects or filters on
    /// @throws UnsupportedOperationException if a touched column lies below such a group
    public static void refuseTouchedVariants(String fileName, FileSchema schema, BitSet touchedColumns) {
        refuseTouchedVariants(fileName, schema.getRootNode().children(), false, new ArrayDeque<>(),
                touchedColumns);
    }

    private static void refuseTouchedVariants(String fileName, List<SchemaNode> children, boolean parentIsListOrMap,
                                              Deque<String> path, BitSet touchedColumns) {
        for (SchemaNode child : children) {
            if (!(child instanceof SchemaNode.GroupNode group)) {
                continue;
            }
            path.addLast(group.name());
            if (!parentIsListOrMap && group.repetitionType() == RepetitionType.REPEATED && group.isVariant()) {
                if (touchesLeaf(group, touchedColumns)) {
                    throw new UnsupportedOperationException(ExceptionContext.filePrefix(fileName)
                            + "Repeated group '" + String.join(".", path) + "' is annotated " + group.logicalType()
                            + " outside a LIST or MAP group; a list of variants in this form is not supported");
                }
            }
            else {
                refuseTouchedVariants(fileName, group.children(), group.isList() || group.isMap(), path,
                        touchedColumns);
            }
            path.removeLast();
        }
    }

    private static boolean touchesLeaf(SchemaNode node, BitSet touchedColumns) {
        return switch (node) {
            case SchemaNode.PrimitiveNode leaf -> touchedColumns.get(leaf.columnIndex());
            case SchemaNode.GroupNode group -> {
                for (SchemaNode child : group.children()) {
                    if (touchesLeaf(child, touchedColumns)) {
                        yield true;
                    }
                }
                yield false;
            }
        };
    }

    private static boolean isAnnotated(SchemaElement element) {
        return element.convertedType() != null || element.logicalType() != null;
    }

    /// Whether the group at `index` reads as a `LIST` or `MAP`, as [FileSchema#fromSchemaElements]
    /// decides it: by its own annotation, or, carrying no logical type, by a sole repeated
    /// `MAP_KEY_VALUE` child.
    private static boolean isListOrMap(List<SchemaElement> elements, int index) {
        SchemaElement group = elements.get(index);
        ConvertedType converted = group.convertedType();
        LogicalType logical = group.logicalType();
        if (converted == ConvertedType.LIST || converted == ConvertedType.MAP
                || logical instanceof LogicalType.ListType || logical instanceof LogicalType.MapType) {
            return true;
        }
        if (logical != null || group.numChildren() == null || group.numChildren() != 1) {
            return false;
        }
        SchemaElement child = elements.get(index + 1);
        return !child.isPrimitive()
                && child.repetitionType() == RepetitionType.REPEATED
                && child.convertedType() == ConvertedType.MAP_KEY_VALUE;
    }
}
