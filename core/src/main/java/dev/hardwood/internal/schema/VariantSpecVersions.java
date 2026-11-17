/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Deque;
import java.util.List;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// The version of the Variant specification the reader implements, and the refusal of a read
/// that touches a `VARIANT` group annotated with another one.
///
/// A `VARIANT` annotation names the specification version its group's bytes are encoded in. A
/// version the reader does not implement cannot be decoded, nor read as the unannotated group,
/// whose leaves would surface the encoded bytes as though they were plain binaries. The
/// annotation stays in the schema, so the file opens and reports the group as the footer states
/// it, and [#refuseTouchedUnsupported] refuses a read that touches one of the group's columns.
public final class VariantSpecVersions {

    /// The one Variant specification version the reader decodes.
    public static final int SUPPORTED = 1;

    private VariantSpecVersions() {
    }

    /// Refuses a read that touches a column below a `VARIANT` group whose specification version
    /// is not [#SUPPORTED].
    ///
    /// @param fileName the file `schema` was read from, for the message; may be `null`
    /// @param schema the file's schema
    /// @param touchedColumns the leaf ordinals, in `schema`, the read projects or filters on
    /// @throws UnsupportedOperationException if a touched column lies below such a group
    public static void refuseTouchedUnsupported(String fileName, FileSchema schema, BitSet touchedColumns) {
        refuseTouchedUnsupported(fileName, schema.getRootNode().children(), new ArrayDeque<>(), touchedColumns);
    }

    private static void refuseTouchedUnsupported(String fileName, List<SchemaNode> children, Deque<String> path,
                                                 BitSet touchedColumns) {
        for (SchemaNode child : children) {
            if (!(child instanceof SchemaNode.GroupNode group)) {
                continue;
            }
            path.addLast(group.name());
            if (group.logicalType() instanceof LogicalType.VariantType variant
                    && variant.specVersion() != SUPPORTED) {
                if (BareRepeatedGroups.touchesLeaf(group, touchedColumns)) {
                    throw new UnsupportedOperationException(ExceptionContext.filePrefix(fileName)
                            + "Group '" + String.join(".", path) + "' is annotated " + variant + "; version "
                            + variant.specVersion() + " of the Variant specification is not supported, the"
                            + " supported version is " + SUPPORTED);
                }
            }
            else {
                refuseTouchedUnsupported(fileName, group.children(), path, touchedColumns);
            }
            path.removeLast();
        }
    }
}
