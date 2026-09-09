/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.schema.SchemaNode;

/// The `SchemaNode`-aware decode behind the generic accessors on the `PqStruct`,
/// `PqList` and `PqMap` flyweights.
///
/// The decode table itself lives in [LogicalTypeConverter], which knows only a
/// physical type and an annotation, and [LeafKind] states which of its entry points
/// a leaf goes to. What is added here is the `SchemaNode` unwrap and the group
/// short-circuit the flyweights need.
///
/// A typed accessor names the type it returns and reads the stored primitive
/// directly, so it does not come through here.
final class NestedLeafDecoder {

    private NestedLeafDecoder() {
    }

    /// Decode a leaf value for a caller that takes whatever the column holds.
    ///
    /// A group node is returned untouched: struct, list and map values are built by
    /// the flyweights themselves and never carry a leaf decode.
    static Object decode(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        LeafKind kind = LeafKind.of(schema);
        if (kind == null) {
            return rawValue;
        }
        SchemaNode.PrimitiveNode primitive = (SchemaNode.PrimitiveNode) schema;
        return switch (kind) {
            case INT96_TIMESTAMP -> LogicalTypeConverter.int96ToInstant((byte[]) rawValue);
            case RAW -> rawValue;
            case STRING, CONVERT -> LogicalTypeConverter.convert(
                    rawValue, primitive.type(), primitive.logicalType());
        };
    }
}
