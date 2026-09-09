/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.SchemaNode;

/// The `SchemaNode`-aware decode behind the generic accessors on the `PqStruct`,
/// `PqList` and `PqMap` flyweights, and the predicate that tells them which leaves
/// skip it.
///
/// The decode table itself lives in [LogicalTypeConverter], which knows only a
/// physical type and an annotation. What is added here is the `SchemaNode`
/// unwrap, the INT96 convention, and the group short-circuit the flyweights
/// need. A typed accessor names the type it returns and reads the stored
/// primitive directly, so it does not come through here.
final class ValueConverter {

    private ValueConverter() {
    }

    /// Whether a leaf with this physical and logical type decodes to a `String`
    /// (`UTF8`, `ENUM` or `JSON` over `BYTE_ARRAY`). Single source of truth for the
    /// row reader's string-interning gate: the recording side
    /// ([BatchExchange#isStringColumn]) and the consumer side
    /// ([#isStringLeaf(SchemaNode)] and [FlatRowReader#getValue]) all resolve
    /// through it, so the gate that records dictionary indices and the gate that
    /// reads them back cannot disagree.
    ///
    /// `ENUM` carries a UTF-8 payload that the Parquet specification tells readers
    /// without a native enum type to interpret as a string, so it decodes exactly
    /// like `UTF8`.
    static boolean isStringLeaf(PhysicalType type, LogicalType logicalType) {
        return type == PhysicalType.BYTE_ARRAY
                && (logicalType instanceof LogicalType.StringType
                    || logicalType instanceof LogicalType.EnumType
                    || logicalType instanceof LogicalType.JsonType);
    }

    /// Whether `schema` is a string leaf — see
    /// [#isStringLeaf(PhysicalType, LogicalType)]. Such leaves are served from the
    /// row reader's per-chunk interned-`String` cache (`getString`) instead of
    /// decoding per value; other leaves go through [#convertValue].
    static boolean isStringLeaf(SchemaNode schema) {
        return schema instanceof SchemaNode.PrimitiveNode primitive
                && isStringLeaf(primitive.type(), primitive.logicalType());
    }

    /// Decode a leaf value for a caller that takes whatever the column holds.
    /// A group node is returned untouched: struct, list and map values are built
    /// by the flyweights themselves and never carry a leaf decode.
    static Object convertValue(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        if (schema instanceof SchemaNode.GroupNode) {
            return rawValue;
        }
        return convertPrimitive(rawValue, (SchemaNode.PrimitiveNode) schema);
    }

    /// The one decode a leaf goes through, so a struct field, a list element and a
    /// map value all read the same leaf the same way.
    private static Object convertPrimitive(Object rawValue, SchemaNode.PrimitiveNode primitive) {
        LogicalType logicalType = primitive.logicalType();
        if (logicalType == null && primitive.type() == PhysicalType.INT96) {
            // INT96 carries no logical type but is conventionally a TIMESTAMP. An
            // annotation still standing here is one no physical type is imposed on, since
            // FileSchema has already dropped any that INT96 cannot carry, so it decides
            // the decode as it does on any other column. FlatRowReader.classifyLeaf keys
            // its INT96 leaf kind the same way.
            return LogicalTypeConverter.int96ToInstant((byte[]) rawValue);
        }
        return LogicalTypeConverter.convert(rawValue, primitive.type(), logicalType);
    }
}
