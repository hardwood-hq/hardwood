/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.metadata.SchemaElement;

/// Writer for SchemaElement to Thrift Compact Protocol, the inverse of
/// [SchemaElementReader].
public class SchemaElementWriter {

    public static void write(ThriftCompactWriter writer, SchemaElement element) {
        rejectUnsupported(element);

        short saved = writer.pushFieldIdContext();
        try {
            if (element.type() != null) {
                writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.I32);
                writer.writeI32(ThriftEnumLookup.thriftValue(element.type()));
            }
            if (element.typeLength() != null) {
                writer.writeFieldBegin(2, ThriftCompactConstants.FieldType.I32);
                writer.writeI32(element.typeLength());
            }
            if (element.repetitionType() != null) {
                writer.writeFieldBegin(3, ThriftCompactConstants.FieldType.I32);
                writer.writeI32(ThriftEnumLookup.thriftValue(element.repetitionType()));
            }
            // name is required
            writer.writeFieldBegin(4, ThriftCompactConstants.FieldType.BINARY);
            writer.writeString(element.name());
            if (element.numChildren() != null) {
                writer.writeFieldBegin(5, ThriftCompactConstants.FieldType.I32);
                writer.writeI32(element.numChildren());
            }
            writer.writeFieldStop();
        }
        finally {
            writer.popFieldIdContext(saved);
        }
    }

    /// Fail fast on annotations the writer cannot yet serialize, rather than
    /// silently dropping them and producing a schema that does not round-trip.
    private static void rejectUnsupported(SchemaElement element) {
        if (element.convertedType() != null || element.logicalType() != null
                || element.scale() != null || element.precision() != null
                || element.fieldId() != null) {
            throw new UnsupportedOperationException(
                    "Writer does not yet support converted/logical types or field ids: " + element.name());
        }
    }
}
