/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;
import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;

/// Reader for SchemaElement from Thrift Compact Protocol.
public class SchemaElementReader {

    /// A schema element together with whether its `logicalType` field was present but decoded to
    /// no annotation: a union member added to the format after this version, or a union with no
    /// member set. [SchemaElement] cannot record that, since the element reads as unannotated
    /// either way.
    ///
    /// @param element the element as read
    /// @param logicalTypeUnread whether the footer annotated the element with a logical type this
    ///        reader does not decode
    public record ReadElement(SchemaElement element, boolean logicalTypeUnread) {
    }

    public static SchemaElement read(ThriftCompactReader reader) {
        return readElement(reader).element();
    }

    public static ReadElement readElement(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.SCHEMA_ELEMENT);
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static ReadElement readInternal(ThriftCompactReader reader) {
        String name = null;
        PhysicalType type = null;
        Integer typeLength = null;
        RepetitionType repetitionType = null;
        Integer numChildren = null;
        ConvertedType convertedType = null;
        Integer scale = null;
        Integer precision = null;
        Integer fieldId = null;
        LogicalType logicalType = null;
        boolean logicalTypePresent = false;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            switch (ThriftCompactReader.fieldId(header)) {
                case 1: // type (optional)
                    if (reader.acceptField(header, Codes.I32)) {
                        type = ThriftEnumLookup.physicalType(reader.readI32());
                    }
                    break;
                case 2: // type_length (optional)
                    if (reader.acceptField(header, Codes.I32)) {
                        typeLength = reader.readI32();
                    }
                    break;
                case 3: // repetition_type (optional)
                    if (reader.acceptField(header, Codes.I32)) {
                        repetitionType = ThriftEnumLookup.repetitionType(reader.readI32());
                    }
                    break;
                case 4: // name (required)
                    if (reader.acceptField(header, Codes.BINARY)) {
                        name = reader.readString();
                    }
                    break;
                case 5: // num_children (optional)
                    if (reader.acceptField(header, Codes.I32)) {
                        numChildren = reader.readI32();
                    }
                    break;
                case 6: // converted_type (optional)
                    if (reader.acceptField(header, Codes.I32)) {
                        convertedType = ThriftEnumLookup.convertedType(reader.readI32());
                    }
                    break;
                case 7: // scale (optional) - for legacy DECIMAL support
                    if (reader.acceptField(header, Codes.I32)) {
                        scale = reader.readI32();
                    }
                    break;
                case 8: // precision (optional) - for legacy DECIMAL support
                    if (reader.acceptField(header, Codes.I32)) {
                        precision = reader.readI32();
                    }
                    break;
                case 9: // field_id (optional)
                    if (reader.acceptField(header, Codes.I32)) {
                        fieldId = reader.readI32();
                    }
                    break;
                case 10: // logicalType (optional)
                    if (reader.acceptField(header, Codes.STRUCT)) {
                        logicalType = LogicalTypeReader.read(reader);
                        logicalTypePresent = true;
                    }
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        return new ReadElement(
                new SchemaElement(name, type, typeLength, repetitionType, numChildren, convertedType, scale, precision, fieldId, logicalType),
                logicalTypePresent && logicalType == null);
    }
}
