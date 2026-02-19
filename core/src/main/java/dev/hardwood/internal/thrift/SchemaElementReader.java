/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;

import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;

/**
 * Reader for SchemaElement from Thrift Compact Protocol.
 */
public class SchemaElementReader {

    public static SchemaElement read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static SchemaElement readInternal(ThriftCompactReader reader) throws IOException {
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

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // type (optional)
                    if (header.type() == 0x05) { // I32
                        type = PhysicalType.fromThriftValue(reader.readI32());
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 2: // type_length (optional)
                    if (header.type() == 0x05) {
                        typeLength = reader.readI32();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 3: // repetition_type (optional)
                    if (header.type() == 0x05) {
                        repetitionType = RepetitionType.fromThriftValue(reader.readI32());
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 4: // name (required)
                    if (header.type() == 0x08) { // BINARY
                        name = reader.readString();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 5: // num_children (optional)
                    if (header.type() == 0x05) {
                        numChildren = reader.readI32();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 6: // converted_type (optional)
                    if (header.type() == 0x05) { // I32
                        convertedType = ConvertedType.fromThriftValue(reader.readI32());
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 7: // scale (optional) - for legacy DECIMAL support
                    if (header.type() == 0x05) {
                        scale = reader.readI32();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 8: // precision (optional) - for legacy DECIMAL support
                    if (header.type() == 0x05) {
                        precision = reader.readI32();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 9: // field_id (optional)
                    if (header.type() == 0x05) {
                        fieldId = reader.readI32();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 10: // logicalType (optional)
                    if (header.type() == 0x0C) { // STRUCT
                        logicalType = LogicalTypeReader.read(reader);
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                default:
                    reader.skipField(header.type());
                    break;
            }
        }

        return new SchemaElement(name, type, typeLength, repetitionType, numChildren, convertedType, scale, precision, fieldId, logicalType);
    }
}
