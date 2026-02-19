/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.LogicalType.TimeUnit;

/**
 * Reader for LogicalType union from Thrift Compact Protocol.
 * LogicalType is a union with different variants for each type.
 */
public class LogicalTypeReader {

    public static LogicalType read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static LogicalType readInternal(ThriftCompactReader reader) throws IOException {
        LogicalType result = null;

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                return result;
            }

            // Union: only one field should be set, but we need to read to the end
            if (result == null) {
                result = switch (header.fieldId()) {
                    case 1 -> { // STRING
                        reader.skipField(header.type()); // Empty struct
                        yield new LogicalType.StringType();
                    }
                    case 4 -> { // ENUM
                        reader.skipField(header.type()); // Empty struct
                        yield new LogicalType.EnumType();
                    }
                    case 5 -> readDecimalType(reader);
                    case 6 -> { // DATE
                        reader.skipField(header.type()); // Empty struct
                        yield new LogicalType.DateType();
                    }
                    case 7 -> readTimeType(reader);
                    case 8 -> readTimestampType(reader);
                    case 10 -> readIntType(reader);
                    case 12 -> { // JSON
                        reader.skipField(header.type()); // Empty struct
                        yield new LogicalType.JsonType();
                    }
                    case 13 -> { // BSON
                        reader.skipField(header.type()); // Empty struct
                        yield new LogicalType.BsonType();
                    }
                    case 14 -> { // UUID
                        reader.skipField(header.type()); // Empty struct
                        yield new LogicalType.UuidType();
                    }
                    // Skip unsupported types (MAP, LIST, etc.)
                    default -> {
                        reader.skipField(header.type());
                        yield null;
                    }
                };
            }
            else {
                // Already found the union variant, skip remaining fields
                reader.skipField(header.type());
            }
        }
    }

    private static LogicalType.DecimalType readDecimalType(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readDecimalTypeInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static LogicalType.DecimalType readDecimalTypeInternal(ThriftCompactReader reader) throws IOException {
        int scale = -1;
        int precision = -1;

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // scale (required)
                    if (header.type() == 0x05) { // I32
                        scale = reader.readI32();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 2: // precision (required)
                    if (header.type() == 0x05) { // I32
                        precision = reader.readI32();
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

        // Validate both fields were read
        if (scale < 0 || precision <= 0) {
            throw new IllegalStateException(
                    "Invalid DecimalType: scale=" + scale + ", precision=" + precision);
        }

        return new LogicalType.DecimalType(scale, precision);
    }

    private static LogicalType.TimeType readTimeType(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readTimeTypeInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static LogicalType.TimeType readTimeTypeInternal(ThriftCompactReader reader) throws IOException {
        boolean isAdjustedToUTC = true;
        LogicalType.TimeType.TimeUnit unit = LogicalType.TimeType.TimeUnit.MILLIS;

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // isAdjustedToUTC (required)
                    if (header.type() == 0x01) { // TYPE_BOOLEAN_TRUE
                        isAdjustedToUTC = true;
                    }
                    else if (header.type() == 0x02) { // TYPE_BOOLEAN_FALSE
                        isAdjustedToUTC = false;
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 2: // unit (required)
                    unit = readTimeUnit(reader);
                    break;
                default:
                    reader.skipField(header.type());
                    break;
            }
        }

        return new LogicalType.TimeType(isAdjustedToUTC, unit);
    }

    private static LogicalType.TimestampType readTimestampType(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readTimestampTypeInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static LogicalType.TimestampType readTimestampTypeInternal(ThriftCompactReader reader) throws IOException {
        boolean isAdjustedToUTC = true;
        LogicalType.TimestampType.TimeUnit unit = LogicalType.TimestampType.TimeUnit.MILLIS;

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // isAdjustedToUTC (required)
                    if (header.type() == 0x01) { // TYPE_BOOLEAN_TRUE
                        isAdjustedToUTC = true;
                    }
                    else if (header.type() == 0x02) { // TYPE_BOOLEAN_FALSE
                        isAdjustedToUTC = false;
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 2: // unit (required)
                    unit = readTimeUnit(reader);
                    break;
                default:
                    reader.skipField(header.type());
                    break;
            }
        }

        return new LogicalType.TimestampType(isAdjustedToUTC, unit);
    }

    private static LogicalType.IntType readIntType(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readIntTypeInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static LogicalType.IntType readIntTypeInternal(ThriftCompactReader reader) throws IOException {
        int bitWidth = 8;
        boolean isSigned = true;

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // bitWidth (required)
                    if (header.type() == 0x03) { // I8 (byte)
                        bitWidth = reader.readByte();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 2: // isSigned (required)
                    if (header.type() == 0x01) { // TYPE_BOOLEAN_TRUE
                        isSigned = true;
                    }
                    else if (header.type() == 0x02) { // TYPE_BOOLEAN_FALSE
                        isSigned = false;
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

        return new LogicalType.IntType(bitWidth, isSigned);
    }

    private static TimeUnit readTimeUnit(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            int fieldId = header.fieldId();
            reader.skipField(header.type());
            reader.readFieldHeader(); // Consume STOP

            return switch (fieldId) {
                case 1 -> TimeUnit.MILLIS;
                case 2 -> TimeUnit.MICROS;
                case 3 -> TimeUnit.NANOS;
                default -> throw new IllegalArgumentException("Unexpected time unit:" + fieldId);
            };
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }
}
