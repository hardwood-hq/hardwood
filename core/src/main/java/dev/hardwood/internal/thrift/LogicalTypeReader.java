/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.LogicalType.EdgeInterpolationAlgorithm;
import dev.hardwood.metadata.LogicalType.TimeUnit;
import dev.hardwood.reader.ParquetReadException;

/// Reader for LogicalType union from Thrift Compact Protocol.
/// LogicalType is a union with different variants for each type.
public class LogicalTypeReader {

    private static final System.Logger LOG =
            System.getLogger(LogicalTypeReader.class.getName());

    public static LogicalType read(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.LOGICAL_TYPE);
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static LogicalType readInternal(ThriftCompactReader reader) {
        LogicalType result = null;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                return result;
            }

            // Union: only one field should be set, but we need to read to the end
            if (result == null) {
                result = switch (ThriftCompactReader.fieldId(header)) {
                    case 1 -> { // STRING
                        reader.skipField(ThriftCompactReader.fieldType(header)); // Empty struct
                        yield new LogicalType.StringType();
                    }
                    case 2 -> { // MAP
                        reader.skipField(ThriftCompactReader.fieldType(header)); // Empty Struct
                        yield new LogicalType.MapType();
                    }
                    case 3 -> { // LIST
                        reader.skipField(ThriftCompactReader.fieldType(header)); // Empty Struct
                        yield new LogicalType.ListType();
                    }
                    case 4 -> { // ENUM
                        reader.skipField(ThriftCompactReader.fieldType(header)); // Empty struct
                        yield new LogicalType.EnumType();
                    }
                    case 5 -> readDecimalType(reader);
                    case 6 -> { // DATE
                        reader.skipField(ThriftCompactReader.fieldType(header)); // Empty struct
                        yield new LogicalType.DateType();
                    }
                    case 7 -> readTimeType(reader);
                    case 8 -> readTimestampType(reader);
                    case 9 -> { // INTERVAL
                        reader.skipField(ThriftCompactReader.fieldType(header)); // Empty struct
                        yield new LogicalType.IntervalType();
                    }
                    case 10 -> readIntType(reader);
                    case 11 -> { // NULL
                        reader.skipField(ThriftCompactReader.fieldType(header)); // Empty struct
                        yield new LogicalType.NullType();
                    }
                    case 12 -> { // JSON
                        reader.skipField(ThriftCompactReader.fieldType(header)); // Empty struct
                        yield new LogicalType.JsonType();
                    }
                    case 13 -> { // BSON
                        reader.skipField(ThriftCompactReader.fieldType(header)); // Empty struct
                        yield new LogicalType.BsonType();
                    }
                    case 14 -> { // UUID
                        reader.skipField(ThriftCompactReader.fieldType(header)); // Empty struct
                        yield new LogicalType.UuidType();
                    }
                    case 15 -> { // FLOAT16
                        reader.skipField(ThriftCompactReader.fieldType(header)); // Empty struct
                        yield new LogicalType.Float16Type();
                    }
                    case 16 -> readVariantType(reader);
                    case 17 -> readGeometryType(reader);
                    case 18 -> readGeographyType(reader);
                    // An arm this version does not know. The format treats a new logical
                    // type as forward-compatible: read the physical values and lose the
                    // semantics, rather than refuse a file a newer writer produced.
                    default -> {
                        LOG.log(System.Logger.Level.WARNING,
                                "Ignoring unrecognized LogicalType union field {0};"
                                + " the column will be read as its physical type."
                                + " The file may have been written against a newer"
                                + " version of the format.",
                                ThriftCompactReader.fieldId(header));
                        reader.skipField(ThriftCompactReader.fieldType(header));
                        yield null;
                    }
                };
            }
            else {
                // Already found the union variant, skip remaining fields
                reader.skipField(ThriftCompactReader.fieldType(header));
            }
        }
    }

    private static LogicalType.DecimalType readDecimalType(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.DECIMAL_TYPE);
        try {
            return readDecimalTypeInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static LogicalType.DecimalType readDecimalTypeInternal(ThriftCompactReader reader) {
        int scale = -1;
        int precision = -1;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            switch (ThriftCompactReader.fieldId(header)) {
                case 1: // scale (required)
                    if (reader.acceptField(header, Codes.I32)) {
                        scale = reader.readI32();
                    }
                    break;
                case 2: // precision (required)
                    if (reader.acceptField(header, Codes.I32)) {
                        precision = reader.readI32();
                    }
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        // Validate both fields were read
        if (scale < 0 || precision <= 0) {
            throw new ParquetReadException(
                    "Invalid DecimalType: scale=" + scale + ", precision=" + precision);
        }

        return new LogicalType.DecimalType(scale, precision);
    }

    private static LogicalType.TimeType readTimeType(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.TIME_TYPE);
        try {
            return readTimeTypeInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static LogicalType.TimeType readTimeTypeInternal(ThriftCompactReader reader) {
        boolean isAdjustedToUTC = true;
        LogicalType.TimeType.TimeUnit unit = LogicalType.TimeType.TimeUnit.MILLIS;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            switch (ThriftCompactReader.fieldId(header)) {
                case 1: // isAdjustedToUTC (required)
                    isAdjustedToUTC = reader.readBooleanField(header, isAdjustedToUTC);
                    break;
                case 2: // unit (required)
                    unit = readTimeUnit(reader);
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        return new LogicalType.TimeType(isAdjustedToUTC, unit);
    }

    private static LogicalType.TimestampType readTimestampType(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.TIMESTAMP_TYPE);
        try {
            return readTimestampTypeInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static LogicalType.TimestampType readTimestampTypeInternal(ThriftCompactReader reader) {
        boolean isAdjustedToUTC = true;
        LogicalType.TimestampType.TimeUnit unit = LogicalType.TimestampType.TimeUnit.MILLIS;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            switch (ThriftCompactReader.fieldId(header)) {
                case 1: // isAdjustedToUTC (required)
                    isAdjustedToUTC = reader.readBooleanField(header, isAdjustedToUTC);
                    break;
                case 2: // unit (required)
                    unit = readTimeUnit(reader);
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        return new LogicalType.TimestampType(isAdjustedToUTC, unit);
    }

    private static LogicalType.IntType readIntType(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.INT_TYPE);
        try {
            return readIntTypeInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static LogicalType.IntType readIntTypeInternal(ThriftCompactReader reader) {
        int bitWidth = 8;
        boolean isSigned = true;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            switch (ThriftCompactReader.fieldId(header)) {
                case 1: // bitWidth (required)
                    if (reader.acceptField(header, Codes.BYTE)) {
                        bitWidth = reader.readByte();
                    }
                    break;
                case 2: // isSigned (required)
                    isSigned = reader.readBooleanField(header, isSigned);
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        return new LogicalType.IntType(bitWidth, isSigned);
    }

    private static LogicalType.VariantType readVariantType(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.VARIANT_TYPE);
        try {
            return readVariantTypeInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static LogicalType.VariantType readVariantTypeInternal(ThriftCompactReader reader) {
        int specVersion = 1; // Per Parquet Variant spec: default when unset.

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            switch (ThriftCompactReader.fieldId(header)) {
                case 1: // specification_version (optional i8)
                    if (reader.acceptField(header, Codes.BYTE)) {
                        specVersion = reader.readByte();
                    }
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        return new LogicalType.VariantType(specVersion);
    }

    private static TimeUnit readTimeUnit(ThriftCompactReader reader) {
        int fieldId = reader.readUnionVariant(ThriftStruct.TIME_UNIT);
        return switch (fieldId) {
            case 1 -> TimeUnit.MILLIS;
            case 2 -> TimeUnit.MICROS;
            case 3 -> TimeUnit.NANOS;
            default -> throw new ParquetReadException(
                    ThriftStruct.TIME_UNIT.describe(fieldId) + " is not a time unit");
        };
    }

    private static LogicalType.GeometryType readGeometryType(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.GEOMETRY_TYPE);
        try {
            return readGeometryTypeInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static LogicalType.GeometryType readGeometryTypeInternal(ThriftCompactReader reader) {
        String crs = null;
        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            switch (ThriftCompactReader.fieldId(header)) {
                case 1: // CRS
                    if (reader.acceptField(header, Codes.BINARY)) {
                        crs = reader.readString();
                    }
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        if (crs == null) {
            crs = "OGC:CRS84";
        }

        return new LogicalType.GeometryType(crs);
    }

    private static LogicalType.GeographyType readGeographyType(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.GEOGRAPHY_TYPE);
        try {
            return readGeographyTypeInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static LogicalType.GeographyType readGeographyTypeInternal(ThriftCompactReader reader) {
        String crs = null;
        EdgeInterpolationAlgorithm edgeInterpolation = null;
        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            switch (ThriftCompactReader.fieldId(header)) {
                case 1: // CRS
                    if (reader.acceptField(header, Codes.BINARY)) {
                        crs = reader.readString();
                    }
                    break;
                case 2: // algorithm — a Thrift enum, so an i32 rather than a union
                    if (reader.acceptField(header, Codes.I32)) {
                        edgeInterpolation = ThriftEnumLookup.edgeInterpolationAlgorithm(reader.readI32());
                    }
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        if (crs == null) {
            crs = "OGC:CRS84";
        }
        if (edgeInterpolation == null) {
            edgeInterpolation = EdgeInterpolationAlgorithm.SPHERICAL;
        }

        return new LogicalType.GeographyType(crs, edgeInterpolation);
    }

    /// Reads the single variant of a Thrift union and returns its field id, leaving the reader on
    /// the byte after the union's STOP. The variant's value is consumed but not decoded: which
    /// variant is set is the whole of the union's meaning here.
    ///
    /// A union carries exactly one variant. None leaves nothing to report, and the field it
    /// stands for — a timestamp's unit, a geography's edge model — has no default that could
    /// stand in for it. More than one is worse than ambiguous: the byte after the first variant
    /// is then another field header rather than STOP, so reading on would take the second
}
