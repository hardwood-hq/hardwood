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

    /// The required fields of the member structs that have any — `DecimalType`, `TimeType`,
    /// `TimestampType` and `IntType` — which are fields 1 and 2 of each.
    private static final int[] BOTH_FIELDS = { 1, 2 };

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
                    case 1 -> emptyArm(reader, header, LogicalType.string()); // STRING
                    case 2 -> emptyArm(reader, header, LogicalType.map()); // MAP
                    case 3 -> emptyArm(reader, header, LogicalType.list()); // LIST
                    case 4 -> emptyArm(reader, header, LogicalType.enumType()); // ENUM
                    case 5 -> readDecimalType(reader, header);
                    case 6 -> emptyArm(reader, header, LogicalType.date()); // DATE
                    case 7 -> readTimeType(reader, header);
                    case 8 -> readTimestampType(reader, header);
                    case 9 -> emptyArm(reader, header, LogicalType.interval()); // INTERVAL
                    case 10 -> readIntType(reader, header);
                    case 11 -> emptyArm(reader, header, LogicalType.nullType()); // NULL
                    case 12 -> emptyArm(reader, header, LogicalType.json()); // JSON
                    case 13 -> emptyArm(reader, header, LogicalType.bson()); // BSON
                    case 14 -> emptyArm(reader, header, LogicalType.uuid()); // UUID
                    case 15 -> emptyArm(reader, header, LogicalType.float16()); // FLOAT16
                    case 16 -> readVariantType(reader, header);
                    case 17 -> readGeometryType(reader, header);
                    case 18 -> readGeographyType(reader, header);
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

    /// Reads a member arm whose struct carries no fields. Like every member arm this version
    /// knows, it must be declared as a struct: skipped by any other wire type, it would still
    /// assert an annotation the bytes do not encode.
    private static LogicalType emptyArm(ThriftCompactReader reader, int header, LogicalType type) {
        reader.requireField(header, Codes.STRUCT);
        reader.skipField(ThriftCompactReader.fieldType(header));
        return type;
    }

    private static LogicalType.DecimalType readDecimalType(ThriftCompactReader reader, int header) {
        reader.requireField(header, Codes.STRUCT);
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
        long seen = 0;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            int fieldId = ThriftCompactReader.fieldId(header);
            switch (fieldId) {
                case 1: // scale (required)
                    reader.requireField(header, Codes.I32);
                    scale = reader.readI32();
                    seen |= 1L << fieldId;
                    break;
                case 2: // precision (required)
                    reader.requireField(header, Codes.I32);
                    precision = reader.readI32();
                    seen |= 1L << fieldId;
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        ThriftCompactReader.requireFields(ThriftStruct.DECIMAL_TYPE, seen, BOTH_FIELDS);

        // Validate that the pair is one the annotation admits: the record rejects a scale above
        // the precision as a caller error, which a file carrying one is not.
        if (scale < 0 || precision <= 0 || scale > precision) {
            throw new ParquetReadException(
                    "Invalid DecimalType: scale=" + scale + ", precision=" + precision);
        }

        return LogicalType.decimal(precision, scale);
    }

    private static LogicalType.TimeType readTimeType(ThriftCompactReader reader, int header) {
        reader.requireField(header, Codes.STRUCT);
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
        long seen = 0;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            int fieldId = ThriftCompactReader.fieldId(header);
            switch (fieldId) {
                case 1: // isAdjustedToUTC (required)
                    isAdjustedToUTC = reader.requireBooleanField(header);
                    seen |= 1L << fieldId;
                    break;
                case 2: // unit (required union)
                    reader.requireField(header, Codes.STRUCT);
                    unit = readTimeUnit(reader);
                    seen |= 1L << fieldId;
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        ThriftCompactReader.requireFields(ThriftStruct.TIME_TYPE, seen, BOTH_FIELDS);

        return LogicalType.time(isAdjustedToUTC, unit);
    }

    private static LogicalType.TimestampType readTimestampType(ThriftCompactReader reader, int header) {
        reader.requireField(header, Codes.STRUCT);
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
        long seen = 0;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            int fieldId = ThriftCompactReader.fieldId(header);
            switch (fieldId) {
                case 1: // isAdjustedToUTC (required)
                    isAdjustedToUTC = reader.requireBooleanField(header);
                    seen |= 1L << fieldId;
                    break;
                case 2: // unit (required union)
                    reader.requireField(header, Codes.STRUCT);
                    unit = readTimeUnit(reader);
                    seen |= 1L << fieldId;
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        ThriftCompactReader.requireFields(ThriftStruct.TIMESTAMP_TYPE, seen, BOTH_FIELDS);

        return LogicalType.timestamp(isAdjustedToUTC, unit);
    }

    private static LogicalType.IntType readIntType(ThriftCompactReader reader, int header) {
        reader.requireField(header, Codes.STRUCT);
        int saved = reader.pushFieldIdContext(ThriftStruct.INT_TYPE);
        try {
            return readIntTypeInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static LogicalType.IntType readIntTypeInternal(ThriftCompactReader reader) {
        int bitWidth = -1;
        boolean isSigned = true;
        long seen = 0;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            int fieldId = ThriftCompactReader.fieldId(header);
            switch (fieldId) {
                case 1: // bitWidth (required)
                    reader.requireField(header, Codes.BYTE);
                    bitWidth = reader.readByte();
                    seen |= 1L << fieldId;
                    break;
                case 2: // isSigned (required)
                    isSigned = reader.requireBooleanField(header);
                    seen |= 1L << fieldId;
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        ThriftCompactReader.requireFields(ThriftStruct.INT_TYPE, seen, BOTH_FIELDS);

        // Validate that the width is one of the four the annotation defines: the record rejects
        // any other as a caller error, which a file carrying one is not.
        if (bitWidth != 8 && bitWidth != 16 && bitWidth != 32 && bitWidth != 64) {
            throw new ParquetReadException("Invalid IntType: bitWidth=" + bitWidth);
        }

        return LogicalType.intType(bitWidth, isSigned);
    }

    private static LogicalType.VariantType readVariantType(ThriftCompactReader reader, int header) {
        reader.requireField(header, Codes.STRUCT);
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

        if (specVersion < 1) {
            throw new ParquetReadException("Invalid VariantType: specVersion=" + specVersion);
        }
        return LogicalType.variant(specVersion);
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

    private static LogicalType.GeometryType readGeometryType(ThriftCompactReader reader, int header) {
        reader.requireField(header, Codes.STRUCT);
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

        // An absent CRS is the format's default, which the factory substitutes.
        return LogicalType.geometry(crs);
    }

    private static LogicalType.GeographyType readGeographyType(ThriftCompactReader reader, int header) {
        reader.requireField(header, Codes.STRUCT);
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

        // An absent CRS or algorithm is the format's default, which the factory substitutes.
        return LogicalType.geography(crs, edgeInterpolation);
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
