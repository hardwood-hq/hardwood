/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.internal.schema.AnnotationKind;
import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.LogicalType.TimeUnit;

/// Writer for the `LogicalType` union to Thrift Compact Protocol, the inverse of
/// [LogicalTypeReader].
///
/// A Thrift union is a struct with exactly one field set: the field id selects the member and
/// the value is the member's struct, empty for the unparameterized types. Every nested struct
/// brackets its fields with a field-id context, because compact-protocol field ids are
/// delta-encoded per struct.
///
/// The field id of each member is the one [AnnotationKind] states, which [LogicalTypeReader] reads
/// by too. `INTERVAL` has no union member — parquet.thrift reserves field 9 for it but never
/// defined the struct — so it is annotated by the legacy `converted_type` alone and is rejected
/// here.
public class LogicalTypeWriter {

    public static void write(ThriftCompactWriter writer, LogicalType logicalType) {
        short saved = writer.pushFieldIdContext();
        try {
            writeMember(writer, logicalType);
            writer.writeFieldStop();
        }
        finally {
            writer.popFieldIdContext(saved);
        }
    }

    /// Writes the union member: the field id [AnnotationKind] states for the annotation, carrying
    /// the member struct.
    private static void writeMember(ThriftCompactWriter writer, LogicalType logicalType) {
        AnnotationKind kind = AnnotationKind.of(logicalType);
        if (!kind.hasUnionMember()) {
            throw new IllegalArgumentException(logicalType + " has no LogicalType union member and is written as the "
                    + "legacy converted_type only");
        }
        writer.writeFieldBegin(kind.unionField(), FieldType.STRUCT);
        short saved = writer.pushFieldIdContext();
        writeMemberFields(writer, kind, logicalType);
        writer.writeFieldStop();
        writer.popFieldIdContext(saved);
    }

    /// Writes the fields of the member struct, none for an unparameterized member.
    private static void writeMemberFields(ThriftCompactWriter writer, AnnotationKind kind, LogicalType logicalType) {
        switch (kind) {
            case DECIMAL -> writeDecimal(writer, (LogicalType.DecimalType) logicalType);
            case TIME -> {
                LogicalType.TimeType time = (LogicalType.TimeType) logicalType;
                writeTimeFields(writer, time.isAdjustedToUTC(), time.unit());
            }
            case TIMESTAMP -> {
                LogicalType.TimestampType stamp = (LogicalType.TimestampType) logicalType;
                writeTimeFields(writer, stamp.isAdjustedToUTC(), stamp.unit());
            }
            case INT -> writeInt(writer, (LogicalType.IntType) logicalType);
            case VARIANT -> writeVariant(writer, (LogicalType.VariantType) logicalType);
            case GEOMETRY -> writeCrs(writer, ((LogicalType.GeometryType) logicalType).crs());
            case GEOGRAPHY -> writeGeography(writer, (LogicalType.GeographyType) logicalType);
            case STRING, MAP, LIST, ENUM, DATE, NULL, JSON, BSON, UUID, FLOAT16 -> {
            }
            case INTERVAL -> throw new IllegalStateException(kind + " has no union member to write");
        }
    }

    /// Writes an unparameterized struct: an empty struct under `fieldId`.
    private static void writeEmpty(ThriftCompactWriter writer, int fieldId) {
        writer.writeFieldBegin(fieldId, FieldType.STRUCT);
        short saved = writer.pushFieldIdContext();
        writer.writeFieldStop();
        writer.popFieldIdContext(saved);
    }

    private static void writeDecimal(ThriftCompactWriter writer, LogicalType.DecimalType decimal) {
        writer.writeFieldBegin(1, FieldType.I32);
        writer.writeI32(decimal.scale());
        writer.writeFieldBegin(2, FieldType.I32);
        writer.writeI32(decimal.precision());
    }

    /// `TimeType` and `TimestampType` are structurally identical: a UTC-adjustment flag and a
    /// unit.
    private static void writeTimeFields(ThriftCompactWriter writer, boolean isAdjustedToUTC, TimeUnit unit) {
        writer.writeBool(1, isAdjustedToUTC);
        writer.writeFieldBegin(2, FieldType.STRUCT);
        short unitContext = writer.pushFieldIdContext();
        writeEmpty(writer, unitFieldId(unit));
        writer.writeFieldStop();
        writer.popFieldIdContext(unitContext);
    }

    private static int unitFieldId(TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> 1;
            case MICROS -> 2;
            case NANOS -> 3;
        };
    }

    private static void writeInt(ThriftCompactWriter writer, LogicalType.IntType integer) {
        writer.writeFieldBegin(1, FieldType.BYTE);
        writer.writeByte((byte) integer.bitWidth());
        writer.writeBool(2, integer.isSigned());
    }

    private static void writeVariant(ThriftCompactWriter writer, LogicalType.VariantType variant) {
        writer.writeFieldBegin(1, FieldType.BYTE);
        writer.writeByte(toSpecVersionByte(variant.specVersion()));
    }

    /// The spec version is an `i8` on the wire, so a version that does not fit is a caller
    /// error rather than a value silently truncated into a different version.
    private static byte toSpecVersionByte(int specVersion) {
        if (specVersion > Byte.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Variant specification version does not fit in an i8: " + specVersion);
        }
        return (byte) specVersion;
    }

    private static void writeGeography(ThriftCompactWriter writer, LogicalType.GeographyType geography) {
        writeCrs(writer, geography.crs());
        if (geography.edgeInterpolation() != null) {
            // `algorithm` is a Thrift enum, not a union, so it is an i32 of the enum's value.
            writer.writeFieldBegin(2, FieldType.I32);
            writer.writeI32(ThriftEnumLookup.thriftValue(geography.edgeInterpolation()));
        }
    }

    /// The CRS is optional; an absent one is omitted, and the reader substitutes the
    /// `OGC:CRS84` default.
    private static void writeCrs(ThriftCompactWriter writer, String crs) {
        if (crs != null) {
            writer.writeFieldBegin(1, FieldType.BINARY);
            writer.writeString(crs);
        }
    }

}
