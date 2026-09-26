/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.row.PqInterval;
import dev.hardwood.schema.SchemaNode;

/// The leaf decode every row-reader accessor shares: `FlatRowReader`, `NestedBatchDataView`
/// and the `PqStruct`, `PqList` and `PqMap` flyweights, in both the generic and the typed
/// shape.
///
/// The decode table itself lives in [LogicalTypeConverter], which knows only a
/// physical type and an annotation, and [LeafKind] states which of its entry points
/// a leaf goes to. What is added here is the read from a batch's value array to a
/// decoded value, and for the nested path the `SchemaNode` unwrap.
///
/// Every `*At` method takes the column's value array as the batch holds it (`int[]`,
/// `long[]` or [BinaryBatchValues]) and the value's index in it, so a reader addresses the
/// array however its layout requires and hands over only the result. The `*At` methods
/// carry no guard: the caller has established that the accessor fits the column, through
/// [LogicalAccessorKind] or [TimestampAccessorKind]. The overloads taking a unit or a scale
/// serve a caller decoding a whole column of elements, which reads it off the annotation
/// once for the whole view; the overloads taking the annotation read it per value.
///
/// [#decode] serves a generic accessor, which returns whatever the column holds and so
/// starts from a boxed `Object`. The `read*` methods serve a typed accessor on the nested
/// path, unwrapping the leaf's `SchemaNode` and reading the stored primitive out of the
/// batch without boxing it. `readDate`, `readString`, `readUuid` and `readInterval` apply
/// their [LogicalAccessorKind] guard first; the caller checks the timestamp kind through
/// [TimestampAccessorKind] before `readTimestamp` or `readLocalTimestamp`, and
/// `readTime` and `readDecimal` rely on the cast of the annotation.
///
/// A column the accessor does not fit is caught by the cast on the way to the value,
/// which is #971's population — except in the cases [LogicalAccessorKind] covers, where
/// every cast succeeds and only a check on the annotation is left.
final class LeafDecoder {

    private LeafDecoder() {
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
        if (kind == LeafKind.GROUP) {
            return rawValue;
        }
        SchemaNode.PrimitiveNode primitive = (SchemaNode.PrimitiveNode) schema;
        return decode(rawValue, kind, primitive.type(), primitive.logicalType());
    }

    /// Decode a present leaf value of the given kind, for a caller that classified the
    /// column once, as `FlatRowReader` does at construction. A `STRING` leaf decodes from
    /// the raw bytes here; a caller holding the batch reads the cached `String` instead.
    static Object decode(Object rawValue, LeafKind kind, PhysicalType type, LogicalType logicalType) {
        return switch (kind) {
            case GROUP, RAW -> rawValue;
            case INT96_TIMESTAMP -> LogicalTypeConverter.int96ToInstant((byte[]) rawValue);
            case STRING, CONVERT -> LogicalTypeConverter.convert(rawValue, type, logicalType);
        };
    }

    // ==================== Typed reads on the nested path ====================

    static LocalDate readDate(NestedBatchIndex batch, int projCol, int idx, SchemaNode.PrimitiveNode leaf) {
        LogicalAccessorKind.requireDate(batch.fileName, leaf);
        return dateAt(batch.valueArrays[projCol], idx);
    }

    static LocalTime readTime(NestedBatchIndex batch, int projCol, int idx, SchemaNode.PrimitiveNode leaf) {
        return timeAt(batch.valueArrays[projCol], idx, leaf.type(), leaf.logicalType());
    }

    /// The [Instant] a UTC-adjusted `TIMESTAMP` leaf holds, or the one a legacy `INT96`
    /// leaf holds by convention. The caller has already established through
    /// [TimestampAccessorKind] that the leaf is the UTC-adjusted kind.
    static Instant readTimestamp(NestedBatchIndex batch, int projCol, int idx, SchemaNode.PrimitiveNode leaf) {
        return timestampAt(batch.valueArrays[projCol], idx, leaf.type(), leaf.logicalType());
    }

    static LocalDateTime readLocalTimestamp(
            NestedBatchIndex batch, int projCol, int idx, SchemaNode.PrimitiveNode leaf) {
        return localTimestampAt(batch.valueArrays[projCol], idx, leaf.type(), leaf.logicalType());
    }

    static BigDecimal readDecimal(NestedBatchIndex batch, int projCol, int idx, SchemaNode.PrimitiveNode leaf) {
        return decimalAt(batch.valueArrays[projCol], idx, leaf.type(), leaf.logicalType());
    }

    /// The text a leaf holds, once [LogicalAccessorKind] has established that its stored
    /// bytes are the UTF-8 encoding of a string rather than a payload read as something else.
    static String readString(NestedBatchIndex batch, int projCol, int idx, SchemaNode.PrimitiveNode leaf) {
        LogicalAccessorKind.requireText(batch.fileName, leaf);
        return batch.getString(projCol, idx);
    }

    static UUID readUuid(NestedBatchIndex batch, int projCol, int idx, SchemaNode.PrimitiveNode leaf) {
        LogicalAccessorKind.requireUuid(batch.fileName, leaf);
        return uuidAt(batch.valueArrays[projCol], idx);
    }

    static PqInterval readInterval(NestedBatchIndex batch, int projCol, int idx, SchemaNode.PrimitiveNode leaf) {
        LogicalAccessorKind.requireInterval(batch.fileName, leaf);
        return intervalAt(batch.valueArrays[projCol], idx);
    }

    /// Whether `leaf` is the legacy `INT96` timestamp, which carries the value in a
    /// 12-byte payload rather than an `int64` and no annotation to decode through.
    static boolean isInt96Timestamp(SchemaNode.PrimitiveNode leaf) {
        return isInt96Timestamp(leaf.type(), leaf.logicalType());
    }

    static boolean isInt96Timestamp(PhysicalType type, LogicalType logicalType) {
        return logicalType == null && type == PhysicalType.INT96;
    }

    // ==================== Guardless reads out of a column's value array ====================

    static LocalDate dateAt(Object values, int idx) {
        return LogicalTypeConverter.intToDate(((int[]) values)[idx]);
    }

    static LocalTime timeAt(Object values, int idx, PhysicalType type, LogicalType logicalType) {
        return timeAt(values, idx, type, ((LogicalType.TimeType) logicalType).unit());
    }

    static LocalTime timeAt(Object values, int idx, PhysicalType type, LogicalType.TimeUnit unit) {
        long rawValue = type == PhysicalType.INT32
                ? ((int[]) values)[idx]
                : ((long[]) values)[idx];
        return LogicalTypeConverter.longToTime(rawValue, unit);
    }

    /// The instant a UTC-adjusted `TIMESTAMP` leaf stores, or the one a legacy `INT96` leaf
    /// stores by convention, as `type` and `logicalType` say.
    static Instant timestampAt(Object values, int idx, PhysicalType type, LogicalType logicalType) {
        if (isInt96Timestamp(type, logicalType)) {
            return int96TimestampAt(values, idx);
        }
        return timestampAt(values, idx, type, ((LogicalType.TimestampType) logicalType).unit());
    }

    /// The instant a UTC-adjusted `TIMESTAMP` leaf stores, in an `INT64` or in a
    /// `FIXED_LEN_BYTE_ARRAY(12)` as `type` says.
    static Instant timestampAt(Object values, int idx, PhysicalType type, LogicalType.TimeUnit unit) {
        if (type == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            return ((BinaryBatchValues) values).flba12InstantAt(idx, unit);
        }
        return LogicalTypeConverter.longToTimestamp(((long[]) values)[idx], unit);
    }

    static Instant int96TimestampAt(Object values, int idx) {
        return LogicalTypeConverter.int96ToInstant(((BinaryBatchValues) values).byteArrayAt(idx));
    }

    static LocalDateTime localTimestampAt(Object values, int idx, PhysicalType type, LogicalType logicalType) {
        return localTimestampAt(values, idx, type, ((LogicalType.TimestampType) logicalType).unit());
    }

    /// The wall clock a local `TIMESTAMP` leaf stores; see [#timestampAt(Object, int, PhysicalType, LogicalType.TimeUnit)].
    static LocalDateTime localTimestampAt(Object values, int idx, PhysicalType type, LogicalType.TimeUnit unit) {
        if (type == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            return ((BinaryBatchValues) values).flba12LocalDateTimeAt(idx, unit);
        }
        return LogicalTypeConverter.longToLocalTimestamp(((long[]) values)[idx], unit);
    }

    static BigDecimal decimalAt(Object values, int idx, PhysicalType type, LogicalType logicalType) {
        return decimalAt(values, idx, type, ((LogicalType.DecimalType) logicalType).scale());
    }

    static BigDecimal decimalAt(Object values, int idx, PhysicalType type, int scale) {
        return switch (type) {
            case INT32 -> LogicalTypeConverter.longToDecimal(((int[]) values)[idx], scale);
            case INT64 -> LogicalTypeConverter.longToDecimal(((long[]) values)[idx], scale);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> ((BinaryBatchValues) values).decimalAt(idx, scale);
            default -> throw new IllegalArgumentException("Unexpected physical type for DECIMAL: " + type);
        };
    }

    static UUID uuidAt(Object values, int idx) {
        return ((BinaryBatchValues) values).uuidAt(idx);
    }

    static PqInterval intervalAt(Object values, int idx) {
        return ((BinaryBatchValues) values).intervalAt(idx);
    }

    /// The float a `FLOAT16` leaf stores. The caller has ruled out a plain `FLOAT` column and
    /// established through [LogicalAccessorKind#requireFloat16] that this one is annotated
    /// `FLOAT16`.
    static float float16At(Object values, int idx) {
        return ((BinaryBatchValues) values).float16At(idx);
    }
}
