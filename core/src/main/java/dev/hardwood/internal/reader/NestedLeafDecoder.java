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

/// The leaf decode the `PqStruct`, `PqList` and `PqMap` flyweights share, in both the
/// generic and the typed shape.
///
/// The decode table itself lives in [LogicalTypeConverter], which knows only a
/// physical type and an annotation, and [LeafKind] states which of its entry points
/// a leaf goes to. What is added here is the `SchemaNode` unwrap the flyweights need,
/// and the reads that go straight from a column array to a decoded value.
///
/// [#decode] serves a generic accessor, which returns whatever the column holds and
/// so starts from a boxed `Object`. The `read*` methods serve a typed accessor: each
/// reads the stored primitive out of the column array without boxing it. A caller
/// decoding a whole column of elements calls the matching `*At` method per element,
/// having read the unit or the scale off the annotation once for the whole view.
///
/// A column the accessor does not fit is caught by the cast on the way to the value,
/// which is #971's population — except in the three cases [LogicalAccessorKind]
/// covers, where every cast succeeds and only a check on the annotation is left.
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
        return switch (LeafKind.of(schema)) {
            case GROUP, RAW -> rawValue;
            case INT96_TIMESTAMP -> LogicalTypeConverter.int96ToInstant((byte[]) rawValue);
            case STRING, CONVERT -> {
                SchemaNode.PrimitiveNode primitive = (SchemaNode.PrimitiveNode) schema;
                yield LogicalTypeConverter.convert(rawValue, primitive.type(), primitive.logicalType());
            }
        };
    }

    // ==================== Typed reads: guard, then read the stored primitive ====================

    static LocalDate readDate(NestedBatchIndex batch, int projCol, int idx, SchemaNode.PrimitiveNode leaf) {
        LogicalAccessorKind.requireDate(batch.fileName, leaf);
        return dateAt(batch, projCol, idx);
    }

    static LocalTime readTime(NestedBatchIndex batch, int projCol, int idx, SchemaNode.PrimitiveNode leaf) {
        return timeAt(batch, projCol, idx, leaf.type(),
                ((LogicalType.TimeType) leaf.logicalType()).unit());
    }

    /// The [Instant] a UTC-adjusted `TIMESTAMP` leaf holds, or the one a legacy `INT96`
    /// leaf holds by convention. The caller has already established through
    /// [TimestampAccessorKind] that the leaf is the UTC-adjusted kind.
    static Instant readTimestamp(NestedBatchIndex batch, int projCol, int idx, SchemaNode.PrimitiveNode leaf) {
        if (isInt96Timestamp(leaf)) {
            return int96TimestampAt(batch, projCol, idx);
        }
        return timestampAt(batch, projCol, idx,
                ((LogicalType.TimestampType) leaf.logicalType()).unit());
    }

    static LocalDateTime readLocalTimestamp(
            NestedBatchIndex batch, int projCol, int idx, SchemaNode.PrimitiveNode leaf) {
        return localTimestampAt(batch, projCol, idx,
                ((LogicalType.TimestampType) leaf.logicalType()).unit());
    }

    static BigDecimal readDecimal(NestedBatchIndex batch, int projCol, int idx, SchemaNode.PrimitiveNode leaf) {
        return decimalAt(batch, projCol, idx, leaf.type(),
                ((LogicalType.DecimalType) leaf.logicalType()).scale());
    }

    /// The text a leaf holds, once [LogicalAccessorKind] has established that its stored
    /// bytes are the UTF-8 encoding of a string rather than a payload read as something else.
    static String readString(NestedBatchIndex batch, int projCol, int idx, SchemaNode.PrimitiveNode leaf) {
        LogicalAccessorKind.requireText(batch.fileName, leaf);
        return batch.getString(projCol, idx);
    }

    static UUID readUuid(NestedBatchIndex batch, int projCol, int idx, SchemaNode.PrimitiveNode leaf) {
        LogicalAccessorKind.requireUuid(batch.fileName, leaf);
        return uuidAt(batch, projCol, idx);
    }

    static PqInterval readInterval(NestedBatchIndex batch, int projCol, int idx, SchemaNode.PrimitiveNode leaf) {
        LogicalAccessorKind.requireInterval(batch.fileName, leaf);
        return intervalAt(batch, projCol, idx);
    }

    /// Whether `leaf` is the legacy `INT96` timestamp, which carries the value in a
    /// 12-byte payload rather than an `int64` and no annotation to decode through.
    static boolean isInt96Timestamp(SchemaNode.PrimitiveNode leaf) {
        return leaf.logicalType() == null && leaf.type() == PhysicalType.INT96;
    }

    // ==================== Guardless reads, for a caller that hoisted the guard ====================

    static LocalDate dateAt(NestedBatchIndex batch, int projCol, int idx) {
        return LogicalTypeConverter.intToDate(((int[]) batch.valueArrays[projCol])[idx]);
    }

    static LocalTime timeAt(
            NestedBatchIndex batch, int projCol, int idx, PhysicalType type, LogicalType.TimeUnit unit) {
        long rawValue = type == PhysicalType.INT32
                ? ((int[]) batch.valueArrays[projCol])[idx]
                : ((long[]) batch.valueArrays[projCol])[idx];
        return LogicalTypeConverter.longToTime(rawValue, unit);
    }

    static Instant timestampAt(NestedBatchIndex batch, int projCol, int idx, LogicalType.TimeUnit unit) {
        return LogicalTypeConverter.longToTimestamp(((long[]) batch.valueArrays[projCol])[idx], unit);
    }

    static Instant int96TimestampAt(NestedBatchIndex batch, int projCol, int idx) {
        return LogicalTypeConverter.int96ToInstant(batch.getBinary(projCol, idx));
    }

    static LocalDateTime localTimestampAt(
            NestedBatchIndex batch, int projCol, int idx, LogicalType.TimeUnit unit) {
        return LogicalTypeConverter.longToLocalTimestamp(((long[]) batch.valueArrays[projCol])[idx], unit);
    }

    static BigDecimal decimalAt(NestedBatchIndex batch, int projCol, int idx, PhysicalType type, int scale) {
        return switch (type) {
            case INT32 -> LogicalTypeConverter.longToDecimal(((int[]) batch.valueArrays[projCol])[idx], scale);
            case INT64 -> LogicalTypeConverter.longToDecimal(((long[]) batch.valueArrays[projCol])[idx], scale);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY ->
                    ((BinaryBatchValues) batch.valueArrays[projCol]).decimalAt(idx, scale);
            default -> throw new IllegalArgumentException("Unexpected physical type for DECIMAL: " + type);
        };
    }

    static UUID uuidAt(NestedBatchIndex batch, int projCol, int idx) {
        return ((BinaryBatchValues) batch.valueArrays[projCol]).uuidAt(idx);
    }

    static PqInterval intervalAt(NestedBatchIndex batch, int projCol, int idx) {
        return ((BinaryBatchValues) batch.valueArrays[projCol]).intervalAt(idx);
    }
}
