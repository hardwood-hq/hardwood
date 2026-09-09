/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.SchemaNode;

/// The check `getDate`, `getUuid` and `getInterval` need, and no other accessor does.
///
/// Asking a typed accessor for a type its column does not hold is normally caught by
/// a cast the decode already performs, and #971 is the sweep that gives that failure
/// a message; validating ahead of it was measured at 4% per accessor, above the bar
/// `_designs/EXCEPTION_MODEL.md` sets, so nothing here duplicates it.
///
/// These three are the accessors no cast can catch. A `DATE`, a bare `INT32` and a
/// `TIME(MILLIS)` are one `int[]`; every `FIXED_LEN_BYTE_ARRAY(16)` is one
/// [BinaryBatchValues], as is every 12-byte payload including an `INT96`. Each cast
/// on the way to the value succeeds, and the accessor returns a decode of the wrong
/// bytes. There is no exception here to improve — only one to raise.
///
/// The rejection is a caller's error, so per `_designs/EXCEPTION_MODEL.md` it carries
/// the file name and nothing else of the read's position, and names the column in the
/// problem, as [NestedBatchIndex#requireFloatAccess] does.
final class LogicalAccessorKind {

    private LogicalAccessorKind() {
    }

    static void requireDate(String fileName, SchemaNode.PrimitiveNode leaf) {
        requireDate(fileName, leaf.name(), leaf.type(), leaf.logicalType());
    }

    static void requireDate(
            String fileName, String column, PhysicalType type, LogicalType logicalType) {
        if (!(logicalType instanceof LogicalType.DateType)) {
            throw rejection(fileName, column, type, logicalType, "a date");
        }
    }

    static void requireUuid(String fileName, SchemaNode.PrimitiveNode leaf) {
        requireUuid(fileName, leaf.name(), leaf.type(), leaf.logicalType());
    }

    static void requireUuid(
            String fileName, String column, PhysicalType type, LogicalType logicalType) {
        if (!(logicalType instanceof LogicalType.UuidType)) {
            throw rejection(fileName, column, type, logicalType, "a UUID");
        }
    }

    static void requireInterval(String fileName, SchemaNode.PrimitiveNode leaf) {
        requireInterval(fileName, leaf.name(), leaf.type(), leaf.logicalType());
    }

    static void requireInterval(
            String fileName, String column, PhysicalType type, LogicalType logicalType) {
        if (!(logicalType instanceof LogicalType.IntervalType)) {
            throw rejection(fileName, column, type, logicalType, "an interval");
        }
    }

    private static IllegalArgumentException rejection(
            String fileName, String column, PhysicalType type, LogicalType logicalType, String wanted) {
        return new IllegalArgumentException(ExceptionContext.filePrefix(fileName)
                + "Column '" + column + "' is " + type
                + (logicalType == null ? "" : " annotated " + logicalType)
                + ", which cannot be read as " + wanted);
    }
}
