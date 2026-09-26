/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.schema.TextColumns;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.SchemaNode;

/// The check `getDate`, `getUuid`, `getInterval`, `getString` and the `FLOAT16` branch of
/// `getFloat` need, and no other accessor does.
///
/// Asking a typed accessor for a type its column does not hold is normally caught by
/// a cast the decode already performs, and #971 is the sweep that gives that failure
/// a message; validating ahead of it would cost every accessor call
/// (`_designs/EXCEPTION_MODEL.md`), so nothing here duplicates it.
///
/// These are the accessors no cast can catch. A `DATE`, a bare `INT32` and a
/// `TIME(MILLIS)` are one `int[]`; every `FIXED_LEN_BYTE_ARRAY(16)` is one
/// [BinaryBatchValues], as is every 12-byte payload including an `INT96`. Each cast
/// on the way to the value succeeds, and the accessor returns a decode of the wrong
/// bytes. `getString` is the widest of them: every column held as bytes decodes to
/// *some* text, so without a check it returns a `DECIMAL`'s or an `INT96`'s stored
/// bytes as characters. `getFloat` reads any byte-array column that is not a `FLOAT` as
/// a `FLOAT16`, two bytes at a time. There is no exception here to improve — only one to
/// raise.
///
/// The rejection is a caller's error, so per `_designs/EXCEPTION_MODEL.md` it carries
/// the file name and nothing else of the read's position, and names the column in the
/// problem.
public final class LogicalAccessorKind {

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

    static void requireFloat16(String fileName, SchemaNode.PrimitiveNode leaf) {
        requireFloat16(fileName, leaf.name(), leaf.type(), leaf.logicalType());
    }

    /// Refuses a column `getFloat` cannot read, once its `FLOAT` fast path has been ruled
    /// out: the one other column it reads is a `FLOAT16`. A column whose annotation its
    /// width cannot carry arrives unannotated, the annotation having been dropped where the
    /// schema was built, so it is refused as what it physically is.
    static void requireFloat16(
            String fileName, String column, PhysicalType type, LogicalType logicalType) {
        if (!(logicalType instanceof LogicalType.Float16Type)) {
            throw rejection(fileName, column, type, logicalType, "a float");
        }
    }

    static void requireText(String fileName, SchemaNode.PrimitiveNode leaf) {
        requireText(fileName, leaf.name(), leaf.type(), leaf.logicalType());
    }

    /// Refuses a column whose stored bytes are not the UTF-8 encoding of a string.
    ///
    /// [TextColumns] answers for both this and the `String` filter literal, which is what
    /// keeps a value `getString` cannot read out of a column from being one a `String` can
    /// filter it by.
    public static void requireText(
            String fileName, String column, PhysicalType type, LogicalType logicalType) {
        if (!TextColumns.holdsText(type, logicalType)) {
            throw notText(fileName, column, type, logicalType);
        }
    }

    /// The rejection [#requireText] raises, for a caller that has already established the
    /// column does not hold text: `FlatRowReader` asks [TextColumns] once per column and
    /// keeps the question off the accessor itself.
    static IllegalArgumentException notText(
            String fileName, String column, PhysicalType type, LogicalType logicalType) {
        return rejection(fileName, column, type, logicalType, "a string");
    }

    private static IllegalArgumentException rejection(
            String fileName, String column, PhysicalType type, LogicalType logicalType, String wanted) {
        return new IllegalArgumentException(ExceptionContext.filePrefix(fileName)
                + "Column '" + column + "' is " + type
                + (logicalType == null ? "" : " annotated " + logicalType)
                + ", which cannot be read as " + wanted);
    }
}
