/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

import dev.hardwood.internal.conversion.PhysicalValueConverter;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.row.PqInterval;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.writer.ColumnBatch;
import dev.hardwood.writer.PrecisionLossPolicy;

/// One leaf column in the row-oriented layer's plan: the setters the builders expose, the
/// conversion each one performs against the column's declared type, and the [LeafStage] the
/// converted values land in.
///
/// Every setter validates against the column as declared and throws before staging anything,
/// so a value the column cannot hold never reaches the encoder. A `null` value is accepted
/// wherever the reader would return one: it stages a null, or throws if the column is
/// `REQUIRED`.
final class RowLeafNode extends RowNode {

    private final int columnIndex;
    private final PhysicalType physicalType;
    private final LogicalType logicalType;
    private final int typeLength;
    private final boolean optional;
    private final LeafStage stage;
    private final PrecisionLossPolicy precisionLossPolicy;

    /// The range the column's annotation declares, which the physical setters check the value
    /// against — the same object [ColumnBatch] scans its arrays with, so the two write APIs
    /// cannot disagree about which values the column may hold.
    private final LogicalTypeValueRange range;

    RowLeafNode(String path, ColumnSchema column, PrecisionLossPolicy precisionLossPolicy) {
        super(path);
        this.precisionLossPolicy = precisionLossPolicy;
        this.columnIndex = column.columnIndex();
        this.physicalType = column.type();
        this.logicalType = column.logicalType();
        this.typeLength = column.typeLength() == null ? -1 : column.typeLength();
        this.optional = column.repetitionType() == RepetitionType.OPTIONAL;
        this.stage = LeafStage.forType(physicalType, typeLength, logicalType);
        this.range = LogicalTypeValueRange.of(column);
    }

    LeafStage stage() {
        return stage;
    }

    // ==================== Physical setters ====================

    void setInt(int value) {
        require(physicalType == PhysicalType.INT32, "setInt", "an INT32 column");
        requireValueAllowed("setInt");
        checkRange(value);
        ((LeafStage.IntStage) stage).append(value);
    }

    void setLong(long value) {
        require(physicalType == PhysicalType.INT64, "setLong", "an INT64 column");
        requireValueAllowed("setLong");
        checkRange(value);
        ((LeafStage.LongStage) stage).append(value);
    }

    void setFloat(float value) {
        require(physicalType == PhysicalType.FLOAT, "setFloat", "a FLOAT column");
        requireValueAllowed("setFloat");
        ((LeafStage.FloatStage) stage).append(value);
    }

    void setDouble(double value) {
        require(physicalType == PhysicalType.DOUBLE, "setDouble", "a DOUBLE column");
        requireValueAllowed("setDouble");
        ((LeafStage.DoubleStage) stage).append(value);
    }

    void setBoolean(boolean value) {
        require(physicalType == PhysicalType.BOOLEAN, "setBoolean", "a BOOLEAN column");
        requireValueAllowed("setBoolean");
        ((LeafStage.BooleanStage) stage).append(value);
    }

    void setBinary(byte[] value) {
        require(physicalType == PhysicalType.BYTE_ARRAY || physicalType == PhysicalType.FIXED_LEN_BYTE_ARRAY,
                "setBinary", "a BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY column");
        if (value == null) {
            setNull();
            return;
        }
        appendBinary(value);
    }

    void setString(String value) {
        require(physicalType == PhysicalType.BYTE_ARRAY && isStringAnnotated(),
                "setString", "a BYTE_ARRAY column annotated STRING, ENUM or JSON, or unannotated");
        if (value == null) {
            setNull();
            return;
        }
        appendBinary(PhysicalValueConverter.stringToBytes(value));
    }

    // ==================== Logical setters ====================

    void setDate(LocalDate value) {
        require(physicalType == PhysicalType.INT32 && logicalType instanceof LogicalType.DateType,
                "setDate", "an INT32 column annotated DATE");
        if (value == null) {
            setNull();
            return;
        }
        ((LeafStage.IntStage) stage).append(PhysicalValueConverter.dateToInt(path, value));
    }

    void setTime(LocalTime value) {
        require(logicalType instanceof LogicalType.TimeType, "setTime", "a column annotated TIME");
        if (value == null) {
            setNull();
            return;
        }
        LogicalType.TimeType timeType = (LogicalType.TimeType) logicalType;
        long units = PhysicalValueConverter.timeToLong(path, value, timeType.unit(), precisionLossPolicy);
        appendTimeUnits(units);
    }

    void setTimestamp(Instant value) {
        LogicalType.TimestampType timestampType = requireTimestamp("setTimestamp", true);
        if (value == null) {
            setNull();
            return;
        }
        if (physicalType == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            appendBinary(PhysicalValueConverter.timestampToFixed12(path, value, timestampType.unit(),
                    precisionLossPolicy));
            return;
        }
        ((LeafStage.LongStage) stage).append(
                PhysicalValueConverter.timestampToLong(path, value, timestampType.unit(), precisionLossPolicy));
    }

    void setLocalTimestamp(LocalDateTime value) {
        LogicalType.TimestampType timestampType = requireTimestamp("setLocalTimestamp", false);
        if (value == null) {
            setNull();
            return;
        }
        if (physicalType == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            appendBinary(PhysicalValueConverter.localTimestampToFixed12(path, value, timestampType.unit(),
                    precisionLossPolicy));
            return;
        }
        ((LeafStage.LongStage) stage).append(
                PhysicalValueConverter.localTimestampToLong(path, value, timestampType.unit(), precisionLossPolicy));
    }

    void setDecimal(BigDecimal value) {
        require(logicalType instanceof LogicalType.DecimalType, "setDecimal", "a column annotated DECIMAL");
        if (value == null) {
            setNull();
            return;
        }
        LogicalType.DecimalType decimalType = (LogicalType.DecimalType) logicalType;
        switch (physicalType) {
            case INT32 -> ((LeafStage.IntStage) stage)
                    .append(PhysicalValueConverter.decimalToInt(path, value, decimalType, precisionLossPolicy));
            case INT64 -> ((LeafStage.LongStage) stage)
                    .append(PhysicalValueConverter.decimalToLong(path, value, decimalType, precisionLossPolicy));
            case BYTE_ARRAY -> appendBinary(PhysicalValueConverter.decimalToBytes(path, value, decimalType, -1, precisionLossPolicy));
            case FIXED_LEN_BYTE_ARRAY -> appendBinary(
                    PhysicalValueConverter.decimalToBytes(path, value, decimalType, typeLength,
                            precisionLossPolicy));
            default -> throw wrongType("setDecimal", "an INT32, INT64, BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY column");
        }
    }

    void setUuid(UUID value) {
        require(physicalType == PhysicalType.FIXED_LEN_BYTE_ARRAY && logicalType instanceof LogicalType.UuidType,
                "setUuid", "a FIXED_LEN_BYTE_ARRAY column annotated UUID");
        if (value == null) {
            setNull();
            return;
        }
        appendBinary(PhysicalValueConverter.uuidToBytes(value));
    }

    void setInterval(PqInterval value) {
        require(physicalType == PhysicalType.FIXED_LEN_BYTE_ARRAY
                        && logicalType instanceof LogicalType.IntervalType,
                "setInterval", "a FIXED_LEN_BYTE_ARRAY column annotated INTERVAL");
        if (value == null) {
            setNull();
            return;
        }
        appendBinary(PhysicalValueConverter.intervalToBytes(path, value));
    }

    void setNull() {
        if (!optional) {
            throw new RejectedRecordException(path, requiredMessage());
        }
        stage.appendNull();
    }

    // ==================== Scope bookkeeping ====================

    @Override
    void appendNullInstance() {
        setNull();
    }

    @Override
    void appendAbsentInstance() {
        // An OPTIONAL leaf can say what the slot is: null. The ancestor's definition level already
        // caps the leaf's, so the null bit changes nothing on the wire — but it tells the batch the
        // slot carries no value, which is what an UNKNOWN column requires of every row and what
        // spares every other annotation from range-checking a placeholder. A REQUIRED leaf has no
        // null bit to set and keeps the placeholder, which is a value its column can hold.
        if (optional) {
            stage.appendNull();
        }
        else {
            stage.appendPlaceholder();
        }
    }

    @Override
    void collect(RowPlan.Nodes nodes) {
        nodes.leaves().add(this);
    }

    @Override
    void fill(ColumnBatch batch) {
        stage.fill(batch, columnIndex);
    }

    @Override
    String kind() {
        return physicalType + " leaf field";
    }

    // ==================== Validation helpers ====================

    /// Appends a binary value, checking it against a fixed-width column's declared length and,
    /// under a `DECIMAL` annotation, against the unscaled values that annotation can hold.
    private void appendBinary(byte[] value) {
        requireValueAllowed("setBinary");
        if (physicalType == PhysicalType.FIXED_LEN_BYTE_ARRAY && value.length != typeLength) {
            throw new RejectedRecordException(path, "Field " + path + ": value is " + value.length
                    + " bytes but the column is FIXED_LEN_BYTE_ARRAY(" + typeLength + ")");
        }
        if (range.isBounded() && !range.containsUnscaled(value)) {
            throw new RejectedRecordException(path, "Field " + path
                    + ": the value is not an unscaled value the column's " + logicalType + " can hold");
        }
        ((LeafStage.BinaryStage) stage).append(value);
    }

    /// Rejects a value on a column annotated `UNKNOWN`, which holds only nulls: no value it
    /// could carry matches the annotation, and the reader refuses to materialize one.
    private void requireValueAllowed(String setter) {
        if (range.holdsNoValue()) {
            throw new RejectedRecordException(path, "Field " + path
                    + " is annotated UNKNOWN, which holds only nulls; " + setter
                    + " cannot set a value on it");
        }
    }

    /// Checks an integral value against the range the column's annotation declares. An
    /// unannotated column, and one whose annotation narrows nothing, admits every value.
    private void checkRange(long value) {
        if (range.isBounded() && !range.contains(value)) {
            throw new RejectedRecordException(path, "Field " + path + ": " + value
                    + " is out of range for a " + logicalType + " column");
        }
    }

    /// Stages a `TIME` value, which is stored in an `INT32` at `MILLIS` and in an `INT64`
    /// at the finer units.
    private void appendTimeUnits(long units) {
        if (physicalType == PhysicalType.INT32) {
            ((LeafStage.IntStage) stage).append(Math.toIntExact(units));
        }
        else if (physicalType == PhysicalType.INT64) {
            ((LeafStage.LongStage) stage).append(units);
        }
        else {
            throw wrongType("setTime", "an INT32 or INT64 column");
        }
    }

    /// The annotation of a `TIMESTAMP` column of the kind `setter` writes. `FileSchema` keeps the
    /// annotation on an `INT64` or a `FIXED_LEN_BYTE_ARRAY(12)` only.
    private LogicalType.TimestampType requireTimestamp(String setter, boolean adjustedToUtc) {
        require(logicalType instanceof LogicalType.TimestampType type && type.isAdjustedToUTC() == adjustedToUtc,
                setter, "a column annotated TIMESTAMP with isAdjustedToUTC=" + adjustedToUtc);
        return (LogicalType.TimestampType) logicalType;
    }

    private boolean isStringAnnotated() {
        return logicalType == null
                || logicalType instanceof LogicalType.StringType
                || logicalType instanceof LogicalType.EnumType
                || logicalType instanceof LogicalType.JsonType;
    }

    private void require(boolean condition, String setter, String requirement) {
        if (!condition) {
            throw wrongType(setter, requirement);
        }
    }

    private IllegalArgumentException wrongType(String setter, String requirement) {
        return new IllegalArgumentException("Field " + path + " is " + physicalType
                + (logicalType == null ? "" : " annotated " + logicalType)
                + "; " + setter + " requires " + requirement);
    }

    private String requiredMessage() {
        return "Field " + path + " is REQUIRED; it must be set to a non-null value in every record";
    }
}
