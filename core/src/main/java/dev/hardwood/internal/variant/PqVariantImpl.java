/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.variant;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.UUID;

import dev.hardwood.row.PqVariant;
import dev.hardwood.row.PqVariantArray;
import dev.hardwood.row.PqVariantObject;
import dev.hardwood.row.VariantType;
import dev.hardwood.row.VariantTypeException;

/// Flyweight [PqVariant] implementation backed by a shared metadata + value pair
/// and an offset identifying where this value's header byte lives in the value
/// buffer. Nested sub-values (object fields, array elements) wrap the same
/// buffers at different offsets — no buffer copying.
public final class PqVariantImpl implements PqVariant {

    private final VariantMetadata metadata;
    private final byte[] valueBuf;
    private final int valueOffset;

    /// Construct a top-level Variant from raw `metadata` and `value` bytes. The
    /// value header starts at `valueBuf[0]`.
    public PqVariantImpl(byte[] metadataBytes, byte[] valueBytes) {
        this(new VariantMetadata(metadataBytes), valueBytes, 0);
    }

    public PqVariantImpl(VariantMetadata metadata, byte[] valueBuf, int valueOffset) {
        this.metadata = metadata;
        this.valueBuf = valueBuf;
        this.valueOffset = valueOffset;
    }

    VariantMetadata metadataView() {
        return metadata;
    }

    byte[] valueBuffer() {
        return valueBuf;
    }

    int valueHeaderOffset() {
        return valueOffset;
    }

    @Override
    public byte[] metadata() {
        byte[] src = metadata.buffer();
        byte[] out = new byte[src.length];
        System.arraycopy(src, 0, out, 0, src.length);
        return out;
    }

    @Override
    public String toString() {
        return switch (type()) {
            case NULL -> "null";
            case BOOLEAN_TRUE -> "true";
            case BOOLEAN_FALSE -> "false";
            case INT8, INT16, INT32 -> Integer.toString(asInt());
            case INT64 -> Long.toString(asLong());
            case FLOAT -> Float.toString(asFloat());
            case DOUBLE -> Double.toString(asDouble());
            case DECIMAL4, DECIMAL8, DECIMAL16 -> asDecimal().toString();
            case DATE -> asDate().toString();
            case TIME_NTZ -> asTime().toString();
            case TIMESTAMP, TIMESTAMP_NTZ, TIMESTAMP_NANOS, TIMESTAMP_NTZ_NANOS -> asTimestamp().toString();
            case BINARY -> Arrays.toString(asBinary());
            case STRING -> '"' + asString() + '"';
            case UUID -> asUuid().toString();
            // Recursing into objects/arrays requires walking PqVariantObject /
            // PqVariantArray contents — out of scope here; print a type marker
            // so debug logs at least say what kind of Variant they have.
            case OBJECT -> "<Variant OBJECT>";
            case ARRAY -> "<Variant ARRAY>";
        };
    }

    @Override
    public byte[] value() {
        // Return a fresh byte[] containing exactly this value's bytes —
        // defensive copy protects the batch's column arrays, and
        // `VariantValueDecoder.valueLength` walks the encoding to get the real
        // extent for sub-values (array elements, object fields).
        int length = VariantValueDecoder.valueLength(valueBuf, valueOffset);
        byte[] out = new byte[length];
        System.arraycopy(valueBuf, valueOffset, out, 0, length);
        return out;
    }

    @Override
    public VariantType type() {
        return VariantValueDecoder.type(valueBuf, valueOffset);
    }

    @Override
    public boolean isNull() {
        return type() == VariantType.NULL;
    }

    @Override
    public boolean asBoolean() {
        return VariantValueDecoder.asBoolean(valueBuf, valueOffset);
    }

    @Override
    public int asInt() {
        return VariantValueDecoder.asInt(valueBuf, valueOffset);
    }

    @Override
    public long asLong() {
        return VariantValueDecoder.asLong(valueBuf, valueOffset);
    }

    @Override
    public float asFloat() {
        return VariantValueDecoder.asFloat(valueBuf, valueOffset);
    }

    @Override
    public double asDouble() {
        return VariantValueDecoder.asDouble(valueBuf, valueOffset);
    }

    @Override
    public String asString() {
        return VariantValueDecoder.asString(valueBuf, valueOffset);
    }

    @Override
    public byte[] asBinary() {
        return VariantValueDecoder.asBinary(valueBuf, valueOffset);
    }

    @Override
    public BigDecimal asDecimal() {
        return VariantValueDecoder.asDecimal(valueBuf, valueOffset);
    }

    @Override
    public LocalDate asDate() {
        return VariantValueDecoder.asDate(valueBuf, valueOffset);
    }

    @Override
    public LocalTime asTime() {
        return VariantValueDecoder.asTime(valueBuf, valueOffset);
    }

    @Override
    public Instant asTimestamp() {
        return VariantValueDecoder.asTimestamp(valueBuf, valueOffset);
    }

    @Override
    public UUID asUuid() {
        return VariantValueDecoder.asUuid(valueBuf, valueOffset);
    }

    @Override
    public PqVariantObject asObject() {
        if (VariantBinary.basicType(valueBuf[valueOffset]) != VariantBinary.BASIC_TYPE_OBJECT) {
            throw VariantTypeException.expected(VariantType.OBJECT, type());
        }
        return new PqVariantObjectImpl(metadata, valueBuf, valueOffset);
    }

    @Override
    public PqVariantArray asArray() {
        if (VariantBinary.basicType(valueBuf[valueOffset]) != VariantBinary.BASIC_TYPE_ARRAY) {
            throw VariantTypeException.expected(VariantType.ARRAY, type());
        }
        return new PqVariantArrayImpl(metadata, valueBuf, valueOffset);
    }
}
