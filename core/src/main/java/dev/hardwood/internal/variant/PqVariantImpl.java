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
        return metadata.buffer();
    }

    @Override
    public byte[] value() {
        // For a top-level Variant (offset == 0), the value buffer is the value
        // itself. For nested views, return a copy starting at the value's header
        // byte so callers see the canonical bytes for that sub-value.
        if (valueOffset == 0) {
            return valueBuf;
        }
        int end = computeEndOffset();
        byte[] out = new byte[end - valueOffset];
        System.arraycopy(valueBuf, valueOffset, out, 0, out.length);
        return out;
    }

    private int computeEndOffset() {
        // Determining the exact byte length of an arbitrary Variant value requires
        // walking the encoding. For sub-values produced by navigation, the
        // containing view tracks the extent; for now, fall back to the buffer end
        // (sufficient for value() on the top-level since valueOffset == 0 there).
        return valueBuf.length;
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
