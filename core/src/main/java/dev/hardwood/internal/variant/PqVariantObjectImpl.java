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

import dev.hardwood.internal.variant.VariantValueDecoder.ObjectLayout;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.PqVariantArray;
import dev.hardwood.row.PqVariantObject;
import dev.hardwood.row.VariantType;
import dev.hardwood.row.VariantTypeException;

/// [PqVariantObject] implementation. Caches the parsed [ObjectLayout] so
/// repeated field accesses don't re-walk the header; field-id-to-index lookup
/// uses binary search over the object's id array (ids are guaranteed sorted
/// ascending per the Variant spec).
final class PqVariantObjectImpl implements PqVariantObject {

    private final VariantMetadata metadata;
    private final byte[] valueBuf;
    private final ObjectLayout layout;

    PqVariantObjectImpl(VariantMetadata metadata, byte[] valueBuf, int objectHeaderOffset) {
        this.metadata = metadata;
        this.valueBuf = valueBuf;
        this.layout = VariantValueDecoder.parseObject(valueBuf, objectHeaderOffset);
    }

    /// Locate the child-array index for the given field name, or -1 if absent.
    ///
    /// The object's field_ids array is sorted by the **name** of each field
    /// (unsigned byte order per the Variant spec), but those ids are not
    /// necessarily sorted numerically unless the metadata dictionary itself is
    /// sorted. Rather than conditionally switching strategies, this looks up the
    /// target name's dictionary id once and then scans the object's id array
    /// linearly — simple and correct for both sorted and unsorted metadata.
    private int indexOf(String name) {
        int dictId = metadata.findField(name);
        if (dictId < 0) {
            return -1;
        }
        int n = layout.numElements();
        for (int i = 0; i < n; i++) {
            if (VariantValueDecoder.objectFieldId(valueBuf, layout, i) == dictId) {
                return i;
            }
        }
        return -1;
    }

    private int valueOffsetFor(String name) {
        int i = indexOf(name);
        if (i < 0) {
            throw new IllegalArgumentException("Field not found: " + name);
        }
        return VariantValueDecoder.objectValueOffset(valueBuf, layout, i);
    }

    private PqVariant wrap(int valueOffset) {
        return new PqVariantImpl(metadata, valueBuf, valueOffset);
    }

    // ==================== Metadata ====================

    @Override
    public int getFieldCount() {
        return layout.numElements();
    }

    @Override
    public String getFieldName(int index) {
        int id = VariantValueDecoder.objectFieldId(valueBuf, layout, index);
        return metadata.getField(id);
    }

    @Override
    public boolean isNull(String name) {
        int i = indexOf(name);
        if (i < 0) {
            throw new IllegalArgumentException("Field not found: " + name);
        }
        int off = VariantValueDecoder.objectValueOffset(valueBuf, layout, i);
        return VariantValueDecoder.type(valueBuf, off) == VariantType.NULL;
    }

    // ==================== Primitives ====================

    @Override
    public int getInt(String name) {
        int off = valueOffsetFor(name);
        return VariantValueDecoder.asInt(valueBuf, off);
    }

    @Override
    public long getLong(String name) {
        return VariantValueDecoder.asLong(valueBuf, valueOffsetFor(name));
    }

    @Override
    public float getFloat(String name) {
        return VariantValueDecoder.asFloat(valueBuf, valueOffsetFor(name));
    }

    @Override
    public double getDouble(String name) {
        return VariantValueDecoder.asDouble(valueBuf, valueOffsetFor(name));
    }

    @Override
    public boolean getBoolean(String name) {
        return VariantValueDecoder.asBoolean(valueBuf, valueOffsetFor(name));
    }

    @Override
    public String getString(String name) {
        int off = valueOffsetFor(name);
        if (VariantValueDecoder.type(valueBuf, off) == VariantType.NULL) {
            return null;
        }
        return VariantValueDecoder.asString(valueBuf, off);
    }

    @Override
    public byte[] getBinary(String name) {
        int off = valueOffsetFor(name);
        if (VariantValueDecoder.type(valueBuf, off) == VariantType.NULL) {
            return null;
        }
        return VariantValueDecoder.asBinary(valueBuf, off);
    }

    @Override
    public LocalDate getDate(String name) {
        int off = valueOffsetFor(name);
        if (VariantValueDecoder.type(valueBuf, off) == VariantType.NULL) {
            return null;
        }
        return VariantValueDecoder.asDate(valueBuf, off);
    }

    @Override
    public LocalTime getTime(String name) {
        int off = valueOffsetFor(name);
        if (VariantValueDecoder.type(valueBuf, off) == VariantType.NULL) {
            return null;
        }
        return VariantValueDecoder.asTime(valueBuf, off);
    }

    @Override
    public Instant getTimestamp(String name) {
        int off = valueOffsetFor(name);
        if (VariantValueDecoder.type(valueBuf, off) == VariantType.NULL) {
            return null;
        }
        return VariantValueDecoder.asTimestamp(valueBuf, off);
    }

    @Override
    public BigDecimal getDecimal(String name) {
        int off = valueOffsetFor(name);
        if (VariantValueDecoder.type(valueBuf, off) == VariantType.NULL) {
            return null;
        }
        return VariantValueDecoder.asDecimal(valueBuf, off);
    }

    @Override
    public UUID getUuid(String name) {
        int off = valueOffsetFor(name);
        if (VariantValueDecoder.type(valueBuf, off) == VariantType.NULL) {
            return null;
        }
        return VariantValueDecoder.asUuid(valueBuf, off);
    }

    @Override
    public PqVariant getVariant(String name) {
        int off = valueOffsetFor(name);
        if (VariantValueDecoder.type(valueBuf, off) == VariantType.NULL) {
            return null;
        }
        return wrap(off);
    }

    // ==================== Variant complex types ====================

    @Override
    public PqVariantObject getObject(String name) {
        int off = valueOffsetFor(name);
        VariantType t = VariantValueDecoder.type(valueBuf, off);
        if (t == VariantType.NULL) {
            return null;
        }
        if (t != VariantType.OBJECT) {
            throw VariantTypeException.expected(VariantType.OBJECT, t);
        }
        return new PqVariantObjectImpl(metadata, valueBuf, off);
    }

    @Override
    public PqVariantArray getArray(String name) {
        int off = valueOffsetFor(name);
        VariantType t = VariantValueDecoder.type(valueBuf, off);
        if (t == VariantType.NULL) {
            return null;
        }
        if (t != VariantType.ARRAY) {
            throw VariantTypeException.expected(VariantType.ARRAY, t);
        }
        return new PqVariantArrayImpl(metadata, valueBuf, off);
    }

    @Override
    public Object getValue(String name) {
        int off = valueOffsetFor(name);
        if (VariantValueDecoder.type(valueBuf, off) == VariantType.NULL) {
            return null;
        }
        return wrap(off);
    }
}
