/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import dev.hardwood.internal.variant.ShredLevel;
import dev.hardwood.internal.variant.ShredLevel.Typed;
import dev.hardwood.internal.variant.VariantMetadata;
import dev.hardwood.internal.variant.VariantValueDecoder;
import dev.hardwood.internal.variant.VariantValueDecoder.ObjectLayout;
import dev.hardwood.internal.variant.VariantValueEncoder;
import dev.hardwood.metadata.LogicalType;

/// Reassembles the canonical Variant `value` bytes for a row whose Variant
/// column was shredded. Walks the [ShredLevel] tree built at schema-construction
/// time, reading `value` + `typed_value` columns per level and emitting Variant
/// binary via [VariantValueEncoder].
///
/// Follows the spec's per-level decision tree (see `VariantShredding.md`):
/// value null + typed_value null → SQL NULL, pass-through when value is the
/// only side populated, typed extraction when typed_value is the only side,
/// object merge when both are set (object-only per spec).
///
/// One instance per [dev.hardwood.reader.RowReader]; the internal scratch
/// buffer is reused across rows.
public final class VariantShredReassembler {

    private byte[] scratch = new byte[256];
    private int pos;

    /// Assemble the `value` bytes for the current row's top-level Variant. The
    /// caller already established that the variant group itself is non-null
    /// (metadata def level passes). Returns `null` when the reassembled result
    /// is a missing variant (both `value` and `typed_value` absent at the top
    /// level — spec calls this a `None`-valued variant, surfaced as SQL NULL).
    public byte[] reassemble(ShredLevel root, NestedBatchIndex batch, int rowIndex) {
        pos = 0;
        int produced = writeLevel(root, batch, rowIndex);
        if (produced < 0) {
            return null;
        }
        byte[] out = new byte[pos];
        System.arraycopy(scratch, 0, out, 0, pos);
        return out;
    }

    // ==================== Level dispatch ====================

    /// Write the variant bytes for one shredded level. Returns the number of
    /// bytes appended, or `-1` if the level represents a missing variant (both
    /// `value` and `typed_value` absent).
    private int writeLevel(ShredLevel level, NestedBatchIndex batch, int rowIndex) {
        int before = pos;
        int valueCol = level.valueCol();
        int valueIdx = valueCol >= 0 ? batch.getValueIndex(valueCol, rowIndex) : -1;
        boolean valuePresent = valueCol >= 0 && batch.getDefLevel(valueCol, valueIdx) >= level.valueDefLevel();

        if (level.typed() == null) {
            // Purely untyped level: only `value` carries data.
            return valuePresent
                    ? writeRawValue(batch, valueCol, valueIdx)
                    : -1;
        }

        return switch (level.typed()) {
            case Typed.Primitive p -> writePrimitiveLevel(level, p, batch, rowIndex, valueCol, valueIdx, valuePresent);
            case Typed.Array a -> writeArrayLevel(level, a, batch, rowIndex, valueCol, valueIdx, valuePresent);
            case Typed.Object o -> writeObjectLevel(level, o, batch, rowIndex, valueCol, valueIdx, valuePresent);
        };
    }

    // ==================== Primitive shredding ====================

    private int writePrimitiveLevel(ShredLevel level, Typed.Primitive typed,
                                    NestedBatchIndex batch, int rowIndex,
                                    int valueCol, int valueIdx, boolean valuePresent) {
        int before = pos;
        int typedIdx = typed.col() >= 0 ? batch.getValueIndex(typed.col(), rowIndex) : -1;
        boolean typedPresent = typed.col() >= 0
                && batch.getDefLevel(typed.col(), typedIdx) >= typed.defLevel()
                && !batch.isElementNull(typed.col(), typedIdx);

        if (typedPresent) {
            encodePrimitive(typed, batch, typedIdx);
            return pos - before;
        }
        if (valuePresent) {
            writeRawValue(batch, valueCol, valueIdx);
            return pos - before;
        }
        return -1;
    }

    private void encodePrimitive(Typed.Primitive typed, NestedBatchIndex batch, int idx) {
        ensureCapacity(32);
        Object arr = batch.valueArrays[typed.col()];
        LogicalType logical = typed.logicalType();
        switch (typed.physicalType()) {
            case INT32 -> encodeInt32((int[]) arr, idx, logical);
            case INT64 -> encodeInt64((long[]) arr, idx, logical);
            case FLOAT -> pos = VariantValueEncoder.writeFloat(scratch, pos, ((float[]) arr)[idx]);
            case DOUBLE -> pos = VariantValueEncoder.writeDouble(scratch, pos, ((double[]) arr)[idx]);
            case BOOLEAN -> pos = VariantValueEncoder.writeBoolean(scratch, pos, ((boolean[]) arr)[idx]);
            case BYTE_ARRAY -> encodeBinary((BinaryBatchValues) arr, idx, logical);
            case FIXED_LEN_BYTE_ARRAY -> encodeFixedLen((BinaryBatchValues) arr, idx, logical);
            case INT96 -> encodeInt96(idx);
        }
    }

    private void encodeInt32(int[] values, int idx, LogicalType logical) {
        int v = values[idx];
        if (logical instanceof LogicalType.DecimalType d) {
            pos = VariantValueEncoder.writeDecimal4(scratch, pos, v, d.scale());
            return;
        }
        if (logical instanceof LogicalType.DateType) {
            pos = VariantValueEncoder.writeDate(scratch, pos, v);
            return;
        }
        if (logical instanceof LogicalType.IntType i) {
            switch (i.bitWidth()) {
                case 8 -> { pos = VariantValueEncoder.writeInt8(scratch, pos, v); return; }
                case 16 -> { pos = VariantValueEncoder.writeInt16(scratch, pos, v); return; }
                default -> { /* fall through to INT32 */ }
            }
        }
        pos = VariantValueEncoder.writeInt32(scratch, pos, v);
    }

    private void encodeInt64(long[] values, int idx, LogicalType logical) {
        long v = values[idx];
        if (logical instanceof LogicalType.DecimalType d) {
            pos = VariantValueEncoder.writeDecimal8(scratch, pos, v, d.scale());
            return;
        }
        if (logical instanceof LogicalType.TimestampType ts) {
            boolean adjusted = ts.isAdjustedToUTC();
            switch (ts.unit()) {
                case MILLIS -> pos = VariantValueEncoder.writeTimestampMicros(scratch, pos, Math.multiplyExact(v, 1_000L), adjusted);
                case MICROS -> pos = VariantValueEncoder.writeTimestampMicros(scratch, pos, v, adjusted);
                case NANOS -> pos = VariantValueEncoder.writeTimestampNanos(scratch, pos, v, adjusted);
            }
            return;
        }
        if (logical instanceof LogicalType.TimeType) {
            pos = VariantValueEncoder.writeTimeMicros(scratch, pos, v);
            return;
        }
        pos = VariantValueEncoder.writeInt64(scratch, pos, v);
    }

    private void encodeBinary(BinaryBatchValues values, int idx, LogicalType logical) {
        byte[] raw = values.byteArrayAt(idx);
        ensureCapacity(raw.length + 8);
        if (logical instanceof LogicalType.StringType
                || logical instanceof LogicalType.JsonType
                || logical instanceof LogicalType.EnumType) {
            pos = VariantValueEncoder.writeString(scratch, pos, raw);
            return;
        }
        if (logical instanceof LogicalType.DecimalType d) {
            pos = VariantValueEncoder.writeDecimal16(scratch, pos, new BigInteger(raw), d.scale());
            return;
        }
        pos = VariantValueEncoder.writeBinary(scratch, pos, raw);
    }

    private void encodeFixedLen(BinaryBatchValues values, int idx, LogicalType logical) {
        byte[] raw = values.byteArrayAt(idx);
        ensureCapacity(raw.length + 8);
        if (logical instanceof LogicalType.UuidType) {
            long msb = bytesToLongBE(raw, 0);
            long lsb = bytesToLongBE(raw, 8);
            pos = VariantValueEncoder.writeUuid(scratch, pos, new UUID(msb, lsb));
            return;
        }
        if (logical instanceof LogicalType.DecimalType d) {
            BigInteger unscaled = new BigInteger(raw);
            int width = pickDecimalWidth(d.precision());
            switch (width) {
                case 4 -> pos = VariantValueEncoder.writeDecimal4(scratch, pos, unscaled.intValue(), d.scale());
                case 8 -> pos = VariantValueEncoder.writeDecimal8(scratch, pos, unscaled.longValue(), d.scale());
                default -> pos = VariantValueEncoder.writeDecimal16(scratch, pos, unscaled, d.scale());
            }
            return;
        }
        pos = VariantValueEncoder.writeBinary(scratch, pos, raw);
    }

    private void encodeInt96(int idx) {
        // INT96 is a deprecated legacy timestamp encoding with no direct
        // Variant equivalent. Writers that want a shredded timestamp should use
        // INT64 + TIMESTAMP(MICROS/NANOS); silently mistyping an INT96 as
        // BINARY would corrupt the Variant's type tag.
        throw new UnsupportedOperationException(
                "Shredded Variant typed_value with INT96 physical type is not supported; "
                        + "convert to INT64 TIMESTAMP before writing");
    }

    private static int pickDecimalWidth(int precision) {
        if (precision <= 9) {
            return 4;
        }
        if (precision <= 18) {
            return 8;
        }
        return 16;
    }

    // ==================== Object shredding ====================

    private int writeObjectLevel(ShredLevel level, Typed.Object typed,
                                 NestedBatchIndex batch, int rowIndex,
                                 int valueCol, int valueIdx, boolean valuePresent) {
        int before = pos;
        boolean structPresent = isTypedValuePresent(typed.fields(), batch, rowIndex, typed.structDefLevel());

        if (!structPresent) {
            // Spec: if typed_value is null, fall through to `value` — which may
            // hold a non-object payload (writer chose not to shred this row).
            return valuePresent ? writeRawValue(batch, valueCol, valueIdx) : -1;
        }

        // Materialize each shredded field's bytes. Primitive-array-backed work
        // storage avoids the Integer[] boxing a Comparator-driven sort would
        // introduce in emitObject; allocation is per-call so recursion into
        // nested objects doesn't clobber our slots.
        int fieldCount = typed.fieldNames().length;
        int extraFromMerge = valuePresent
                ? VariantValueDecoder.parseObject(rawBytes(batch, valueCol, valueIdx), 0).numElements()
                : 0;
        String[] names = new String[fieldCount + extraFromMerge];
        byte[][] values = new byte[fieldCount + extraFromMerge][];
        int count = 0;
        for (int i = 0; i < fieldCount; i++) {
            int snap = pos;
            int produced = writeLevel(typed.fields()[i], batch, rowIndex);
            if (produced < 0) {
                pos = snap;
                continue;
            }
            byte[] fieldBytes = new byte[produced];
            System.arraycopy(scratch, snap, fieldBytes, 0, produced);
            pos = snap; // object-framing writer re-emits into scratch below
            names[count] = typed.fieldNames()[i];
            values[count] = fieldBytes;
            count++;
        }

        if (valuePresent) {
            byte[] raw = rawBytes(batch, valueCol, valueIdx);
            count = mergeUnshreddedObject(raw, names, values, count);
        }

        emitObject(names, values, count);
        return pos - before;
    }

    /// Decode the partial-unshredded object bytes in `raw`, appending each
    /// (name, valueBytes) pair into `names` / `values` starting at `count`.
    /// Spec guarantees these names are disjoint from the shredded set, so we
    /// can simply append — no deduplication needed. Field ids index into the
    /// top-level metadata dictionary (same for all nesting depths within a
    /// variant), resolved via `currentMetadata`. Returns the new count.
    private int mergeUnshreddedObject(byte[] raw, String[] names, byte[][] values, int count) {
        ObjectLayout layout = VariantValueDecoder.parseObject(raw, 0);
        int n = layout.numElements();
        for (int i = 0; i < n; i++) {
            int fieldId = VariantValueDecoder.objectFieldId(raw, layout, i);
            String fieldName = currentMetadata.getField(fieldId);
            int valueStart = VariantValueDecoder.objectValueOffset(raw, layout, i);
            // The object's offset array has `numElements + 1` entries, so
            // offset[i+1] is always valid and points at the byte past element i.
            int valueEnd = VariantValueDecoder.objectValueOffset(raw, layout, i + 1);
            byte[] fieldBytes = new byte[valueEnd - valueStart];
            System.arraycopy(raw, valueStart, fieldBytes, 0, fieldBytes.length);
            names[count] = fieldName;
            values[count] = fieldBytes;
            count++;
        }
        return count;
    }

    /// Emit the object framing with fields sorted alphabetically (spec
    /// requirement) and field-ids resolved from the top-level metadata, which
    /// the caller sets via [#setCurrentMetadata] before invoking.
    ///
    /// Uses a primitive `int[]` index permutation + insertion sort to avoid
    /// the `Integer[]` boxing that a `Comparator<Integer>` sort would
    /// introduce. Insertion sort is chosen because Variant objects typically
    /// have small `n`.
    private void emitObject(String[] names, byte[][] values, int n) {
        int[] order = new int[n];
        for (int i = 0; i < n; i++) {
            order[i] = i;
        }
        insertionSortByName(order, names, n);

        int[] fieldIds = new int[n];
        int maxId = -1;
        int totalBytes = 0;
        byte[][] sortedValues = new byte[n][];
        for (int i = 0; i < n; i++) {
            int src = order[i];
            String name = names[src];
            int id = currentMetadata.findField(name);
            if (id < 0) {
                throw new IllegalStateException(
                        "Shredded Variant field '" + name + "' not present in metadata dictionary");
            }
            fieldIds[i] = id;
            if (id > maxId) {
                maxId = id;
            }
            byte[] v = values[src];
            sortedValues[i] = v;
            totalBytes += v.length;
        }

        // Estimate capacity: 1 header + up to 4 num + n*idSize + (n+1)*offSize + payload.
        ensureCapacity(16 + n * 8 + totalBytes);
        pos = VariantValueEncoder.writeObject(scratch, pos, fieldIds, sortedValues, maxId);
    }

    /// In-place insertion sort over `order[0..n)`, stable, using unsigned-lex
    /// ordering of `names[order[i]]`. Small `n` (Variant objects rarely have
    /// many fields) — O(n²) is fine.
    private static void insertionSortByName(int[] order, String[] names, int n) {
        for (int i = 1; i < n; i++) {
            int cur = order[i];
            byte[] curBytes = names[cur].getBytes(StandardCharsets.UTF_8);
            int j = i - 1;
            while (j >= 0) {
                byte[] prev = names[order[j]].getBytes(StandardCharsets.UTF_8);
                if (Arrays.compareUnsigned(prev, curBytes) <= 0) {
                    break;
                }
                order[j + 1] = order[j];
                j--;
            }
            order[j + 1] = cur;
        }
    }

    // ==================== Array shredding ====================

    private int writeArrayLevel(ShredLevel level, Typed.Array typed,
                                NestedBatchIndex batch, int rowIndex,
                                int valueCol, int valueIdx, boolean valuePresent) {
        int before = pos;
        int leafCol = firstLeafCol(typed.element());
        if (leafCol < 0) {
            return valuePresent ? writeRawValue(batch, valueCol, valueIdx) : -1;
        }

        // Distinguish typed_value-null (list group absent) from empty list by
        // probing the leaf's def level at the record's first-leaf value index:
        // present-but-empty lists still have a synthetic entry with def level
        // equal to the list group's max def level; null lists have lower.
        int probeIdx = batch.getValueIndex(leafCol, rowIndex);
        boolean listPresent = batch.getDefLevel(leafCol, probeIdx) >= typed.listDefLevel();

        if (!listPresent) {
            if (valuePresent) {
                return writeRawValue(batch, valueCol, valueIdx);
            }
            return -1;
        }

        int listStart = batch.getListStart(leafCol, rowIndex);
        int listEnd = batch.getListEnd(leafCol, rowIndex);

        List<byte[]> elements = new ArrayList<>(listEnd - listStart);
        for (int i = listStart; i < listEnd; i++) {
            // Synthetic placeholder rows carry a def level below the element's
            // max def level — they mark present-but-empty lists rather than a
            // real element. Skip them.
            if (batch.getDefLevel(leafCol, i) < typed.elementDefLevel()) {
                continue;
            }
            int snap = pos;
            int produced = writeElementAt(typed.element(), batch, i);
            if (produced < 0) {
                // Missing elements inside an array MUST be encoded as Variant
                // null (basic type 0, tag 0) per the spec; translate here.
                pos = snap;
                ensureCapacity(1);
                pos = VariantValueEncoder.writeNull(scratch, pos);
                produced = pos - snap;
            }
            byte[] bytes = new byte[produced];
            System.arraycopy(scratch, snap, bytes, 0, produced);
            pos = snap;
            elements.add(bytes);
        }
        int totalBytes = 0;
        for (byte[] e : elements) {
            totalBytes += e.length;
        }
        ensureCapacity(16 + totalBytes);
        pos = VariantValueEncoder.writeArray(scratch, pos, elements.toArray(new byte[0][]));
        return pos - before;
    }

    /// Determine whether a typed_value container (object struct or array's
    /// wrapper LIST) is non-null at the current row by probing a leaf column
    /// inside the subtree and comparing its recorded def level to the
    /// container's max def level. Required because the outer variant `value`
    /// column's def level reflects `value`'s nullness, not `typed_value`'s.
    private static boolean isTypedValuePresent(ShredLevel[] children,
                                               NestedBatchIndex batch, int rowIndex,
                                               int containerDefLevel) {
        for (ShredLevel child : children) {
            int leaf = firstLeafCol(child);
            if (leaf < 0) {
                continue;
            }
            int idx = batch.getValueIndex(leaf, rowIndex);
            if (batch.getDefLevel(leaf, idx) >= containerDefLevel) {
                return true;
            }
        }
        return false;
    }

    private static int firstLeafCol(ShredLevel level) {
        if (level.valueCol() >= 0) {
            return level.valueCol();
        }
        if (level.typed() instanceof Typed.Primitive p && p.col() >= 0) {
            return p.col();
        }
        if (level.typed() instanceof Typed.Array a) {
            return firstLeafCol(a.element());
        }
        if (level.typed() instanceof Typed.Object o) {
            for (ShredLevel f : o.fields()) {
                int c = firstLeafCol(f);
                if (c >= 0) {
                    return c;
                }
            }
        }
        return -1;
    }

    /// Write one array element. Elements inside a shredded list use the
    /// value-index derived from the list's multi-level offsets — supplied
    /// directly by the caller as `elementIdx`, not via a row-index lookup.
    private int writeElementAt(ShredLevel element, NestedBatchIndex batch, int elementIdx) {
        int before = pos;
        int valueCol = element.valueCol();
        boolean valuePresent = valueCol >= 0 && batch.getDefLevel(valueCol, elementIdx) >= element.valueDefLevel();

        if (element.typed() == null) {
            if (valuePresent) {
                writeRawValue(batch, valueCol, elementIdx);
                return pos - before;
            }
            return -1;
        }
        return switch (element.typed()) {
            case Typed.Primitive p -> writeElementPrimitive(element, p, batch, elementIdx, valueCol, valuePresent);
            case Typed.Array a ->
                // Nested list inside list — resolve inner list boundaries via
                // the multi-level offsets of a leaf inside the inner element.
                    writeElementNestedArray(element, a, batch, elementIdx, valueCol, valuePresent);
            case Typed.Object o -> writeElementObject(element, o, batch, elementIdx, valueCol, valuePresent);
        };
    }

    private int writeElementPrimitive(ShredLevel level, Typed.Primitive typed,
                                      NestedBatchIndex batch, int elementIdx,
                                      int valueCol, boolean valuePresent) {
        int before = pos;
        int typedCol = typed.col();
        if (typedCol >= 0
                && batch.getDefLevel(typedCol, elementIdx) >= typed.defLevel()
                && !batch.isElementNull(typedCol, elementIdx)) {
            encodePrimitive(typed, batch, elementIdx);
            return pos - before;
        }
        if (valuePresent) {
            writeRawValue(batch, valueCol, elementIdx);
            return pos - before;
        }
        return -1;
    }

    private int writeElementObject(ShredLevel level, Typed.Object typed,
                                   NestedBatchIndex batch, int elementIdx,
                                   int valueCol, boolean valuePresent) {
        // Mirrors writeObjectLevel but with elementIdx instead of rowIndex for
        // the children's value index.
        int before = pos;
        int fieldCount = typed.fieldNames().length;
        int extraFromMerge = valuePresent
                ? VariantValueDecoder.parseObject(rawBytes(batch, valueCol, elementIdx), 0).numElements()
                : 0;
        String[] names = new String[fieldCount + extraFromMerge];
        byte[][] values = new byte[fieldCount + extraFromMerge][];
        int count = 0;
        for (int i = 0; i < fieldCount; i++) {
            int snap = pos;
            int produced = writeElementAt(typed.fields()[i], batch, elementIdx);
            if (produced < 0) {
                pos = snap;
                continue;
            }
            byte[] fieldBytes = new byte[produced];
            System.arraycopy(scratch, snap, fieldBytes, 0, produced);
            pos = snap;
            names[count] = typed.fieldNames()[i];
            values[count] = fieldBytes;
            count++;
        }
        if (count == 0 && !valuePresent) {
            return -1;
        }
        if (valuePresent) {
            byte[] raw = rawBytes(batch, valueCol, elementIdx);
            count = mergeUnshreddedObject(raw, names, values, count);
        }
        emitObject(names, values, count);
        return pos - before;
    }

    private int writeElementNestedArray(ShredLevel level, Typed.Array typed,
                                        NestedBatchIndex batch, int elementIdx,
                                        int valueCol, boolean valuePresent) {
        // Navigate the inner list via the nested element's leaf column. The
        // outer list has already placed us at `elementIdx` in level-0; for the
        // inner list, we walk level-1 of the leaf's multi-level offsets.
        int before = pos;
        int innerLeaf = firstLeafCol(typed.element());
        if (innerLeaf < 0) {
            return valuePresent ? writeRawValue(batch, valueCol, elementIdx) : -1;
        }
        int innerStart = batch.getLevelStart(innerLeaf, 1, elementIdx);
        int innerEnd = batch.getLevelEnd(innerLeaf, 1, elementIdx);
        // Present-but-empty lists still carry a synthetic entry at `innerStart`
        // whose def level reports the list's max def level; truly-null lists
        // carry a lower def level at that position. `innerStart == innerEnd`
        // just means there are no real elements — the synthetic entry (if any)
        // sits at `innerStart`, which is the valid probe position either way.
        boolean listPresent = innerStart < batch.valueCounts[innerLeaf]
                && batch.getDefLevel(innerLeaf, innerStart) >= typed.listDefLevel();
        if (!listPresent) {
            return valuePresent ? writeRawValue(batch, valueCol, elementIdx) : -1;
        }
        List<byte[]> inner = new ArrayList<>(innerEnd - innerStart);
        for (int i = innerStart; i < innerEnd; i++) {
            if (batch.getDefLevel(innerLeaf, i) < typed.elementDefLevel()) {
                continue;
            }
            int snap = pos;
            int produced = writeElementAt(typed.element(), batch, i);
            if (produced < 0) {
                pos = snap;
                ensureCapacity(1);
                pos = VariantValueEncoder.writeNull(scratch, pos);
                produced = pos - snap;
            }
            byte[] bytes = new byte[produced];
            System.arraycopy(scratch, snap, bytes, 0, produced);
            pos = snap;
            inner.add(bytes);
        }
        int totalBytes = 0;
        for (byte[] e : inner) {
            totalBytes += e.length;
        }
        ensureCapacity(16 + totalBytes);
        pos = VariantValueEncoder.writeArray(scratch, pos, inner.toArray(new byte[0][]));
        return pos - before;
    }

    // ==================== Raw value helpers ====================

    private int writeRawValue(NestedBatchIndex batch, int valueCol, int valueIdx) {
        byte[] raw = rawBytes(batch, valueCol, valueIdx);
        ensureCapacity(raw.length);
        pos = VariantValueEncoder.writeRaw(scratch, pos, raw, 0, raw.length);
        return raw.length;
    }


    private static byte[] rawBytes(NestedBatchIndex batch, int valueCol, int valueIdx) {
        return batch.getBinary(valueCol, valueIdx);
    }

    private static long bytesToLongBE(byte[] buf, int offset) {
        long result = 0L;
        for (int i = 0; i < 8; i++) {
            result = (result << 8) | (buf[offset + i] & 0xFFL);
        }
        return result;
    }

    // ==================== Metadata context ====================

    /// Set before each top-level [#reassemble] call so object-shredded fields
    /// can resolve their names to dictionary ids.
    private VariantMetadata currentMetadata;

    public void setCurrentMetadata(VariantMetadata metadata) {
        this.currentMetadata = metadata;
    }

    // ==================== Capacity ====================

    private void ensureCapacity(int extra) {
        int needed = pos + extra;
        if (needed <= scratch.length) {
            return;
        }
        int newCap = scratch.length;
        while (newCap < needed) {
            newCap *= 2;
        }
        byte[] grown = new byte[newCap];
        System.arraycopy(scratch, 0, grown, 0, pos);
        scratch = grown;
    }

}
