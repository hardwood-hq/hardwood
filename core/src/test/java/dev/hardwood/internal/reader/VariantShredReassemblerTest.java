/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.variant.ShredLevel;
import dev.hardwood.internal.variant.ShredLevel.Typed;
import dev.hardwood.internal.variant.VariantMetadata;
import dev.hardwood.internal.variant.VariantValueEncoder;
import dev.hardwood.metadata.PhysicalType;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// White-box tests for [VariantShredReassembler]'s handling of malformed
/// shredded input that violates the per-level shredding invariants.
class VariantShredReassemblerTest {

    /// Variant metadata dictionary holding a single field `dup` (header 0x01,
    /// dictionary_size 1, offsets [0, 3], strings "dup").
    private static final byte[] METADATA_DUP = { 0x01, 0x01, 0x00, 0x03, 'd', 'u', 'p' };

    /// A shredded OBJECT may carry both a `typed_value` struct and a `value`
    /// blob (partial shredding), but only when their field-name sets are
    /// disjoint. A file where the same field appears on both sides is malformed
    /// and must be rejected rather than silently reassembled into a Variant
    /// object with duplicate field ids.
    @Test
    void shreddedObjectFieldAlsoPresentInUnshreddedValueIsRejected() {
        // Column layout (projected indices):
        //   col 0 = top-level `value`        (BYTE_ARRAY)
        //   col 1 = field "dup" typed_value  (INT64)
        ShredLevel fieldDup = new ShredLevel(-1, 0,
                new Typed.Primitive(1, 1, PhysicalType.INT64, null));
        ShredLevel root = new ShredLevel(0, 1,
                new Typed.Object(1, new String[] { "dup" }, new ShredLevel[] { fieldDup }));

        // The unshredded `value` is an object that ALSO carries field "dup" —
        // colliding with the shredded struct.
        byte[] innerValue = encode(buf -> VariantValueEncoder.writeInt8(buf, 0, 5));
        byte[] unshreddedObject = encode(buf ->
                VariantValueEncoder.writeObject(buf, 0, new int[] { 0 }, new byte[][] { innerValue }, 0));

        NestedBatchIndex batch = singleRowObjectBatch(unshreddedObject, 42L);

        VariantShredReassembler reassembler = new VariantShredReassembler();
        reassembler.setCurrentMetadata(new VariantMetadata(METADATA_DUP));

        assertThatThrownBy(() -> reassembler.reassemble(root, batch, 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("dup");
    }

    // ==================== Helpers ====================

    @FunctionalInterface
    private interface VariantWriter {
        int write(byte[] buf);
    }

    private static byte[] encode(VariantWriter writer) {
        byte[] buf = new byte[64];
        return Arrays.copyOf(buf, writer.write(buf));
    }

    /// Build a single-row, two-column batch matching the column layout above:
    /// col 0 is a top-level `value` BYTE_ARRAY holding `valueObject`; col 1 is
    /// the shredded field's INT64 `typed_value`. Both are non-null at def level
    /// 1. Repetition/record offsets are left null (non-repeated columns).
    private static NestedBatchIndex singleRowObjectBatch(byte[] valueObject, long typedFieldValue) {
        NestedBatch valueCol = new NestedBatch();
        valueCol.values = new BinaryBatchValues(valueObject, new int[] { 0, valueObject.length });
        valueCol.valueCount = 1;
        valueCol.recordCount = 1;
        valueCol.definitionLevels = new int[] { 1 };

        NestedBatch typedCol = new NestedBatch();
        typedCol.values = new long[] { typedFieldValue };
        typedCol.valueCount = 1;
        typedCol.recordCount = 1;
        typedCol.definitionLevels = new int[] { 1 };

        return NestedBatchIndex.buildFromBatches(
                new NestedBatch[] { valueCol, typedCol },
                null, null, null, null);
    }
}
