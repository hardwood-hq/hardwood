/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.variant.ShredLevel;
import dev.hardwood.internal.variant.ShredLevel.Typed;
import dev.hardwood.internal.variant.VariantMetadata;
import dev.hardwood.internal.variant.VariantValueEncoder;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.LogicalType.TimeUnit;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// White-box tests for [VariantShredReassembler]'s handling of malformed
/// shredded input that violates the per-level shredding invariants, and of
/// typed_value payloads a writer may encode in more than one way.
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
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Malformed shredded Variant: field 'dup' appears in both the shredded "
                         + "typed_value and the unshredded value object");
    }

    @Test
    void outOfDictionaryEncodedFieldIdIsAReadFailure() {
        ShredLevel fieldDup = new ShredLevel(-1, 0,
                new Typed.Primitive(1, 1, PhysicalType.INT64, null));
        ShredLevel root = new ShredLevel(0, 1,
                new Typed.Object(1, new String[]{"dup"}, new ShredLevel[]{fieldDup}));

        byte[] innerValue = encode(buf -> VariantValueEncoder.writeInt8(buf, 0, 5));
        byte[] unshreddedObject = encode(buf ->
                VariantValueEncoder.writeObject(buf, 0, new int[]{1}, new byte[][]{innerValue}, 1));
        NestedBatchIndex batch = singleRowObjectBatch(unshreddedObject, 42L);

        VariantShredReassembler reassembler = new VariantShredReassembler();
        reassembler.setCurrentMetadata(new VariantMetadata(METADATA_DUP));

        assertThatThrownBy(() -> reassembler.reassemble(root, batch, 0))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Field id out of range: 1 (size=1)");
    }

    @Test
    void shreddedFieldMissingFromValueMetadataIsAReadFailure() {
        ShredLevel fieldMissing = new ShredLevel(-1, 0,
                new Typed.Primitive(1, 1, PhysicalType.INT64, null));
        ShredLevel root = new ShredLevel(0, 1,
                new Typed.Object(1, new String[]{"missing"}, new ShredLevel[]{fieldMissing}));
        byte[] unshreddedObject = encode(buf ->
                VariantValueEncoder.writeObject(buf, 0, new int[0], new byte[0][], 0));
        NestedBatchIndex batch = singleRowObjectBatch(unshreddedObject, 42L);

        VariantShredReassembler reassembler = new VariantShredReassembler();
        reassembler.setCurrentMetadata(new VariantMetadata(METADATA_DUP));

        assertThatThrownBy(() -> reassembler.reassemble(root, batch, 0))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Shredded Variant field 'missing' not present in metadata dictionary");
    }

    /// A `BYTE_ARRAY` `DECIMAL` typed_value stored as no bytes is zero, as the column's
    /// accessors read it.
    @Test
    void anEmptyByteArrayDecimalTypedValueIsZero() {
        ShredLevel root = new ShredLevel(-1, 0,
                new Typed.Primitive(0, 1, PhysicalType.BYTE_ARRAY, LogicalType.decimal(20, 2)));

        NestedBatch typedCol = new NestedBatch();
        typedCol.values = BinaryBatchValuesFixtures.contiguous(new byte[0], new int[] { 0, 0 });
        typedCol.valueCount = 1;
        typedCol.recordCount = 1;
        typedCol.definitionLevels = new int[] { 1 };
        NestedBatchIndex batch = NestedBatchIndex.buildFromBatches(
                new NestedBatch[] { typedCol }, null, null, null, null);

        VariantShredReassembler reassembler = new VariantShredReassembler();
        reassembler.setCurrentMetadata(new VariantMetadata(METADATA_DUP));

        assertThat(reassembler.reassemble(root, batch, 0))
                .isEqualTo(encode(buf -> VariantValueEncoder.writeDecimal16(buf, 0, BigInteger.ZERO, 2)));
    }

    /// Each carrier the Variant shredding spec defines encodes to the matching Variant value.
    static Stream<Arguments> supportedTypedValueCarriers() {
        byte[] uuid = new byte[16];
        uuid[15] = 1;
        return Stream.of(
                Arguments.of(PhysicalType.INT32, null, new int[] { 7 },
                        encode(buf -> VariantValueEncoder.writeInt32(buf, 0, 7))),
                Arguments.of(PhysicalType.INT32, LogicalType.intType(32, true), new int[] { 7 },
                        encode(buf -> VariantValueEncoder.writeInt32(buf, 0, 7))),
                Arguments.of(PhysicalType.INT32, LogicalType.intType(8, true), new int[] { -7 },
                        encode(buf -> VariantValueEncoder.writeInt8(buf, 0, -7))),
                Arguments.of(PhysicalType.INT32, LogicalType.intType(16, true), new int[] { 300 },
                        encode(buf -> VariantValueEncoder.writeInt16(buf, 0, 300))),
                Arguments.of(PhysicalType.INT32, LogicalType.date(), new int[] { 19_000 },
                        encode(buf -> VariantValueEncoder.writeDate(buf, 0, 19_000))),
                Arguments.of(PhysicalType.INT32, LogicalType.decimal(9, 2), new int[] { 12_345 },
                        encode(buf -> VariantValueEncoder.writeDecimal4(buf, 0, 12_345, 2))),
                Arguments.of(PhysicalType.INT64, null, new long[] { 7L },
                        encode(buf -> VariantValueEncoder.writeInt64(buf, 0, 7L))),
                Arguments.of(PhysicalType.INT64, LogicalType.intType(64, true), new long[] { 7L },
                        encode(buf -> VariantValueEncoder.writeInt64(buf, 0, 7L))),
                Arguments.of(PhysicalType.INT64, LogicalType.decimal(18, 3), new long[] { 12_345L },
                        encode(buf -> VariantValueEncoder.writeDecimal8(buf, 0, 12_345L, 3))),
                Arguments.of(PhysicalType.INT64, LogicalType.time(false, TimeUnit.MICROS), new long[] { 1_000_000L },
                        encode(buf -> VariantValueEncoder.writeTimeMicros(buf, 0, 1_000_000L))),
                Arguments.of(PhysicalType.INT64, LogicalType.timestamp(true, TimeUnit.MICROS), new long[] { 5L },
                        encode(buf -> VariantValueEncoder.writeTimestampMicros(buf, 0, 5L, true))),
                Arguments.of(PhysicalType.INT64, LogicalType.timestamp(false, TimeUnit.NANOS), new long[] { 5L },
                        encode(buf -> VariantValueEncoder.writeTimestampNanos(buf, 0, 5L, false))),
                Arguments.of(PhysicalType.FLOAT, null, new float[] { 1.5f },
                        encode(buf -> VariantValueEncoder.writeFloat(buf, 0, 1.5f))),
                Arguments.of(PhysicalType.DOUBLE, null, new double[] { 1.5 },
                        encode(buf -> VariantValueEncoder.writeDouble(buf, 0, 1.5))),
                Arguments.of(PhysicalType.BOOLEAN, null, new boolean[] { true },
                        encode(buf -> VariantValueEncoder.writeBoolean(buf, 0, true))),
                Arguments.of(PhysicalType.BYTE_ARRAY, null, binary(new byte[] { 1, 2 }),
                        encode(buf -> VariantValueEncoder.writeBinary(buf, 0, new byte[] { 1, 2 }))),
                Arguments.of(PhysicalType.BYTE_ARRAY, LogicalType.string(), binary(new byte[] { 'h', 'i' }),
                        encode(buf -> VariantValueEncoder.writeString(buf, 0, new byte[] { 'h', 'i' }))),
                Arguments.of(PhysicalType.BYTE_ARRAY, LogicalType.decimal(20, 2), binary(new byte[] { 1 }),
                        encode(buf -> VariantValueEncoder.writeDecimal16(buf, 0, BigInteger.ONE, 2))),
                Arguments.of(PhysicalType.FIXED_LEN_BYTE_ARRAY, LogicalType.decimal(9, 2),
                        binary(new byte[] { 0, 0, 0, 1 }),
                        encode(buf -> VariantValueEncoder.writeDecimal4(buf, 0, 1, 2))),
                Arguments.of(PhysicalType.FIXED_LEN_BYTE_ARRAY, LogicalType.decimal(18, 2),
                        binary(new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 }),
                        encode(buf -> VariantValueEncoder.writeDecimal8(buf, 0, 1L, 2))),
                Arguments.of(PhysicalType.FIXED_LEN_BYTE_ARRAY, LogicalType.decimal(20, 2),
                        binary(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 1 }),
                        encode(buf -> VariantValueEncoder.writeDecimal16(buf, 0, BigInteger.ONE, 2))),
                Arguments.of(PhysicalType.FIXED_LEN_BYTE_ARRAY, LogicalType.uuid(), binary(uuid),
                        encode(buf -> VariantValueEncoder.writeUuid(buf, 0, new UUID(0L, 1L)))));
    }

    @ParameterizedTest
    @MethodSource("supportedTypedValueCarriers")
    void typedValueCarrierWithAVariantTypeIsEncoded(PhysicalType physicalType, LogicalType logicalType,
                                                    Object values, byte[] expected) {
        ShredLevel root = new ShredLevel(-1, 0, new Typed.Primitive(0, 1, physicalType, logicalType));
        NestedBatchIndex batch = singleValueBatch(values);

        VariantShredReassembler reassembler = new VariantShredReassembler();
        reassembler.setCurrentMetadata(new VariantMetadata(METADATA_DUP));

        assertThat(reassembler.reassemble(root, batch, 0)).isEqualTo(expected);
    }

    /// A shredded level whose `value` and `typed_value` are both null is Variant NULL, the
    /// one-byte value `0x00`.
    @Test
    void levelWithValueAndTypedValueBothNullIsVariantNull() {
        ShredLevel root = new ShredLevel(0, 1,
                new Typed.Primitive(1, 1, PhysicalType.INT64, null));

        NestedBatch valueCol = new NestedBatch();
        valueCol.values = BinaryBatchValuesFixtures.contiguous(new byte[0], new int[] { 0, 0 });
        valueCol.valueCount = 1;
        valueCol.recordCount = 1;
        valueCol.definitionLevels = new int[] { 0 };
        NestedBatch typedCol = new NestedBatch();
        typedCol.values = new long[] { 0L };
        typedCol.valueCount = 1;
        typedCol.recordCount = 1;
        typedCol.definitionLevels = new int[] { 0 };
        NestedBatchIndex batch = NestedBatchIndex.buildFromBatches(
                new NestedBatch[] { valueCol, typedCol }, null, null, null, null);

        VariantShredReassembler reassembler = new VariantShredReassembler();
        reassembler.setCurrentMetadata(new VariantMetadata(METADATA_DUP));

        assertThat(reassembler.reassemble(root, batch, 0)).containsExactly(0x00);
    }

    // ==================== Helpers ====================

    private static BinaryBatchValues binary(byte[] value) {
        return BinaryBatchValuesFixtures.contiguous(value, new int[] { 0, value.length });
    }

    /// Build a single-row batch of one non-null, non-repeated column holding `values`.
    private static NestedBatchIndex singleValueBatch(Object values) {
        NestedBatch col = new NestedBatch();
        col.values = values;
        col.valueCount = 1;
        col.recordCount = 1;
        col.definitionLevels = new int[] { 1 };
        return NestedBatchIndex.buildFromBatches(new NestedBatch[] { col }, null, null, null, null);
    }

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
        valueCol.values = BinaryBatchValuesFixtures.contiguous(valueObject, new int[] { 0, valueObject.length });
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
