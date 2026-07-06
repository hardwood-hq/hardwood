/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.variant;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import dev.hardwood.row.PqVariant;
import dev.hardwood.row.PqVariantArray;
import dev.hardwood.row.PqVariantObject;
import dev.hardwood.row.VariantType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Pins the fail-early error paths on the Variant read surface. Each accessor
/// that rejects malformed input or out-of-contract calls has at least one
/// assertion here so silent-regression to wrong-but-plausible output is
/// caught immediately.
class PqVariantInvalidInputTest {

    /// Minimal valid metadata (version 1, empty dictionary) for pairing with
    /// hand-built malformed value buffers.
    private static final byte[] EMPTY_METADATA = { 0x01, 0x00, 0x00 };

    /// Metadata with a single-entry dictionary `["a"]` (version 1, offset_size 1),
    /// for pairing with hand-built nested-object value buffers keyed on `"a"`.
    private static final byte[] SINGLE_FIELD_METADATA = { 0x01, 0x01, 0x00, 0x01, 0x61 };

    /// A leaf INT8 value (`42`) to sit at the bottom of a nesting chain.
    private static final byte[] LEAF_INT8 = { 0x0C, 42 };

    @Test
    void objectGetOnMissingFieldThrows() throws IOException {
        PqVariantObject obj = load("object_primitive").asObject();
        assertThatThrownBy(() -> obj.getInt("no_such_field"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Field not found")
                .hasMessageContaining("no_such_field");
    }

    @Test
    void objectIsNullOnMissingFieldThrows() throws IOException {
        PqVariantObject obj = load("object_primitive").asObject();
        assertThatThrownBy(() -> obj.isNull("no_such_field"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Field not found");
    }

    @Test
    void arrayGetNegativeIndexThrows() throws IOException {
        PqVariantArray arr = load("array_primitive").asArray();
        assertThatThrownBy(() -> arr.get(-1))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessageContaining("-1");
    }

    @Test
    void arrayGetPastEndThrows() throws IOException {
        PqVariantArray arr = load("array_primitive").asArray();
        int size = arr.size();
        assertThatThrownBy(() -> arr.get(size))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessageContaining(String.valueOf(size));
    }

    @Test
    void metadataGetFieldOutOfRangeThrows() throws IOException {
        VariantMetadata metadata = new VariantMetadata(
                readResource("/variant/object_primitive.metadata"));
        assertThatThrownBy(() -> metadata.getField(metadata.size()))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessageContaining("out of range");
        assertThatThrownBy(() -> metadata.getField(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void metadataUnsupportedVersionRejected() {
        // Header with version bits = 2 in the low 4 bits. Spec defines only
        // version 1; future versions should fail-fast rather than risk
        // misinterpreting the layout.
        byte[] bytes = { 0x02 };
        assertThatThrownBy(() -> new VariantMetadata(bytes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported Variant metadata version");
    }

    @Test
    void oversizedPrimitiveLengthRejected() {
        // PRIM_BINARY (0x3C), 4-byte length 0x7FFFFFFF — runs far past the buffer.
        byte[] value = { 0x3C, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x7F };
        assertThatThrownBy(() -> new PqVariantImpl(EMPTY_METADATA, value).value())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("string/binary length");
    }

    @Test
    void shortStringLengthExceedsBufferRejected() {
        // SHORT_STRING (0x29) declaring length 10 with no payload bytes present.
        byte[] value = { 0x29 };
        assertThatThrownBy(() -> new PqVariantImpl(EMPTY_METADATA, value).value())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("short string length");
    }

    @Test
    void truncatedPrimitiveSizeRejected() {
        // PRIM_STRING (0x40) whose 4-byte length prefix is truncated to 2 bytes.
        byte[] value = { 0x40, 0x05, 0x00 };
        assertThatThrownBy(() -> new PqVariantImpl(EMPTY_METADATA, value).value())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("truncated");
    }

    @Test
    void binaryAccessorOversizedLengthRejected() {
        // asBinary: 0x80000000 reads back negative and slipped past the old check.
        byte[] value = { 0x3C, 0x00, 0x00, 0x00, (byte) 0x80 };
        assertThatThrownBy(() -> new PqVariantImpl(EMPTY_METADATA, value).asBinary())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("string/binary length");
    }

    @Test
    void validPrimitiveStringDecodes() {
        // The guard rejects only out-of-range lengths — a well-formed string reads.
        byte[] value = { 0x40, 0x03, 0x00, 0x00, 0x00, 'a', 'b', 'c' };
        PqVariant v = new PqVariantImpl(EMPTY_METADATA, value);
        assertThat(v.asString()).isEqualTo("abc");
        assertThat(v.value()).isEqualTo(value);
    }

    @Test
    void stringAccessorOversizedLengthRejected() {
        // asString: PRIM_STRING (0x40) with a 0x7FFFFFFF length prefix.
        byte[] value = { 0x40, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x7F };
        assertThatThrownBy(() -> new PqVariantImpl(EMPTY_METADATA, value).asString())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("string/binary length");
    }

    @Test
    void objectValueExtentRejected() {
        // Empty OBJECT (0x0E) whose sole offset-table entry claims a 0x7FFFFFFF-byte
        // values section — value() would size a copy far past the buffer.
        byte[] value = { 0x0E, 0x00, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x7F };
        assertThatThrownBy(() -> new PqVariantImpl(EMPTY_METADATA, value).value())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("object/array value");
    }

    @Test
    void arrayValueExtentRejected() {
        // Empty ARRAY (0x0F) whose sole offset-table entry claims a 0x7FFFFFFF-byte
        // values section — exercises the array branch of the extent guard.
        byte[] value = { 0x0F, 0x00, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x7F };
        assertThatThrownBy(() -> new PqVariantImpl(EMPTY_METADATA, value).value())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("object/array value");
    }

    @Test
    void fixedWidthPrimitiveTruncationRejected() {
        // PRIM_INT64 (0x18) header claiming an 8-byte payload with only 2 bytes present.
        byte[] value = { 0x18, 0x01, 0x02 };
        assertThatThrownBy(() -> new PqVariantImpl(EMPTY_METADATA, value).value())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("primitive value");
    }

    @Test
    void deeplyNestedArrayRejected() {
        // Arrays nested far past the limit must fail fast when navigated, rather
        // than being descended into unbounded recursion.
        PqVariant root = new PqVariantImpl(EMPTY_METADATA, nestInArrays(VariantValueDecoder.MAX_NESTING_DEPTH + 100));
        assertThatThrownBy(() -> descendArrayToLeaf(root))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nesting")
                .hasMessageContaining(String.valueOf(VariantValueDecoder.MAX_NESTING_DEPTH));
    }

    @Test
    void arrayNestedAtLimitNavigatesFully() {
        // Exactly at the limit: the whole chain navigates and the leaf reads back.
        PqVariant root = new PqVariantImpl(EMPTY_METADATA, nestInArrays(VariantValueDecoder.MAX_NESTING_DEPTH));
        assertThat(descendArrayToLeaf(root).asInt()).isEqualTo(42);
    }

    @Test
    void arrayNestedOnePastLimitRejected() {
        // One level beyond the limit is the boundary that must be rejected.
        PqVariant root = new PqVariantImpl(EMPTY_METADATA, nestInArrays(VariantValueDecoder.MAX_NESTING_DEPTH + 1));
        assertThatThrownBy(() -> descendArrayToLeaf(root))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nesting");
    }

    @Test
    void deeplyNestedObjectRejected() {
        // The object descent path (getObject) is guarded the same way as arrays.
        int levels = VariantValueDecoder.MAX_NESTING_DEPTH + 100;
        PqVariant root = new PqVariantImpl(SINGLE_FIELD_METADATA, nestInObjects(levels));
        assertThatThrownBy(() -> descendObject(root.asObject(), levels))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nesting")
                .hasMessageContaining(String.valueOf(VariantValueDecoder.MAX_NESTING_DEPTH));
    }

    @Test
    void objectNestedAtLimitNavigatesFully() {
        // Object path navigates to exactly the limit: getObject to the innermost
        // level, then getVariant reads the leaf at the 500th (allowed) descent.
        PqVariantObject object = new PqVariantImpl(SINGLE_FIELD_METADATA,
                nestInObjects(VariantValueDecoder.MAX_NESTING_DEPTH)).asObject();
        for (int i = 1; i < VariantValueDecoder.MAX_NESTING_DEPTH; i++) {
            object = object.getObject("a");
        }
        assertThat(object.getVariant("a").asInt()).isEqualTo(42);
    }

    @Test
    void arrayIterationNestingRejected() {
        // for-each iteration descends through the guarded get(int), so deep nesting
        // is rejected on the iteration path too.
        PqVariant root = new PqVariantImpl(EMPTY_METADATA, nestInArrays(VariantValueDecoder.MAX_NESTING_DEPTH + 100));
        assertThatThrownBy(() -> descendArrayByIteration(root))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nesting");
    }

    /// Descend through array-of-array values via [PqVariantArray#get(int)] until a
    /// non-array leaf is reached, returning it.
    private static PqVariant descendArrayToLeaf(PqVariant value) {
        PqVariant current = value;
        while (current.type() == VariantType.ARRAY) {
            current = current.asArray().get(0);
        }
        return current;
    }

    /// Descend through array-of-array values via for-each iteration, which routes
    /// through [PqVariantArray#get(int)].
    private static void descendArrayByIteration(PqVariant value) {
        PqVariant current = value;
        while (current.type() == VariantType.ARRAY) {
            for (PqVariant element : current.asArray()) {
                current = element;
                break;
            }
        }
    }

    /// Descend `levels` deep through object-in-object values via
    /// [PqVariantObject#getObject(String)] on field `"a"`.
    private static void descendObject(PqVariantObject object, int levels) {
        PqVariantObject current = object;
        for (int i = 0; i < levels; i++) {
            current = current.getObject("a");
        }
    }

    /// Wrap [#LEAF_INT8] in `levels` single-element arrays, innermost first.
    private static byte[] nestInArrays(int levels) {
        byte[] current = LEAF_INT8;
        for (int i = 0; i < levels; i++) {
            byte[] buf = new byte[current.length + 16];
            int end = VariantValueEncoder.writeArray(buf, 0, new byte[][] { current });
            current = Arrays.copyOf(buf, end);
        }
        return current;
    }

    /// Wrap [#LEAF_INT8] in `levels` single-field objects keyed on `"a"` (id 0).
    private static byte[] nestInObjects(int levels) {
        byte[] current = LEAF_INT8;
        for (int i = 0; i < levels; i++) {
            byte[] buf = new byte[current.length + 16];
            int end = VariantValueEncoder.writeObject(buf, 0, new int[] { 0 }, new byte[][] { current }, 0);
            current = Arrays.copyOf(buf, end);
        }
        return current;
    }

    private static PqVariant load(String caseName) throws IOException {
        byte[] metadata = readResource("/variant/" + caseName + ".metadata");
        byte[] value = readResource("/variant/" + caseName + ".value");
        return new PqVariantImpl(metadata, value);
    }

    private static byte[] readResource(String name) throws IOException {
        try (InputStream in = PqVariantInvalidInputTest.class.getResourceAsStream(name)) {
            if (in == null) {
                throw new IOException("Missing test resource: " + name);
            }
            return in.readAllBytes();
        }
    }
}
