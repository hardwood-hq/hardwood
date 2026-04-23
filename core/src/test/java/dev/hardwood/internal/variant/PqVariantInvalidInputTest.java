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

import org.junit.jupiter.api.Test;

import dev.hardwood.row.PqVariant;
import dev.hardwood.row.PqVariantArray;
import dev.hardwood.row.PqVariantObject;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Pins the fail-early error paths on the Variant read surface. Each accessor
/// that rejects malformed input or out-of-contract calls has at least one
/// assertion here so silent-regression to wrong-but-plausible output is
/// caught immediately.
class PqVariantInvalidInputTest {

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
