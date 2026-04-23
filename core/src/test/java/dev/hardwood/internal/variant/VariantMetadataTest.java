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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Verifies [VariantMetadata] against `parquet-testing/variant/*.metadata`
/// fixtures bundled into `core/src/test/resources/variant/`. Covers the empty
/// dictionary (used by all primitive fixtures), a populated sorted dictionary
/// (`object_primitive`), and the error path for a truncated buffer.
class VariantMetadataTest {

    @Test
    void emptyDictionary() throws IOException {
        byte[] bytes = readResource("/variant/primitive_int32.metadata");
        VariantMetadata metadata = new VariantMetadata(bytes);
        assertThat(metadata.size()).isZero();
        assertThat(metadata.findField("anything")).isEqualTo(-1);
    }

    @Test
    void objectPrimitiveDictionary() throws IOException {
        byte[] bytes = readResource("/variant/object_primitive.metadata");
        VariantMetadata metadata = new VariantMetadata(bytes);
        // object_primitive has 7 fields:
        //   boolean_false_field, boolean_true_field, double_field, int_field,
        //   null_field, string_field, timestamp_field
        assertThat(metadata.size()).isEqualTo(7);

        String[] expected = {
                "boolean_false_field",
                "boolean_true_field",
                "double_field",
                "int_field",
                "null_field",
                "string_field",
                "timestamp_field"
        };
        // Every expected name must be findable; if sorted, dictionary order
        // matches lexicographic order.
        for (String name : expected) {
            assertThat(metadata.findField(name))
                    .as("findField(%s)", name)
                    .isNotNegative();
            int id = metadata.findField(name);
            assertThat(metadata.getField(id)).isEqualTo(name);
        }
        assertThat(metadata.findField("missing_field")).isEqualTo(-1);
    }

    @Test
    void truncatedBufferRejected() {
        byte[] bytes = { 0x01 }; // header only, no dictionary size bytes
        assertThatThrownBy(() -> new VariantMetadata(bytes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("truncated");
    }

    @Test
    void emptyBufferRejected() {
        assertThatThrownBy(() -> new VariantMetadata(new byte[0]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static byte[] readResource(String name) throws IOException {
        try (InputStream in = VariantMetadataTest.class.getResourceAsStream(name)) {
            if (in == null) {
                throw new IOException("Missing test resource: " + name);
            }
            return in.readAllBytes();
        }
    }
}
