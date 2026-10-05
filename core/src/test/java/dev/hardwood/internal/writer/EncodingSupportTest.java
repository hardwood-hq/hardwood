/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;

import static org.assertj.core.api.Assertions.assertThat;

/// [EncodingSupport] against the encoders it describes.
///
/// The table is the writer's answer to "what can this type carry", and anything enumerating the
/// writer's capabilities reads it rather than keeping a copy. That only holds while it agrees
/// with the encoders themselves, and one of the two answers it gives is reached from a single
/// encoder: `BooleanValueEncoder` asks the table whether a `BOOLEAN` can be dictionary-encoded,
/// while the other five decide from whether they were given a dictionary to fill. Nothing but
/// this test holds those two accounts to each other.
class EncodingSupportTest {

    /// Every encoder built under [ColumnEncoding#AUTO] — the one policy that builds a dictionary
    /// at all — reports the capability the table claims for its type.
    @Test
    void theDictionaryTableAgreesWithEveryEncoder() {
        for (PhysicalType type : PhysicalType.values()) {
            if (type == PhysicalType.INT96) {
                continue; // Not writable, so it has no encoder to disagree with.
            }
            ValueEncoder encoder = ValueEncoder.forColumn(column(type), ColumnEncoding.AUTO, 64, 4096);

            assertThat(encoder.dictionaryCapable())
                    .as("%s: EncodingSupport says %s", type, EncodingSupport.dictionaryCapable(type))
                    .isEqualTo(EncodingSupport.dictionaryCapable(type));
        }
    }

    /// A column under a named policy builds no dictionary whatever its type, so the table
    /// describes what `AUTO` may reach rather than what every chunk holds.
    @Test
    void aNamedPolicyBuildsNoDictionary() {
        ValueEncoder encoder = ValueEncoder.forColumn(
                column(PhysicalType.INT64), ColumnEncoding.PLAIN, 64, 4096);

        assertThat(encoder.dictionaryCapable()).isFalse();
    }

    private static ColumnSchema column(PhysicalType type) {
        FileSchema.Builder schema = FileSchema.builder("schema");
        return (type == PhysicalType.FIXED_LEN_BYTE_ARRAY
                ? schema.addColumn("v", type, RepetitionType.REQUIRED, c -> c.typeLength(8))
                : schema.addColumn("v", type, RepetitionType.REQUIRED))
                .build()
                .getColumn(0);
    }
}
