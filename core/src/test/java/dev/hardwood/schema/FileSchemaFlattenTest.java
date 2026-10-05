/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.schema;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;

import static dev.hardwood.metadata.SchemaElement.fixedLengthPrimitive;
import static dev.hardwood.metadata.SchemaElement.primitive;
import static dev.hardwood.metadata.SchemaElement.root;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Characterization test for [FileSchema#toSchemaElements], the footer-write path
/// [dev.hardwood.writer.ParquetFileWriter] calls: pins what survives a round trip
/// through [FileSchema#fromSchemaElements] and back.
///
/// `type_length` carries both of the meanings parquet.thrift gives it: the byte length
/// of a `FIXED_LEN_BYTE_ARRAY` value, and the maximum bit length of a value of any other
/// physical type. Both are optional in the footer, so all four combinations reach the
/// reader and each one has to come back out unchanged.
class FileSchemaFlattenTest {

    private static SchemaElement onlyColumnOf(SchemaElement column) {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(root("schema", 1), column));
        List<SchemaElement> flattened = schema.toSchemaElements();
        assertThat(flattened).hasSize(2);
        return flattened.get(1);
    }

    @Test
    void fixedWidthColumnKeepsItsByteLength() {
        SchemaElement column = onlyColumnOf(fixedLengthPrimitive("digest", 16, RepetitionType.REQUIRED));

        assertThat(column.type()).isEqualTo(PhysicalType.FIXED_LEN_BYTE_ARRAY);
        assertThat(column.typeLength()).isEqualTo(16);
    }

    /// `type_length` is optional in the footer, so a `FIXED_LEN_BYTE_ARRAY` element can
    /// arrive without one. Flattening preserves the missing width; a later writer validation
    /// rejects the schema before it can write values.
    @Test
    void fixedWidthColumnWithNoByteLengthSurvivesFlattening() {
        SchemaElement noWidth = new SchemaElement("digest", PhysicalType.FIXED_LEN_BYTE_ARRAY, null,
                RepetitionType.REQUIRED, null, null, null, null, null, null);

        SchemaElement column = onlyColumnOf(noWidth);

        assertThat(column.type()).isEqualTo(PhysicalType.FIXED_LEN_BYTE_ARRAY);
        assertThat(column.typeLength()).isNull();
    }

    /// A non-FLBA element may carry a `type_length`, which the format defines as the
    /// maximum bit length of a value. The primitive factories do not express it, so the
    /// canonical constructor builds it. It does not affect decoding, and it survives the
    /// round trip.
    @Test
    void maximumBitLengthOfIntColumnSurvivesFlattening() {
        SchemaElement tag = new SchemaElement("tag", PhysicalType.INT32, 3, RepetitionType.REQUIRED, null, null, null,
                null, null, null);

        SchemaElement column = onlyColumnOf(tag);

        assertThat(column.type()).isEqualTo(PhysicalType.INT32);
        assertThat(column.typeLength()).isEqualTo(3);
    }

    /// `Builder.map` rejects a `FIXED_LEN_BYTE_ARRAY` key declared with neither a type length
    /// nor an annotation implying one.
    @Test
    void mapWithFixedWidthKeyRequiresAByteLength() {
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .map("m", RepetitionType.OPTIONAL, PhysicalType.FIXED_LEN_BYTE_ARRAY,
                        value -> value.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("FIXED_LEN_BYTE_ARRAY column key requires a positive type length");
    }

    @Test
    void columnWithNoTypeLengthStaysWithout() {
        SchemaElement column = onlyColumnOf(primitive("id", PhysicalType.INT64, RepetitionType.REQUIRED));

        assertThat(column.type()).isEqualTo(PhysicalType.INT64);
        assertThat(column.typeLength()).isNull();
    }
}
