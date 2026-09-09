/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.schema;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.LogicalType.EdgeInterpolationAlgorithm;
import dev.hardwood.metadata.LogicalType.TimeUnit;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Tests for declaring logical type annotations through [FileSchema.Builder] and lowering them
/// back to the [SchemaElement] list written to the footer.
///
/// parquet-format requires a writer to emit the modern `LogicalType` union *and* the legacy
/// `converted_type` wherever one exists, so both representations are asserted on every
/// annotation that has both.
class FileSchemaLogicalTypeTest {

    private static SchemaElement element(FileSchema schema, String name) {
        return schema.toSchemaElements().stream()
                .filter(candidate -> candidate.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No schema element named " + name));
    }

    private static FileSchema withColumn(PhysicalType type, LogicalType logicalType) {
        return FileSchema.builder("schema")
                .addColumn("annotated", type, RepetitionType.OPTIONAL, logicalType)
                .build();
    }

    private static SchemaElement lowered(PhysicalType type, LogicalType logicalType) {
        return element(withColumn(type, logicalType), "annotated");
    }

    @Test
    void stringCarriesBothRepresentations() {
        SchemaElement string = lowered(PhysicalType.BYTE_ARRAY, new LogicalType.StringType());

        assertThat(string.logicalType()).isEqualTo(new LogicalType.StringType());
        assertThat(string.convertedType()).isEqualTo(ConvertedType.UTF8);
    }

    @Test
    void decimalCarriesScaleAndPrecisionAlongsideTheUnion() {
        SchemaElement decimal = lowered(PhysicalType.INT64, new LogicalType.DecimalType(2, 18));

        assertThat(decimal.logicalType()).isEqualTo(new LogicalType.DecimalType(2, 18));
        assertThat(decimal.convertedType()).isEqualTo(ConvertedType.DECIMAL);
        assertThat(decimal.scale()).isEqualTo(2);
        assertThat(decimal.precision()).isEqualTo(18);
    }

    @Test
    void signedAndUnsignedIntegersMapToTheirLegacyEnum() {
        assertThat(lowered(PhysicalType.INT32, new LogicalType.IntType(16, true)).convertedType())
                .isEqualTo(ConvertedType.INT_16);
        assertThat(lowered(PhysicalType.INT32, new LogicalType.IntType(16, false)).convertedType())
                .isEqualTo(ConvertedType.UINT_16);
        assertThat(lowered(PhysicalType.INT64, new LogicalType.IntType(64, false)).convertedType())
                .isEqualTo(ConvertedType.UINT_64);
    }

    /// The legacy annotations denoted UTC-normalized values, but parquet-format requires a
    /// writer to annotate local times with them too, so libraries predating the union still see
    /// an annotation. The union carries the exact semantics.
    @Test
    void localTimestampStillCarriesTheLegacyAnnotation() {
        SchemaElement local = lowered(PhysicalType.INT64, new LogicalType.TimestampType(false, TimeUnit.MILLIS));

        assertThat(local.logicalType()).isEqualTo(new LogicalType.TimestampType(false, TimeUnit.MILLIS));
        assertThat(local.convertedType()).isEqualTo(ConvertedType.TIMESTAMP_MILLIS);
    }

    @Test
    void nanosecondUnitsAreUnionOnly() {
        assertThat(lowered(PhysicalType.INT64, new LogicalType.TimestampType(true, TimeUnit.NANOS)).convertedType())
                .isNull();
        assertThat(lowered(PhysicalType.INT64, new LogicalType.TimeType(true, TimeUnit.NANOS)).convertedType())
                .isNull();
    }

    @Test
    void typesWithoutALegacyEquivalentAreUnionOnly() {
        SchemaElement uuid = element(FileSchema.builder("schema")
                .addColumn("id", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 16,
                        new LogicalType.UuidType())
                .build(), "id");

        assertThat(uuid.logicalType()).isEqualTo(new LogicalType.UuidType());
        assertThat(uuid.convertedType()).isNull();
    }

    /// parquet.thrift reserves the INTERVAL union member without defining it, so an interval
    /// column is annotated by the legacy `converted_type` alone — and still reads back as the
    /// `IntervalType` the caller declared.
    @Test
    void intervalIsLegacyOnly() {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("duration", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 12,
                        new LogicalType.IntervalType())
                .build();
        SchemaElement interval = element(schema, "duration");

        assertThat(interval.logicalType()).isNull();
        assertThat(interval.convertedType()).isEqualTo(ConvertedType.INTERVAL);
        assertThat(schema.getColumn("duration").logicalType()).isEqualTo(new LogicalType.IntervalType());
    }

    @Test
    void listAndMapGroupsCarryBothRepresentations() {
        FileSchema schema = FileSchema.builder("schema")
                .list("tags", RepetitionType.OPTIONAL, element -> element.primitive(
                        PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, new LogicalType.StringType()))
                .map("counts", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY,
                        value -> value.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        assertThat(element(schema, "tags").logicalType()).isEqualTo(new LogicalType.ListType());
        assertThat(element(schema, "tags").convertedType()).isEqualTo(ConvertedType.LIST);
        assertThat(element(schema, "counts").logicalType()).isEqualTo(new LogicalType.MapType());
        assertThat(element(schema, "counts").convertedType()).isEqualTo(ConvertedType.MAP);
        assertThat(element(schema, "element").logicalType()).isEqualTo(new LogicalType.StringType());
    }

    /// A map key is a primitive like any other and carries its own annotation; without one the
    /// key of a `MAP<STRING, …>` reads back as an opaque `BYTE_ARRAY`.
    @Test
    void mapKeysCarryTheirAnnotation() {
        FileSchema schema = FileSchema.builder("schema")
                .map("counts", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY, new LogicalType.StringType(),
                        value -> value.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        assertThat(element(schema, "key").logicalType()).isEqualTo(new LogicalType.StringType());
        assertThat(element(schema, "key").convertedType()).isEqualTo(ConvertedType.UTF8);
        assertThat(schema.getColumn("counts.key_value.key").logicalType())
                .isEqualTo(new LogicalType.StringType());
    }

    @Test
    void mapKeysCarryAFixedLengthAnnotation() {
        FileSchema schema = FileSchema.builder("schema")
                .map("byId", RepetitionType.OPTIONAL, PhysicalType.FIXED_LEN_BYTE_ARRAY, 16,
                        new LogicalType.UuidType(),
                        value -> value.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        assertThat(element(schema, "key").logicalType()).isEqualTo(new LogicalType.UuidType());
        assertThat(element(schema, "key").typeLength()).isEqualTo(16);
    }

    /// The key overloads exist on all three builders, and a map nested in a struct, inside a list,
    /// or as another map's value reaches a different one each time. Each is a hand-written
    /// delegation, so a transposed or dropped argument in one of them would otherwise surface only
    /// once a caller wrote that shape.
    @Test
    void nestedMapsCarryTheirKeyAnnotation() {
        FileSchema schema = FileSchema.builder("schema")
                .struct("s", RepetitionType.OPTIONAL, group -> group
                        .map("counts", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY,
                                new LogicalType.StringType(),
                                value -> value.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                        .map("byId", RepetitionType.OPTIONAL, PhysicalType.FIXED_LEN_BYTE_ARRAY, 16,
                                new LogicalType.UuidType(),
                                value -> value.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .list("tags", RepetitionType.OPTIONAL, element -> element.map(
                        RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY, new LogicalType.StringType(),
                        value -> value.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .map("outer", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY, new LogicalType.StringType(),
                        value -> value.map(RepetitionType.OPTIONAL, PhysicalType.FIXED_LEN_BYTE_ARRAY, 16,
                                new LogicalType.UuidType(),
                                inner -> inner.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .build();

        assertThat(schema.getColumn("s.counts.key_value.key").logicalType())
                .isEqualTo(new LogicalType.StringType());
        assertThat(schema.getColumn("s.byId.key_value.key").logicalType())
                .isEqualTo(new LogicalType.UuidType());
        assertThat(schema.getColumn("s.byId.key_value.key").typeLength()).isEqualTo(16);
        assertThat(schema.getColumn("tags.list.element.key_value.key").logicalType())
                .isEqualTo(new LogicalType.StringType());
        assertThat(schema.getColumn("outer.key_value.value.key_value.key").logicalType())
                .isEqualTo(new LogicalType.UuidType());
        assertThat(schema.getColumn("outer.key_value.value.key_value.key").typeLength()).isEqualTo(16);
    }

    /// The key goes through the same validation as any other primitive, so an illegal pairing —
    /// or a `FIXED_LEN_BYTE_ARRAY` key with no length, which used to fail only once the writer
    /// reached the column — is rejected where the map is declared.
    @Test
    void mapKeysAreValidatedWhereTheyAreDeclared() {
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .map("counts", RepetitionType.OPTIONAL, PhysicalType.INT32, new LogicalType.StringType(),
                        value -> value.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("STRING annotates a BYTE_ARRAY column, not INT32 (column key)");
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .map("byId", RepetitionType.OPTIONAL, PhysicalType.FIXED_LEN_BYTE_ARRAY,
                        value -> value.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("FIXED_LEN_BYTE_ARRAY column key requires a positive type length");
    }

    @Test
    void nestedFieldsCarryTheirAnnotation() {
        FileSchema schema = FileSchema.builder("schema")
                .struct("person", RepetitionType.OPTIONAL, person -> person
                        .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                                new LogicalType.StringType())
                        .addColumn("born", PhysicalType.INT32, RepetitionType.OPTIONAL,
                                new LogicalType.DateType()))
                .build();

        assertThat(schema.getColumn("person.name").logicalType()).isEqualTo(new LogicalType.StringType());
        assertThat(element(schema, "born").convertedType()).isEqualTo(ConvertedType.DATE);
    }

    /// A schema read from a file that predates the union carries only the legacy annotation.
    /// Writing it back out must emit both representations — and must not lose the decimal's
    /// scale and precision, which the legacy form keeps in sibling fields.
    @Test
    void legacyOnlySchemaIsRewrittenWithBothRepresentations() {
        List<SchemaElement> legacy = List.of(
                new SchemaElement("schema", null, null, RepetitionType.REQUIRED, 2, null, null, null, null, null),
                new SchemaElement("name", PhysicalType.BYTE_ARRAY, null, RepetitionType.OPTIONAL, null,
                        ConvertedType.UTF8, null, null, null, null),
                new SchemaElement("amount", PhysicalType.INT64, null, RepetitionType.REQUIRED, null,
                        ConvertedType.DECIMAL, 4, 15, null, null));

        List<SchemaElement> rewritten = FileSchema.fromSchemaElements(legacy).toSchemaElements();

        assertThat(rewritten.get(1).logicalType()).isEqualTo(new LogicalType.StringType());
        assertThat(rewritten.get(1).convertedType()).isEqualTo(ConvertedType.UTF8);
        assertThat(rewritten.get(2).logicalType()).isEqualTo(new LogicalType.DecimalType(4, 15));
        assertThat(rewritten.get(2).convertedType()).isEqualTo(ConvertedType.DECIMAL);
        assertThat(rewritten.get(2).scale()).isEqualTo(4);
        assertThat(rewritten.get(2).precision()).isEqualTo(15);
    }

    @Test
    void legacyOnlyListGroupIsRewrittenWithBothRepresentations() {
        List<SchemaElement> legacy = List.of(
                new SchemaElement("schema", null, null, RepetitionType.REQUIRED, 1, null, null, null, null, null),
                new SchemaElement("tags", null, null, RepetitionType.OPTIONAL, 1,
                        ConvertedType.LIST, null, null, null, null),
                new SchemaElement("list", null, null, RepetitionType.REPEATED, 1, null, null, null, null, null),
                new SchemaElement("element", PhysicalType.INT32, null, RepetitionType.OPTIONAL, null,
                        null, null, null, null, null));

        List<SchemaElement> rewritten = FileSchema.fromSchemaElements(legacy).toSchemaElements();

        assertThat(rewritten.get(1).logicalType()).isEqualTo(new LogicalType.ListType());
        assertThat(rewritten.get(1).convertedType()).isEqualTo(ConvertedType.LIST);
    }

    @Test
    void annotationMustMatchThePhysicalType() {
        assertThatThrownBy(() -> withColumn(PhysicalType.INT32, new LogicalType.StringType()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("STRING annotates a BYTE_ARRAY column, not INT32"
                        + " (column annotated)");
        assertThatThrownBy(() -> withColumn(PhysicalType.INT64, new LogicalType.DateType()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> withColumn(PhysicalType.INT32, new LogicalType.IntType(64, true)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> withColumn(PhysicalType.INT64, new LogicalType.TimeType(true, TimeUnit.MILLIS)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatCode(() -> withColumn(PhysicalType.BYTE_ARRAY,
                new LogicalType.GeographyType("OGC:CRS84", EdgeInterpolationAlgorithm.SPHERICAL)))
                .doesNotThrowAnyException();
    }

    @Test
    void fixedWidthAnnotationsRequireTheirExactLength() {
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .addColumn("id", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 8,
                        new LogicalType.UuidType())
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("UUID annotates a FIXED_LEN_BYTE_ARRAY of length 16, not 8 (column id)");
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .addColumn("half", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 4,
                        new LogicalType.Float16Type())
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("FLOAT16 annotates a FIXED_LEN_BYTE_ARRAY of length 2, not 4 (column half)");
    }

    @Test
    void decimalPrecisionMustFitThePhysicalType() {
        assertThatThrownBy(() -> withColumn(PhysicalType.INT32, new LogicalType.DecimalType(0, 10)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("DECIMAL precision 10 exceeds the maximum 9 a INT32 can represent on column "
                         + "annotated");
        assertThatThrownBy(() -> withColumn(PhysicalType.INT64, new LogicalType.DecimalType(0, 19)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("DECIMAL precision 19 exceeds the maximum 18 a INT64 can represent on column "
                         + "annotated");
        // Four bytes of two's complement span 9 digits, the same as an INT32.
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .addColumn("amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 4,
                        new LogicalType.DecimalType(0, 10))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("DECIMAL precision 10 exceeds the maximum 9 a FIXED_LEN_BYTE_ARRAY can "
                         + "represent on column amount");
        assertThatThrownBy(() -> withColumn(PhysicalType.BYTE_ARRAY, new LogicalType.DecimalType(5, 4)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("DECIMAL scale 5 exceeds precision 4 on column annotated");
        assertThatThrownBy(() -> withColumn(PhysicalType.BOOLEAN, new LogicalType.DecimalType(0, 4)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void groupAnnotationsAreRejectedOnAPrimitive() {
        assertThatThrownBy(() -> withColumn(PhysicalType.BYTE_ARRAY, new LogicalType.ListType()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("LIST annotates a group, not a primitive column: annotated; declare it with "
                         + "the list or map builder verb instead");
        assertThatThrownBy(() -> withColumn(PhysicalType.BYTE_ARRAY, new LogicalType.MapType()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> withColumn(PhysicalType.BYTE_ARRAY, new LogicalType.VariantType(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("VARIANT annotates a group of metadata and value children, which the writer "
                         + "does not yet build: annotated");
    }

    /// UNKNOWN describes a column that holds only nulls, so it annotates any physical type.
    @Test
    void unknownAnnotatesAnyPhysicalType() {
        for (PhysicalType type : List.of(PhysicalType.BOOLEAN, PhysicalType.INT32, PhysicalType.DOUBLE,
                PhysicalType.BYTE_ARRAY)) {
            assertThat(lowered(type, new LogicalType.NullType()).logicalType())
                    .isEqualTo(new LogicalType.NullType());
        }
    }

    /// A `REQUIRED` column can hold no null, so nothing it could legally contain matches an
    /// `UNKNOWN` annotation — and reading such a column back fails on the null the annotation
    /// promises. The pairing is rejected where it is declared instead.
    @Test
    void unknownIsRejectedOnARequiredColumn() {
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED, new LogicalType.NullType())
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("UNKNOWN annotates a column holding only nulls, so it cannot be REQUIRED "
                         + "(column v)")
                
                ;
    }

    /// Every annotation with a legacy equivalent must resolve from the `converted_type` alone, so
    /// a pre-union reader — one that ignores field 10 — still sees it. Stripping the union from a
    /// schema built with annotations is exactly what such a reader observes.
    @ParameterizedTest
    @MethodSource("legacyEquivalents")
    void annotationsResolveFromTheConvertedTypeAlone(PhysicalType type, LogicalType logicalType) {
        assertThat(withoutUnion(type, logicalType).getColumn("annotated").logicalType())
                .isEqualTo(logicalType);
    }

    static Stream<Arguments> legacyEquivalents() {
        return Stream.of(
                Arguments.of(PhysicalType.BYTE_ARRAY, new LogicalType.StringType()),
                Arguments.of(PhysicalType.BYTE_ARRAY, new LogicalType.EnumType()),
                Arguments.of(PhysicalType.BYTE_ARRAY, new LogicalType.JsonType()),
                Arguments.of(PhysicalType.BYTE_ARRAY, new LogicalType.BsonType()),
                Arguments.of(PhysicalType.INT32, new LogicalType.DateType()),
                Arguments.of(PhysicalType.INT32, new LogicalType.DecimalType(2, 9)),
                Arguments.of(PhysicalType.INT64, new LogicalType.DecimalType(4, 18)),
                Arguments.of(PhysicalType.INT32, new LogicalType.IntType(8, true)),
                Arguments.of(PhysicalType.INT32, new LogicalType.IntType(16, false)),
                Arguments.of(PhysicalType.INT32, new LogicalType.IntType(32, true)),
                Arguments.of(PhysicalType.INT64, new LogicalType.IntType(64, false)),
                Arguments.of(PhysicalType.INT32, new LogicalType.TimeType(true, TimeUnit.MILLIS)),
                Arguments.of(PhysicalType.INT64, new LogicalType.TimeType(true, TimeUnit.MICROS)),
                Arguments.of(PhysicalType.INT64, new LogicalType.TimestampType(true, TimeUnit.MILLIS)),
                Arguments.of(PhysicalType.INT64, new LogicalType.TimestampType(true, TimeUnit.MICROS)));
    }

    /// The legacy annotations denoted UTC-normalized values and cannot express a local one, so a
    /// pre-union reader necessarily resolves a local timestamp as UTC-normalized. A reader that
    /// takes field 10 gets the exact semantics.
    @Test
    void aLocalTimestampResolvesAsUtcFromTheConvertedTypeAlone() {
        FileSchema legacy = withoutUnion(PhysicalType.INT64, new LogicalType.TimestampType(false, TimeUnit.MILLIS));

        assertThat(legacy.getColumn("annotated").logicalType())
                .isEqualTo(new LogicalType.TimestampType(true, TimeUnit.MILLIS));
    }

    /// The schema as a reader that ignores the `LogicalType` union would reconstruct it.
    private static FileSchema withoutUnion(PhysicalType type, LogicalType logicalType) {
        List<SchemaElement> stripped = withColumn(type, logicalType).toSchemaElements().stream()
                .map(element -> new SchemaElement(element.name(), element.type(), element.typeLength(),
                        element.repetitionType(), element.numChildren(), element.convertedType(),
                        element.scale(), element.precision(), element.fieldId(), null))
                .toList();
        return FileSchema.fromSchemaElements(stripped);
    }
}
