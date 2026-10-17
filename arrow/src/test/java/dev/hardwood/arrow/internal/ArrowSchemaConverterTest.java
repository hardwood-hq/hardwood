/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.arrow.internal;

import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.SchemaIncompatibleException;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static dev.hardwood.metadata.RepetitionType.OPTIONAL;
import static dev.hardwood.metadata.RepetitionType.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ArrowSchemaConverterTest {

    private static ArrowPlan plan(FileSchema schema, ColumnProjection projection) {
        return ArrowSchemaConverter.plan(schema, schema.toSchemaElements(), Map.of(), projection);
    }

    private static ArrowPlan.Node single(PhysicalType type, LogicalType annotation) {
        FileSchema schema = FileSchema.builder("root").addColumn("c", type, OPTIONAL, annotation).build();
        return plan(schema, ColumnProjection.all()).roots().get(0);
    }

    private static ArrowPlan.Node singleFixed(int width, LogicalType annotation) {
        FileSchema schema = FileSchema.builder("root")
                .addColumn("c", PhysicalType.FIXED_LEN_BYTE_ARRAY, OPTIONAL, width, annotation)
                .build();
        return plan(schema, ColumnProjection.all()).roots().get(0);
    }

    @Test
    void physicalTypesMapToTheirArrowCounterparts() {
        assertMapping(single(PhysicalType.BOOLEAN, null), ArrowType.Bool.INSTANCE, ScalarKind.BOOLEAN);
        assertMapping(single(PhysicalType.INT32, null), new ArrowType.Int(32, true), ScalarKind.INT32);
        assertMapping(single(PhysicalType.INT64, null), new ArrowType.Int(64, true), ScalarKind.INT64);
        assertMapping(single(PhysicalType.FLOAT, null), new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
                ScalarKind.FLOAT);
        assertMapping(single(PhysicalType.DOUBLE, null), new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
                ScalarKind.DOUBLE);
        assertMapping(single(PhysicalType.BYTE_ARRAY, null), ArrowType.Binary.INSTANCE, ScalarKind.BINARY);
        assertMapping(single(PhysicalType.INT96, null), new ArrowType.Timestamp(TimeUnit.NANOSECOND, null),
                ScalarKind.INT96_TIMESTAMP);
        assertMapping(singleFixed(7, null), new ArrowType.FixedSizeBinary(7), ScalarKind.FIXED_BINARY);
    }

    @Test
    void integerAnnotationsMapByWidthAndSign() {
        assertMapping(single(PhysicalType.INT32, LogicalType.intType(8, true)), new ArrowType.Int(8, true),
                ScalarKind.INT8);
        assertMapping(single(PhysicalType.INT32, LogicalType.intType(16, true)), new ArrowType.Int(16, true),
                ScalarKind.INT16);
        assertMapping(single(PhysicalType.INT32, LogicalType.intType(32, true)), new ArrowType.Int(32, true),
                ScalarKind.INT32);
        assertMapping(single(PhysicalType.INT32, LogicalType.intType(8, false)), new ArrowType.Int(8, false),
                ScalarKind.UINT8);
        assertMapping(single(PhysicalType.INT32, LogicalType.intType(16, false)), new ArrowType.Int(16, false),
                ScalarKind.UINT16);
        assertMapping(single(PhysicalType.INT32, LogicalType.intType(32, false)), new ArrowType.Int(32, false),
                ScalarKind.INT32);
        assertMapping(single(PhysicalType.INT64, LogicalType.intType(64, true)), new ArrowType.Int(64, true),
                ScalarKind.INT64);
        assertMapping(single(PhysicalType.INT64, LogicalType.intType(64, false)), new ArrowType.Int(64, false),
                ScalarKind.INT64);
    }

    @Test
    void temporalAnnotationsMapToArrowTemporalTypes() {
        assertMapping(single(PhysicalType.INT32, LogicalType.date()), new ArrowType.Date(DateUnit.DAY),
                ScalarKind.INT32);
        assertMapping(single(PhysicalType.INT32, LogicalType.time(true, LogicalType.TimeUnit.MILLIS)),
                new ArrowType.Time(TimeUnit.MILLISECOND, 32), ScalarKind.INT32);
        assertMapping(single(PhysicalType.INT64, LogicalType.time(false, LogicalType.TimeUnit.MICROS)),
                new ArrowType.Time(TimeUnit.MICROSECOND, 64), ScalarKind.INT64);
        assertMapping(single(PhysicalType.INT64, LogicalType.time(true, LogicalType.TimeUnit.NANOS)),
                new ArrowType.Time(TimeUnit.NANOSECOND, 64), ScalarKind.INT64);
        assertMapping(single(PhysicalType.INT64, LogicalType.timestamp(true, LogicalType.TimeUnit.MILLIS)),
                new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), ScalarKind.INT64);
        assertMapping(single(PhysicalType.INT64, LogicalType.timestamp(false, LogicalType.TimeUnit.MICROS)),
                new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), ScalarKind.INT64);
        assertMapping(single(PhysicalType.INT64, LogicalType.timestamp(true, LogicalType.TimeUnit.NANOS)),
                new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"), ScalarKind.INT64);
        assertMapping(singleFixed(12, LogicalType.timestamp(true, LogicalType.TimeUnit.NANOS)),
                new ArrowType.FixedSizeBinary(12), ScalarKind.FIXED_BINARY);
    }

    @Test
    void decimalsWidenByPrecisionAndCarrier() {
        assertMapping(single(PhysicalType.INT32, LogicalType.decimal(9, 2)), new ArrowType.Decimal(9, 2, 128),
                ScalarKind.DECIMAL_INT32);
        assertMapping(single(PhysicalType.INT64, LogicalType.decimal(18, 4)), new ArrowType.Decimal(18, 4, 128),
                ScalarKind.DECIMAL_INT64);
        assertMapping(single(PhysicalType.BYTE_ARRAY, LogicalType.decimal(38, 10)),
                new ArrowType.Decimal(38, 10, 128), ScalarKind.DECIMAL_BYTES);
        assertMapping(singleFixed(16, LogicalType.decimal(38, 0)), new ArrowType.Decimal(38, 0, 128),
                ScalarKind.DECIMAL_BYTES);
        assertMapping(singleFixed(32, LogicalType.decimal(39, 5)), new ArrowType.Decimal(39, 5, 256),
                ScalarKind.DECIMAL_BYTES);
        assertMapping(singleFixed(32, LogicalType.decimal(76, 0)), new ArrowType.Decimal(76, 0, 256),
                ScalarKind.DECIMAL_BYTES);
    }

    @Test
    void decimalWiderThanArrowHoldsIsRejected() {
        assertThatThrownBy(() -> singleFixed(33, LogicalType.decimal(77, 0)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Column 'c' is a DECIMAL(77, 0): an Arrow decimal holds at most 76 digits");
        // A declared width past the Arrow decimal's is refused at build, as Arrow C++ refuses it.
        assertThatThrownBy(() -> singleFixed(17, LogicalType.decimal(10, 2)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Column 'c' is a DECIMAL(10, 2) over 17 bytes, wider than the 16-byte Arrow decimal "
                        + "of that precision");
        assertMapping(singleFixed(16, LogicalType.decimal(10, 2)), new ArrowType.Decimal(10, 2, 128),
                ScalarKind.DECIMAL_BYTES);
    }

    @Test
    void binaryAnnotationsMapToUtf8OrBinaryWithoutExtensions() {
        assertMapping(single(PhysicalType.BYTE_ARRAY, LogicalType.string()), ArrowType.Utf8.INSTANCE,
                ScalarKind.BINARY);
        assertMapping(single(PhysicalType.BYTE_ARRAY, LogicalType.json()), ArrowType.Utf8.INSTANCE,
                ScalarKind.BINARY);
        assertMapping(single(PhysicalType.BYTE_ARRAY, LogicalType.enumType()), ArrowType.Binary.INSTANCE,
                ScalarKind.BINARY);
        assertMapping(single(PhysicalType.BYTE_ARRAY, LogicalType.bson()), ArrowType.Binary.INSTANCE,
                ScalarKind.BINARY);
        assertMapping(single(PhysicalType.BYTE_ARRAY, LogicalType.geometry(null)), ArrowType.Binary.INSTANCE,
                ScalarKind.BINARY);
        assertMapping(singleFixed(16, LogicalType.uuid()), new ArrowType.FixedSizeBinary(16),
                ScalarKind.FIXED_BINARY);
        assertMapping(singleFixed(12, LogicalType.interval()), new ArrowType.FixedSizeBinary(12),
                ScalarKind.FIXED_BINARY);
        assertMapping(singleFixed(2, LogicalType.float16()),
                new ArrowType.FloatingPoint(FloatingPointPrecision.HALF), ScalarKind.FIXED_BINARY);
    }

    @Test
    void nullAnnotationMapsToTheNullType() {
        assertMapping(single(PhysicalType.INT32, LogicalType.nullType()), ArrowType.Null.INSTANCE, ScalarKind.NULL);
    }

    @Test
    void nullabilityFollowsRepetition() {
        FileSchema schema = FileSchema.builder("root")
                .addColumn("req", PhysicalType.INT32, REQUIRED)
                .addColumn("opt", PhysicalType.INT32, OPTIONAL)
                .build();
        List<Field> fields = plan(schema, ColumnProjection.all()).schema().getFields();
        assertThat(fields.get(0).isNullable()).isFalse();
        assertThat(fields.get(1).isNullable()).isTrue();
    }

    @Test
    void fieldIdsOfTopLevelColumnsAreCarriedAsMetadata() {
        // Hand-built elements: a top-level column with an id, a group whose child has an id
        // that must not leak onto the top-level column of the same name after it, a top-level
        // column without one, one with id zero, and one with a negative id, which Arrow C++
        // reads as none.
        List<SchemaElement> elements = List.of(
                SchemaElement.root("root", 5),
                new SchemaElement("a", PhysicalType.INT32, null, OPTIONAL, null, null, null, null, 7, null),
                new SchemaElement("g", null, null, OPTIONAL, 1, null, null, null, null, null),
                new SchemaElement("b", PhysicalType.INT32, null, OPTIONAL, null, null, null, null, 99, null),
                new SchemaElement("b", PhysicalType.INT32, null, OPTIONAL, null, null, null, null, null, null),
                new SchemaElement("zero", PhysicalType.INT32, null, OPTIONAL, null, null, null, null, 0, null),
                new SchemaElement("negative", PhysicalType.INT32, null, OPTIONAL, null, null, null, null, -7, null));
        FileSchema schema = FileSchema.fromSchemaElements(elements);
        ArrowPlan plan = ArrowSchemaConverter.plan(schema, elements, Map.of(),
                ColumnProjection.columns("a", "b", "zero", "negative"));
        List<Field> fields = plan.schema().getFields();
        assertThat(fields.get(0).getMetadata()).isEqualTo(Map.of(ArrowSchemaConverter.FIELD_ID_KEY, "7"));
        assertThat(fields.get(1).getMetadata()).isEmpty();
        assertThat(fields.get(2).getMetadata()).isEqualTo(Map.of(ArrowSchemaConverter.FIELD_ID_KEY, "0"));
        assertThat(fields.get(3).getMetadata()).isEmpty();
    }

    /// A name may hold a dot, so the ids are looked up by path components: a top-level
    /// `account.id` and the `id` inside `account` keep their own.
    @Test
    void fieldIdsOfDottedNamesDoNotCollide() {
        List<SchemaElement> elements = List.of(
                SchemaElement.root("root", 2),
                new SchemaElement("account.id", PhysicalType.INT32, null, OPTIONAL, null, null, null, null, 101, null),
                new SchemaElement("account", null, null, OPTIONAL, 1, null, null, null, null, null),
                new SchemaElement("id", PhysicalType.INT32, null, OPTIONAL, null, null, null, null, 202, null));
        FileSchema schema = FileSchema.fromSchemaElements(elements);
        ArrowPlan plan = ArrowSchemaConverter.plan(schema, elements, Map.of(), ColumnProjection.all());
        List<Field> fields = plan.schema().getFields();
        assertThat(fields).extracting(Field::getName).containsExactly("account.id", "account");
        assertThat(fields.get(0).getMetadata()).isEqualTo(Map.of(ArrowSchemaConverter.FIELD_ID_KEY, "101"));
        assertThat(fields.get(1).getMetadata()).isEmpty();
        assertThat(fields.get(1).getChildren().get(0).getMetadata())
                .isEqualTo(Map.of(ArrowSchemaConverter.FIELD_ID_KEY, "202"));
    }

    /// The `key_value` group's id goes nowhere: Arrow C++ leaves the `entries` struct without
    /// metadata, while the map, its key and its value keep theirs.
    @Test
    void mapEntriesCarryNoFieldId() {
        List<SchemaElement> elements = List.of(
                SchemaElement.root("root", 1),
                new SchemaElement("m", null, null, OPTIONAL, 1, null, null, null, 20, LogicalType.map()),
                new SchemaElement("key_value", null, null, RepetitionType.REPEATED, 2, null, null, null, 21, null),
                new SchemaElement("key", PhysicalType.INT32, null, REQUIRED, null, null, null, null, 22, null),
                new SchemaElement("value", PhysicalType.INT64, null, OPTIONAL, null, null, null, null, 23, null));
        FileSchema schema = FileSchema.fromSchemaElements(elements);
        ArrowPlan plan = ArrowSchemaConverter.plan(schema, elements, Map.of(), ColumnProjection.all());
        Field m = plan.schema().getFields().get(0);
        assertThat(m.getMetadata()).isEqualTo(Map.of(ArrowSchemaConverter.FIELD_ID_KEY, "20"));
        Field entries = m.getChildren().get(0);
        assertThat(entries.getMetadata()).isEmpty();
        assertThat(entries.getChildren().get(0).getMetadata()).isEqualTo(Map.of(ArrowSchemaConverter.FIELD_ID_KEY, "22"));
        assertThat(entries.getChildren().get(1).getMetadata()).isEqualTo(Map.of(ArrowSchemaConverter.FIELD_ID_KEY, "23"));
    }

    @Test
    void rootsFollowTheProjectionAndCollapseRepeats() {
        FileSchema schema = FileSchema.builder("root")
                .addColumn("a", PhysicalType.INT32, OPTIONAL)
                .addColumn("b", PhysicalType.INT64, OPTIONAL)
                .build();
        ArrowPlan plan = plan(schema, ColumnProjection.columns("b", "a", "b"));
        assertThat(plan.schema().getFields()).extracting(Field::getName).containsExactly("b", "a");
        assertThat(plan.roots()).extracting(ArrowPlan.Node::position).containsExactly(0, 1);
    }

    /// Groups map to Arrow's nested types, each owning the layer the leaf readers report for
    /// it: a required struct none, an optional struct a validity layer, a list or map a
    /// repeated layer.
    @Test
    void groupsMapToNestedTypesWithTheirLayers() {
        FileSchema schema = FileSchema.builder("root")
                .struct("address", OPTIONAL, s -> s
                        .addColumn("city", PhysicalType.BYTE_ARRAY, OPTIONAL, LogicalType.string())
                        .struct("geo", REQUIRED, g -> g.addColumn("lat", PhysicalType.DOUBLE, REQUIRED)))
                .list("tags", REQUIRED, e -> e.primitive(PhysicalType.BYTE_ARRAY, OPTIONAL, LogicalType.string()))
                .map("attrs", OPTIONAL, PhysicalType.BYTE_ARRAY, LogicalType.string(),
                        v -> v.primitive(PhysicalType.INT32, OPTIONAL))
                .list("rows", OPTIONAL, e -> e.struct(OPTIONAL, s -> s
                        .list("cells", OPTIONAL, c -> c.primitive(PhysicalType.INT32, OPTIONAL))))
                .build();
        ArrowPlan plan = plan(schema, ColumnProjection.all());
        List<Field> fields = plan.schema().getFields();
        assertThat(fields).extracting(Field::getName).containsExactly("address", "tags", "attrs", "rows");

        Field address = fields.get(0);
        assertThat(address.getType()).isEqualTo(ArrowType.Struct.INSTANCE);
        assertThat(address.isNullable()).isTrue();
        assertThat(address.getChildren()).extracting(Field::getName).containsExactly("city", "geo");
        assertThat(address.getChildren().get(1).isNullable()).isFalse();
        ArrowPlan.Node addressNode = plan.roots().get(0);
        assertThat(addressNode.shape()).isEqualTo(ArrowPlan.Shape.STRUCT);
        assertThat(addressNode.layer()).isEqualTo(0);
        assertThat(addressNode.children().get(1).layer()).isEqualTo(-1);

        Field tags = fields.get(1);
        assertThat(tags.getType()).isEqualTo(ArrowType.List.INSTANCE);
        assertThat(tags.isNullable()).isFalse();
        assertThat(tags.getChildren().get(0).getName()).isEqualTo("element");
        assertThat(tags.getChildren().get(0).isNullable()).isTrue();
        assertThat(plan.roots().get(1).layer()).isEqualTo(0);

        Field attrs = fields.get(2);
        assertThat(attrs.getType()).isEqualTo(new ArrowType.Map(false));
        Field entries = attrs.getChildren().get(0);
        assertThat(entries.getName()).isEqualTo("entries");
        assertThat(entries.isNullable()).isFalse();
        assertThat(entries.getChildren()).extracting(Field::getName).containsExactly("key", "value");
        assertThat(entries.getChildren().get(0).isNullable()).isFalse();
        assertThat(entries.getChildren().get(1).isNullable()).isTrue();

        ArrowPlan.Node rows = plan.roots().get(3);
        assertThat(rows.shape()).isEqualTo(ArrowPlan.Shape.LIST);
        assertThat(rows.layer()).isEqualTo(0);
        ArrowPlan.Node element = rows.children().get(0);
        assertThat(element.shape()).isEqualTo(ArrowPlan.Shape.STRUCT);
        assertThat(element.layer()).isEqualTo(1);
        ArrowPlan.Node cells = element.children().get(0);
        assertThat(cells.shape()).isEqualTo(ArrowPlan.Shape.LIST);
        assertThat(cells.layer()).isEqualTo(2);
    }

    /// A Variant group is the struct of the fields it stores: the storage of Arrow's Variant
    /// extension type, which is not applied.
    @Test
    void variantGroupsAreTheirStorageStruct() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("root", 1),
                new SchemaElement("v", null, null, OPTIONAL, 3, null, null, null, null, LogicalType.variant(1)),
                new SchemaElement("metadata", PhysicalType.BYTE_ARRAY, null, REQUIRED, null, null, null, null, null, null),
                new SchemaElement("value", PhysicalType.BYTE_ARRAY, null, OPTIONAL, null, null, null, null, null, null),
                new SchemaElement("typed_value", PhysicalType.INT64, null, OPTIONAL, null, null, null, null, null,
                        null)));
        ArrowPlan plan = plan(schema, ColumnProjection.all());
        Field v = plan.schema().getFields().get(0);
        assertThat(v.getType()).isEqualTo(ArrowType.Struct.INSTANCE);
        assertThat(v.isNullable()).isTrue();
        assertThat(v.getChildren()).extracting(Field::getName).containsExactly("metadata", "value", "typed_value");
        assertThat(v.getChildren()).extracting(Field::isNullable).containsExactly(false, true, true);
        assertThat(plan.roots().get(0).layer()).isEqualTo(0);
    }

    @Test
    void partialMapProjectionIsRejected() {
        FileSchema schema = FileSchema.builder("root")
                .map("attrs", OPTIONAL, PhysicalType.BYTE_ARRAY, LogicalType.string(),
                        v -> v.primitive(PhysicalType.INT32, OPTIONAL))
                .build();
        assertThat(plan(schema, ColumnProjection.columns("attrs")).roots()).hasSize(1);
        assertThatThrownBy(() -> plan(schema, ColumnProjection.columns("attrs.key_value.key")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Column 'attrs' is a MAP group of which only the key or only the value is projected: "
                        + "project the map as a whole");
    }

    /// A bare repeated primitive is a list of present elements, and a bare repeated group a
    /// list of present structs; the legacy two-level list element contributes no layer of
    /// its own. The field's id sits on the list, and on the struct of a group, but not on the
    /// synthetic element of a primitive, as Arrow C++ places it.
    @Test
    void bareRepeatedFieldsAreLists() {
        List<SchemaElement> elements = List.of(
                SchemaElement.root("root", 2),
                new SchemaElement("hits", PhysicalType.INT32, null, RepetitionType.REPEATED, null, null, null, null,
                        11, null),
                new SchemaElement("pairs", null, null, RepetitionType.REPEATED, 1, null, null, null, 12, null),
                new SchemaElement("x", PhysicalType.INT32, null, OPTIONAL, null, null, null, null, 13, null));
        FileSchema schema = FileSchema.fromSchemaElements(elements);
        ArrowPlan plan = ArrowSchemaConverter.plan(schema, elements, Map.of(), ColumnProjection.all());
        Field hits = plan.schema().getFields().get(0);
        assertThat(hits.getType()).isEqualTo(ArrowType.List.INSTANCE);
        assertThat(hits.isNullable()).isFalse();
        assertThat(hits.getMetadata()).isEqualTo(Map.of(ArrowSchemaConverter.FIELD_ID_KEY, "11"));
        assertThat(hits.getChildren().get(0).getName()).isEqualTo("hits");
        assertThat(hits.getChildren().get(0).isNullable()).isFalse();
        assertThat(hits.getChildren().get(0).getMetadata()).isEmpty();
        assertThat(plan.roots().get(0).layer()).isEqualTo(0);
        Field pairs = plan.schema().getFields().get(1);
        assertThat(pairs.getType()).isEqualTo(ArrowType.List.INSTANCE);
        assertThat(pairs.getMetadata()).isEqualTo(Map.of(ArrowSchemaConverter.FIELD_ID_KEY, "12"));
        Field pair = pairs.getChildren().get(0);
        assertThat(pair.getType()).isEqualTo(ArrowType.Struct.INSTANCE);
        assertThat(pair.isNullable()).isFalse();
        assertThat(pair.getMetadata()).isEqualTo(Map.of(ArrowSchemaConverter.FIELD_ID_KEY, "12"));
        assertThat(pair.getChildren().get(0).getMetadata()).isEqualTo(Map.of(ArrowSchemaConverter.FIELD_ID_KEY, "13"));
        assertThat(plan.roots().get(1).children().get(0).layer()).isEqualTo(-1);
    }

    @Test
    void unknownColumnIsRejectedAsCoreRejectsIt() {
        FileSchema schema = FileSchema.builder("root").addColumn("id", PhysicalType.INT32, REQUIRED).build();
        assertThatThrownBy(() -> plan(schema, ColumnProjection.columns("nope")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column not found: nope");
    }

    @Test
    void fixedWidthColumnWithoutWidthIsRejected() {
        List<SchemaElement> elements = List.of(
                SchemaElement.root("root", 1),
                new SchemaElement("f", PhysicalType.FIXED_LEN_BYTE_ARRAY, null, OPTIONAL, null, null, null, null,
                        null, null));
        FileSchema schema = FileSchema.fromSchemaElements(elements);
        assertThatThrownBy(() -> ArrowSchemaConverter.plan(schema, elements, Map.of(), ColumnProjection.all()))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessage("Column 'f' is a FIXED_LEN_BYTE_ARRAY that declares no type length");
    }

    private static void assertMapping(ArrowPlan.Node entry, ArrowType type, ScalarKind kind) {
        assertThat(entry.field().getType()).isEqualTo(type);
        assertThat(entry.kind()).isEqualTo(kind);
    }
}
