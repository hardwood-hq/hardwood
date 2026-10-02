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
import org.junit.jupiter.api.extension.RegisterExtension;

import dev.hardwood.internal.predicate.CapturedWarnings;
import dev.hardwood.internal.schema.BareRepeatedGroups;
import dev.hardwood.internal.schema.LeafAnnotation;
import dev.hardwood.internal.schema.LogicalTypeAnnotations;
import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// An annotation the footer gives a column that no physical type of that shape can carry, which the
/// reader drops rather than refusing the file.
///
/// parquet-format requires both halves of that: "readers should ignore both the logical type
/// annotation and column order for that column. Only the physical type information should be used
/// to process the column's data." This covers the annotation; `UnreadableSortOrderTest` covers the
/// bounds recorded in its order, `BoundsReadability` being package-private to the predicate code.
///
/// A legacy `converted_type` that promotes to an annotation its column cannot carry is dropped under
/// the same rule; `FileSchemaConvertedTypeTest` covers that promotion.
class DroppedAnnotationTest {

    private static final String ROOT = "root";
    private static final String COLUMN = "col";

    @RegisterExtension
    final CapturedWarnings warnings = new CapturedWarnings();

    /// A geospatial payload is WKB in a `BYTE_ARRAY`, the one physical type parquet-format allows it
    /// on, so either annotation is dropped off a column of any other type and kept on that one.
    @Test
    void aGeospatialAnnotationIsDroppedOffAnyPhysicalTypeButByteArray() {
        LogicalType geometry = LogicalType.geometry(null);

        SchemaElement onInt64 = leaf(PhysicalType.INT64, null, geometry);
        assertThat(annotationOf(onInt64)).isNull();
        assertThat(LeafAnnotation.dropFault(onInt64))
                .isEqualTo("GEOMETRY(OGC:CRS84) is read from BYTE_ARRAY, but the column is INT64");

        SchemaElement onByteArray = leaf(PhysicalType.BYTE_ARRAY, null, geometry);
        assertThat(annotationOf(onByteArray)).isEqualTo(geometry);
        assertThat(LeafAnnotation.dropFault(onByteArray)).isNull();

        SchemaElement geographyOnFixed = leaf(PhysicalType.FIXED_LEN_BYTE_ARRAY, 8,
                LogicalType.geography(null, null));
        assertThat(annotationOf(geographyOnFixed)).isNull();
        assertThat(LeafAnnotation.dropFault(geographyOnFixed))
                .isEqualTo("GEOGRAPHY(OGC:CRS84, SPHERICAL) is read from BYTE_ARRAY, but the column is FIXED_LEN_BYTE_ARRAY");
    }

    /// A group carries only the annotations of a structure: `LIST`, `MAP` and `VARIANT`, and the
    /// converted `LIST`, `MAP` and `MAP_KEY_VALUE`. Any other is dropped, leaving a plain struct.
    @Test
    void aPrimitiveAnnotationIsDroppedOffAGroup() {
        SchemaNode.GroupNode stringGroup = group(LogicalType.string(), null);
        assertThat(stringGroup.logicalType()).isNull();
        assertThat(stringGroup.isStruct()).isTrue();

        SchemaNode.GroupNode utf8Group = group(null, ConvertedType.UTF8);
        assertThat(utf8Group.convertedType()).isNull();
        assertThat(utf8Group.isStruct()).isTrue();

        assertThat(warnings.messages()).containsExactly(
                "Ignoring 1 annotation(s) their field cannot carry; each such field is read as though "
                        + "unannotated: g (STRING annotates a primitive, but the field is a group)",
                "Ignoring 1 annotation(s) their field cannot carry; each such field is read as though "
                        + "unannotated: g (UTF8 annotates a primitive, but the field is a group)");
    }

    /// The footer's elements are read the same way before the schema is built, so a group whose
    /// annotation is dropped and whose sole child is a repeated `MAP_KEY_VALUE` group is the
    /// legacy `MAP` that child makes it, not a struct over a bare repeated group.
    @Test
    void aGroupWhoseAnnotationIsDroppedStillReadsAsTheLegacyMapItsChildMakesIt() {
        List<SchemaElement> elements = List.of(
                SchemaElement.group(ROOT, RepetitionType.REQUIRED, 1),
                new SchemaElement("m", null, null, RepetitionType.OPTIONAL, 1, null, null, null, null,
                        LogicalType.string()),
                new SchemaElement("key_value", null, null, RepetitionType.REPEATED, 2,
                        ConvertedType.MAP_KEY_VALUE, null, null, null, null),
                SchemaElement.primitive("key", PhysicalType.INT32, RepetitionType.REQUIRED),
                SchemaElement.primitive("value", PhysicalType.INT32, RepetitionType.OPTIONAL));

        SchemaNode.GroupNode map = (SchemaNode.GroupNode) FileSchema.fromSchemaElements(
                BareRepeatedGroups.dropAnnotations(elements)).getRootNode().children().getFirst();

        assertThat(map.isMap()).isTrue();
    }

    /// The writer writes no annotation the reader would drop off a group.
    @Test
    void aPrimitiveAnnotationIsNotWrittenOnAGroup() {
        assertThatThrownBy(() -> LogicalTypeAnnotations.ofGroup(null, LogicalType.string()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("STRING annotates a primitive, not a group");
        assertThatThrownBy(() -> LogicalTypeAnnotations.ofGroup(ConvertedType.UTF8, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("UTF8 annotates a primitive, not a group");
    }

    /// Where a group's logical type and converted type name different structures, the logical
    /// type decides, as on a primitive, so the group is one structure and not two.
    @Test
    void aConvertedTypeContradictingTheGroupsLogicalTypeIsDropped() {
        SchemaNode.GroupNode group = group(LogicalType.map(), ConvertedType.LIST);

        assertThat(group.isMap()).isTrue();
        assertThat(group.isList()).isFalse();
        assertThat(group.convertedType()).isNull();
        assertThat(warnings.messages()).containsExactly(
                "Ignoring 1 annotation(s) their field cannot carry; each such field is read as though "
                        + "unannotated: g (LIST contradicts the group's MAP)");
    }

    /// A struct-shaped group holding one required `INT32`, annotated as given.
    private static SchemaNode.GroupNode group(LogicalType annotation, ConvertedType converted) {
        SchemaElement root = SchemaElement.group(ROOT, RepetitionType.REQUIRED, 1);
        SchemaElement group = new SchemaElement("g", null, null, RepetitionType.OPTIONAL, 1, converted,
                null, null, null, annotation);
        SchemaElement leaf = SchemaElement.primitive(COLUMN, PhysicalType.INT32, RepetitionType.REQUIRED);
        return (SchemaNode.GroupNode) FileSchema.fromSchemaElements(List.of(root, group, leaf))
                .getRootNode().children().getFirst();
    }

    private static SchemaElement leaf(PhysicalType type, Integer typeLength, LogicalType annotation) {
        return new SchemaElement(COLUMN, type, typeLength, RepetitionType.OPTIONAL,
                null, null, null, null, null, annotation);
    }

    private static LogicalType annotationOf(SchemaElement leaf) {
        SchemaElement root = SchemaElement.group(ROOT, RepetitionType.REQUIRED, 1);
        return FileSchema.fromSchemaElements(List.of(root, leaf)).getColumn(COLUMN).logicalType();
    }
}
