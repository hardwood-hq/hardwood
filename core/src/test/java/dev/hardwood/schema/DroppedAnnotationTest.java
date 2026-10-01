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

import dev.hardwood.internal.schema.LeafAnnotation;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;

import static org.assertj.core.api.Assertions.assertThat;

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

    /// A geospatial payload is WKB in a `BYTE_ARRAY`, the one physical type parquet-format allows it
    /// on, so either annotation is dropped off a column of any other type and kept on that one.
    @Test
    void aGeospatialAnnotationIsDroppedOffAnyPhysicalTypeButByteArray() {
        LogicalType geometry = LogicalType.geometry(null);

        SchemaElement onInt64 = leaf(PhysicalType.INT64, null, geometry);
        assertThat(annotationOf(onInt64)).isNull();
        assertThat(LeafAnnotation.dropFault(onInt64))
                .isEqualTo("GEOMETRY is read from BYTE_ARRAY, but the column is INT64");

        SchemaElement onByteArray = leaf(PhysicalType.BYTE_ARRAY, null, geometry);
        assertThat(annotationOf(onByteArray)).isEqualTo(geometry);
        assertThat(LeafAnnotation.dropFault(onByteArray)).isNull();

        SchemaElement geographyOnFixed = leaf(PhysicalType.FIXED_LEN_BYTE_ARRAY, 8,
                LogicalType.geography(null, null));
        assertThat(annotationOf(geographyOnFixed)).isNull();
        assertThat(LeafAnnotation.dropFault(geographyOnFixed))
                .isEqualTo("GEOGRAPHY is read from BYTE_ARRAY, but the column is FIXED_LEN_BYTE_ARRAY");
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
