/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Verifies [ProjectedSchema#createAugmented]: the columns a predicate needs are appended to
/// the projection, and every exposed column keeps the index it would have had without them.
/// See `_designs/ROW_READER_AUGMENTED_PROJECTION.md`.
class ProjectedSchemaAugmentedTest {

    /// Leaves in file order: `id`, `status`, `tags.list.element`, `meta.kind`,
    /// `labels.key_value.key`, `labels.key_value.value`.
    private static final Path NESTED = Paths.get("src/test/resources/enum_nested_test.parquet");

    @Test
    void appendsPredicateColumnsBehindTheProjectedOnes() throws Exception {
        FileSchema schema = schema();
        ProjectedSchema exposed = ProjectedSchema.create(schema, ColumnProjection.columns("status"), true);
        ProjectedSchema augmented = ProjectedSchema.createAugmented(
                schema, ColumnProjection.columns("status"), List.of("id"), true);

        assertThat(augmented.getProjectedColumnCount()).isEqualTo(2);
        assertThat(augmented.exposedColumnCount()).isEqualTo(1);
        assertThat(columnNames(augmented)).containsExactly("status", "id");
        // 'status' is at the index it holds without augmentation, although 'id' precedes it
        // in the file.
        assertThat(augmented.toProjectedIndex(originalIndexOf(schema, "status")))
                .isEqualTo(exposed.toProjectedIndex(originalIndexOf(schema, "status")))
                .isEqualTo(0);
    }

    @Test
    void keepsEveryExposedIndexWhenPredicateColumnsInterleave() throws Exception {
        FileSchema schema = schema();
        ColumnProjection projection = ColumnProjection.columns("status", "meta.kind");
        ProjectedSchema exposed = ProjectedSchema.create(schema, projection, true);
        // 'id' is leaf 0, so it sorts ahead of both projected leaves in file order.
        ProjectedSchema augmented = ProjectedSchema.createAugmented(schema, projection, List.of("id"), true);

        assertThat(augmented.exposedColumnCount()).isEqualTo(2);
        assertThat(augmented.exposedFieldCount()).isEqualTo(2);
        assertThat(columnNames(augmented)).containsExactly("status", "kind", "id");
        for (String column : List.of("status", "kind")) {
            int original = originalIndexOf(schema, column);
            assertThat(augmented.toProjectedIndex(original)).isEqualTo(exposed.toProjectedIndex(original));
        }
    }

    @Test
    void returnsThePlainProjectionWhenThePredicateAddsNoColumn() throws Exception {
        FileSchema schema = schema();
        ColumnProjection projection = ColumnProjection.columns("id", "status");
        ProjectedSchema augmented = ProjectedSchema.createAugmented(
                schema, projection, List.of("status"), true);

        assertThat(augmented.getProjectedColumnCount()).isEqualTo(2);
        assertThat(augmented.exposedColumnCount()).isEqualTo(2);
    }

    @Test
    void returnsThePlainProjectionWhenEveryColumnIsProjected() throws Exception {
        FileSchema schema = schema();
        ProjectedSchema augmented = ProjectedSchema.createAugmented(
                schema, ColumnProjection.all(), List.of("id"), true);

        assertThat(augmented.getProjectedColumnCount()).isEqualTo(schema.getColumnCount());
        assertThat(augmented.exposedColumnCount()).isEqualTo(schema.getColumnCount());
    }

    @Test
    void aPredicateLeafUnderAProjectedGroupLeavesTheFieldCountAlone() throws Exception {
        FileSchema schema = schema();
        // Both leaves live under the one top-level 'labels' map, so the augmentation adds a
        // column without adding a field.
        ProjectedSchema augmented = ProjectedSchema.createAugmented(
                schema, ColumnProjection.columns("meta.kind"), List.of("labels.key_value.value"), true);

        assertThat(augmented.exposedColumnCount()).isEqualTo(1);
        assertThat(augmented.exposedFieldCount()).isEqualTo(1);
        assertThat(augmented.getProjectedColumnCount()).isGreaterThan(1);
    }

    private static FileSchema schema() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NESTED))) {
            return reader.getFileSchema();
        }
    }

    private static int originalIndexOf(FileSchema schema, String leafName) {
        for (int i = 0; i < schema.getColumnCount(); i++) {
            if (schema.getColumn(i).name().equals(leafName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("No such leaf: " + leafName);
    }

    private static List<String> columnNames(ProjectedSchema projected) {
        return projected.getProjectedColumns().stream().map(column -> column.name()).toList();
    }
}
