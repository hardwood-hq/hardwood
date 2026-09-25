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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Verifies [ReadProjection]: the columns a predicate needs are appended to the payload to form
/// the decoded columns, and every payload column keeps the index it has in the payload.
/// See `_designs-legacy/ROW_READER_AUGMENTED_PROJECTION.md`.
class ReadProjectionTest {

    /// Leaves in file order: `id`, `status`, `tags.list.element`, `meta.kind`,
    /// `labels.key_value.key`, `labels.key_value.value`.
    private static final Path NESTED = Paths.get("src/test/resources/enum_nested_test.parquet");

    @Test
    void appendsPredicateColumnsBehindThePayload() throws Exception {
        FileSchema schema = schema();
        ReadProjection read = ReadProjection.withPredicateColumns(
                schema, ColumnProjection.columns("status"), List.of("id"), true);

        assertThat(columnNames(read.payload())).containsExactly("status");
        // 'status' leads the decoded columns, although 'id' precedes it in the file.
        assertThat(columnNames(read.decoded())).containsExactly("status", "id");
        assertThat(read.payloadColumnCount()).isEqualTo(1);
        assertThat(read.isFilterOnly(0)).isFalse();
        assertThat(read.isFilterOnly(1)).isTrue();
    }

    @Test
    void thePayloadLeadsTheDecodedColumnsWhenPredicateColumnsInterleave() throws Exception {
        FileSchema schema = schema();
        // 'id' is leaf 0, so it sorts ahead of both projected leaves in file order.
        ReadProjection read = ReadProjection.withPredicateColumns(
                schema, ColumnProjection.columns("status", "meta.kind"), List.of("id"), true);

        assertThat(columnNames(read.payload())).containsExactly("status", "kind");
        assertThat(columnNames(read.decoded())).containsExactly("status", "kind", "id");
        for (int i = 0; i < read.payloadColumnCount(); i++) {
            assertThat(read.decoded().toOriginalIndex(i)).isEqualTo(read.payload().toOriginalIndex(i));
        }
        int[] payloadFields = read.payload().getProjectedFieldIndices();
        assertThat(read.decoded().getProjectedFieldIndices())
                .startsWith(payloadFields)
                .hasSize(payloadFields.length + 1);
    }

    @Test
    void decodesThePayloadAloneWhenThePredicateAddsNoColumn() throws Exception {
        ReadProjection read = ReadProjection.withPredicateColumns(
                schema(), ColumnProjection.columns("id", "status"), List.of("status"), true);

        assertThat(read.decoded()).isSameAs(read.payload());
        assertThat(read.payloadColumnCount()).isEqualTo(2);
    }

    @Test
    void decodesThePayloadAloneWhenEveryColumnIsProjected() throws Exception {
        FileSchema schema = schema();
        ReadProjection read = ReadProjection.withPredicateColumns(
                schema, ColumnProjection.all(), List.of("id"), true);

        assertThat(read.decoded()).isSameAs(read.payload());
        assertThat(read.payloadColumnCount()).isEqualTo(schema.getColumnCount());
    }

    @Test
    void aPredicateLeafUnderAProjectedGroupAddsAColumnButNoField() throws Exception {
        // Both leaves live under the one top-level 'labels' map, so the predicate adds a
        // column without adding a field.
        ReadProjection read = ReadProjection.withPredicateColumns(
                schema(), ColumnProjection.columns("labels.key_value.key"), List.of("labels.key_value.value"), true);

        assertThat(read.payload().getProjectedFieldIndices()).hasSize(1);
        assertThat(read.decoded().getProjectedFieldIndices()).hasSize(1);
        assertThat(read.decoded().getProjectedColumnCount()).isGreaterThan(read.payloadColumnCount());
    }

    @Test
    void rejectsADecodedProjectionThePayloadDoesNotLead() throws Exception {
        FileSchema schema = schema();
        ProjectedSchema status = ProjectedSchema.create(schema, ColumnProjection.columns("status"), true);
        // File order puts 'id' first, so 'status' does not lead this projection.
        ProjectedSchema idAndStatus = ProjectedSchema.create(schema, ColumnProjection.columns("id", "status"), true);

        assertThatThrownBy(() -> new ReadProjection(status, idAndStatus))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The payload projection does not lead the decoded one");
    }

    private static FileSchema schema() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NESTED))) {
            return reader.getFileSchema();
        }
    }

    private static List<String> columnNames(ProjectedSchema projected) {
        return projected.getProjectedColumns().stream().map(column -> column.name()).toList();
    }
}
