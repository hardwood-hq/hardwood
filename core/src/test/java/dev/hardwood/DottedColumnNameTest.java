/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Field names containing a dot, such as the `sepal.length` columns PyArrow writes for the iris
/// data set, are addressed by name like any other field (#794).
class DottedColumnNameTest {

    private static final int ROWS = 10;

    private static final String AMBIGUOUS =
            "Column name 'a.b' is ambiguous: it is the dot-separated path of more than one field in the schema";

    private static byte[] file;
    private static byte[] ambiguousFile;

    /// `sepal.length` is a flat column; `acme.info` is a struct holding `x.y`, so the path
    /// `acme.info.x.y` has two levels.
    @BeforeAll
    static void writeFile() throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("sepal.length", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .struct("acme.info", RepetitionType.REQUIRED, info -> info
                        .addColumn("x.y", PhysicalType.INT64, RepetitionType.REQUIRED))
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            for (int i = 0; i < ROWS; i++) {
                final long id = i;
                writer.rowWriter().writeRow(row -> row.setLong("id", id)
                        .setDouble("sepal.length", id * 0.5)
                        .setStruct("acme.info", info -> info.setLong("x.y", 100 + id)));
            }
        }
        file = InMemoryFiles.toByteArray(out);

        // `a` is null on odd rows.
        InMemoryOutputFile ambiguousOut = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(ambiguousOut, ambiguousSchema())) {
            for (int i = 0; i < ROWS; i++) {
                final long id = i;
                writer.rowWriter().writeRow(row -> {
                    row.setLong("id", id).setLong("a.b", 10 + id);
                    if (id % 2 == 0) {
                        row.setStruct("a", a -> a.setLong("b", 20 + id));
                    }
                    else {
                        row.setNull("a");
                    }
                });
            }
        }
        ambiguousFile = InMemoryFiles.toByteArray(ambiguousOut);
    }

    @Test
    void readsAFlatDottedColumnByName() throws Exception {
        try (ParquetFileReader reader = open();
             ColumnReader column = reader.buildColumnReader("sepal.length").build()) {
            assertThat(doubles(column)).containsExactly(0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5);
        }
    }

    @Test
    void readsAFlatDottedColumnByIndex() throws Exception {
        try (ParquetFileReader reader = open();
             ColumnReader column = reader.buildColumnReader(1).build()) {
            assertThat(doubles(column)).containsExactly(0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5);
        }
    }

    @Test
    void projectsDottedNamesAtEveryLevel() throws Exception {
        try (ParquetFileReader reader = open();
             ColumnReaders columns = reader.buildColumnReaders(
                     ColumnProjection.columns("acme.info.x.y", "sepal.length")).build()) {
            assertThat(columns.getColumnCount()).isEqualTo(2);
            assertThat(columns.getColumnReader(0).getColumnSchema().fieldPath())
                    .isEqualTo(FieldPath.of("acme.info", "x.y"));
            assertThat(columns.getColumnReader(1).getColumnSchema().fieldPath())
                    .isEqualTo(FieldPath.of("sepal.length"));
        }
    }

    @Test
    void projectsADottedGroupName() throws Exception {
        try (ParquetFileReader reader = open();
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("acme.info", "sepal.length")).build()) {
            assertThat(rows.getFieldCount()).isEqualTo(2);
            rows.next();
            rows.next();
            assertThat(rows.getStruct("acme.info").getLong("x.y")).isEqualTo(101L);
            assertThat(rows.getDouble("sepal.length")).isEqualTo(0.5);
        }
    }

    @Test
    void filtersOnDottedNames() throws Exception {
        assertThat(ids(FilterPredicate.gt("sepal.length", 3.0))).containsExactly(7L, 8L, 9L);
        assertThat(ids(FilterPredicate.lt("acme.info.x.y", 102L))).containsExactly(0L, 1L);
    }

    @Test
    void rejectsANameTwoFieldPathsJoinTo() {
        assertThatThrownBy(() -> ProjectedSchema.create(ambiguousSchema(), ColumnProjection.columns("a.b")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(AMBIGUOUS);
        assertThatThrownBy(() -> ambiguousSchema().getColumn("a.b"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(AMBIGUOUS);
        assertThat(ambiguousSchema().getColumn(FieldPath.of("a.b")).columnIndex()).isEqualTo(1);
        assertThat(ambiguousSchema().getColumn(FieldPath.of("a", "b")).columnIndex()).isEqualTo(2);
    }

    /// `a.b` names the leaf `a.b` and the group `b` in `a` alike.
    @Test
    void rejectsALeafPathThatIsAlsoAGroupPath() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("schema", 2),
                SchemaElement.primitive("a.b", PhysicalType.INT64, RepetitionType.REQUIRED),
                SchemaElement.group("a", RepetitionType.REQUIRED, 1),
                SchemaElement.group("b", RepetitionType.REQUIRED, 1),
                SchemaElement.primitive("c", PhysicalType.INT64, RepetitionType.REQUIRED)));

        assertThatThrownBy(() -> ProjectedSchema.create(schema, ColumnProjection.columns("a.b")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(AMBIGUOUS);
        assertThatThrownBy(() -> schema.getColumn("a.b"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(AMBIGUOUS);
        assertThat(schema.getColumn("a.b.c").columnIndex()).isEqualTo(1);
    }

    /// Readers of all columns stay apart by position when two of them share a dot-separated
    /// path, and looking either up by that path fails.
    @Test
    void readsColumnsTwoFieldPathsJoinToByPosition() throws Exception {
        try (ParquetFileReader reader = openAmbiguous();
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.all()).build()) {
            assertThat(columns.nextBatch()).isTrue();
            assertThat(columns.getColumnReader(1).getLongs()[0]).isEqualTo(10L);
            assertThat(columns.getColumnReader(2).getLongs()[0]).isEqualTo(20L);
            assertThatThrownBy(() -> columns.getColumnReader("a.b"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Column name 'a.b' is ambiguous: it is the dot-separated path of more than one requested column");
        }
    }

    @Test
    void readsColumnsTwoFieldPathsJoinToByColumnIndex() throws Exception {
        try (ParquetFileReader reader = openAmbiguous();
             ColumnReader flat = reader.buildColumnReader(1).build()) {
            assertThat(flat.getColumnSchema().fieldPath()).isEqualTo(FieldPath.of("a.b"));
            assertThat(flat.nextBatch()).isTrue();
            assertThat(flat.getLongs()[0]).isEqualTo(10L);
        }
        try (ParquetFileReader reader = openAmbiguous();
             ColumnReader nested = reader.buildColumnReader(2).build()) {
            assertThat(nested.getColumnSchema().fieldPath()).isEqualTo(FieldPath.of("a", "b"));
            assertThat(nested.nextBatch()).isTrue();
            assertThat(nested.getLongs()[0]).isEqualTo(20L);
        }
    }

    /// The predicate answers from a leaf of `a`, whose path `a.b` is shared with the flat
    /// column, and decodes that leaf without naming it.
    @Test
    void filtersOnAGroupHoldingALeafWhosePathAnotherColumnShares() throws Exception {
        List<Long> ids = new ArrayList<>();
        try (ParquetFileReader reader = openAmbiguous();
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("id"))
                     .filter(FilterPredicate.isNotNull("a")).build()) {
            while (rows.hasNext()) {
                rows.next();
                ids.add(rows.getLong("id"));
            }
        }
        assertThat(ids).containsExactly(0L, 2L, 4L, 6L, 8L);

        try (ParquetFileReader reader = openAmbiguous();
             ColumnReader column = reader.buildColumnReader("id").filter(FilterPredicate.isNull("a")).build()) {
            assertThat(longs(column)).containsExactly(1L, 3L, 5L, 7L, 9L);
        }
    }

    @Test
    void looksUpAFieldPathByItsElements() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("schema", 1),
                SchemaElement.primitive("a.b", PhysicalType.INT64, RepetitionType.REQUIRED)));

        assertThat(schema.getColumn("a.b").columnIndex()).isZero();
        assertThatThrownBy(() -> schema.getColumn(FieldPath.of("a", "b")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column not found: a.b");
    }

    @Test
    void matchesADottedNameAgainstDottedElements() {
        FieldPath path = FieldPath.of("acme.info", "x.y");

        assertThat(path.matchesDottedName("acme.info.x.y")).isTrue();
        assertThat(path.matchesDottedName("acme.info")).isTrue();
        assertThat(path.matchesDottedName("acme")).isFalse();
        assertThat(path.matchesDottedName("acme.info.x")).isFalse();
        assertThat(path.matchesDottedName("acme.info.")).isFalse();
    }

    private static List<Long> ids(FilterPredicate filter) throws Exception {
        try (ParquetFileReader reader = open();
             ColumnReader column = reader.buildColumnReader("id").filter(filter).build()) {
            return longs(column);
        }
    }

    private static List<Long> longs(ColumnReader column) throws IOException {
        List<Long> values = new ArrayList<>();
        while (column.nextBatch()) {
            long[] batch = column.getLongs();
            for (int i = 0; i < column.getRecordCount(); i++) {
                values.add(batch[i]);
            }
        }
        return values;
    }

    private static List<Double> doubles(ColumnReader column) throws IOException {
        List<Double> values = new ArrayList<>();
        while (column.nextBatch()) {
            double[] batch = column.getDoubles();
            for (int i = 0; i < column.getRecordCount(); i++) {
                values.add(batch[i]);
            }
        }
        return values;
    }

    private static ParquetFileReader open() throws IOException {
        return ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
    }

    private static ParquetFileReader openAmbiguous() throws IOException {
        return ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(ambiguousFile)));
    }

    /// `id`, a flat `a.b` and an optional struct `a` holding `b`: the paths of both `b` columns
    /// join to `a.b`.
    private static FileSchema ambiguousSchema() {
        return FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("a.b", PhysicalType.INT64, RepetitionType.REQUIRED)
                .struct("a", RepetitionType.OPTIONAL, a -> a
                        .addColumn("b", PhysicalType.INT64, RepetitionType.REQUIRED))
                .build();
    }
}
