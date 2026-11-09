/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.reader.SchemaIncompatibleException;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Sibling fields of one name, which Parquet allows: every lookup by that name is refused as
/// ambiguous, and each field stays reachable by index. The siblings hold different values, so a
/// lookup that reached the wrong one would read another value.
class AmbiguousFieldNameTest {

    private static final Path TOP_LEVEL_COLUMNS = Paths.get("src/test/resources/duplicate_top_level_columns.parquet");
    private static final Path TOP_LEVEL_STRUCTS = Paths.get("src/test/resources/duplicate_top_level_structs.parquet");
    private static final Path STRUCT_FIELDS = Paths.get("src/test/resources/duplicate_struct_field_names.parquet");

    private static final String AMBIGUOUS_ID =
            "Field name 'id' is ambiguous: more than one field has it; access the field by index";

    @Test
    void flatReaderRefusesTheNameAndReachesEachColumnByIndex() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(TOP_LEVEL_COLUMNS));
             RowReader rows = reader.rowReader()) {
            rows.next();

            assertThat(rows.getLong(0)).isEqualTo(1L);
            assertThat(rows.getLong(1)).isEqualTo(10L);
            assertThatThrownBy(() -> rows.getLong("id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[duplicate_top_level_columns.parquet] " + AMBIGUOUS_ID);
            assertThatThrownBy(() -> rows.isNull("id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[duplicate_top_level_columns.parquet] " + AMBIGUOUS_ID);
        }
    }

    @Test
    void nestedReaderRefusesTheNameAndReachesEachFieldByIndex() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(TOP_LEVEL_STRUCTS));
             RowReader rows = reader.rowReader()) {
            rows.next();

            assertThat(rows.getStruct(1).getInt("a")).isEqualTo(1);
            assertThat(rows.getStruct(2).getStruct("g").getInt("b")).isEqualTo(5);
            assertThatThrownBy(() -> rows.getStruct("t"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[duplicate_top_level_structs.parquet] Field name 't' is ambiguous: more than one field has it; access the field by index");
        }
    }

    @Test
    void structRefusesTheNameAndReachesEachChildByIndex() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(STRUCT_FIELDS));
             RowReader rows = reader.rowReader()) {
            rows.next();
            PqStruct s = rows.getStruct("s");

            assertThat(s.getInt(0)).isEqualTo(3);
            assertThat(s.getString(1)).isEqualTo("a");
            assertThatThrownBy(() -> s.getInt("v"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Field name 'v' is ambiguous: more than one field has it; access the field by index");
        }
    }

    /// A projection selects a set of fields, not one of them: a name more than one field has
    /// selects all of them, in schema order, each reachable by index.
    @Test
    void projectionOfTheNameSelectsEveryField() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(TOP_LEVEL_COLUMNS));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("id")).build()) {
            rows.next();

            assertThat(rows.getFieldCount()).isEqualTo(2);
            assertThat(rows.getLong(0)).isEqualTo(1L);
            assertThat(rows.getLong(1)).isEqualTo(10L);
        }
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(STRUCT_FIELDS));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("s.v")).build()) {
            rows.next();
            PqStruct s = rows.getStruct(0);

            assertThat(s.getFieldCount()).isEqualTo(2);
            assertThat(s.getInt(0)).isEqualTo(3);
            assertThat(s.getString(1)).isEqualTo("a");
        }
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(TOP_LEVEL_COLUMNS));
             ColumnReaders columns = reader.columnReaders(ColumnProjection.columns("id"))) {
            columns.nextBatch();

            assertThat(columns.getColumnCount()).isEqualTo(2);
            assertThat(columns.getColumnReader(0).getLongs()[0]).isEqualTo(1L);
            assertThat(columns.getColumnReader(1).getLongs()[0]).isEqualTo(10L);
            assertThatThrownBy(() -> columns.getColumnReader("id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Column name 'id' is ambiguous: it is the dot-separated path of more than one requested column");
        }
    }

    /// A single column reader names one column, so the name stays ambiguous for it.
    @Test
    void columnReaderRefusesTheName() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(TOP_LEVEL_COLUMNS))) {
            assertThatThrownBy(() -> reader.buildColumnReader("id").build().close())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Column name 'id' is ambiguous: it is the dot-separated path of more than one field in the schema");
        }
    }

    @Test
    void schemaRefusesTheNameAndThePath() throws Exception {
        FileSchema structs;
        FileSchema fields;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(TOP_LEVEL_STRUCTS))) {
            structs = reader.getFileSchema();
        }
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(STRUCT_FIELDS))) {
            fields = reader.getFileSchema();
        }

        assertThatThrownBy(() -> structs.getField("t"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field name 't' is ambiguous: more than one top-level field has it");
        assertThatThrownBy(() -> fields.getColumn(FieldPath.of("s", "v")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column path 's.v' is ambiguous: it is the path of more than one column in the schema");
        assertThat(fields.getColumn(FieldPath.of("s", "w")).columnIndex()).isEqualTo(2);
    }

    /// `t.g.b` lies under the second `t` only, so a filter on it reaches one field, in either reader.
    @Test
    void filterUnderOneOfTwoSiblingsOfOneNameApplies() throws Exception {
        assertThat(rowIds(FilterPredicate.eq("t.g.b", 5))).containsExactly(1L);
        assertThat(columnIds(FilterPredicate.eq("t.g.b", 5))).containsExactly(1L);
        assertThat(rowIds(FilterPredicate.isNull("t.g"))).containsExactly(2L, 3L);
        assertThat(columnIds(FilterPredicate.isNull("t.g"))).containsExactly(2L, 3L);
    }

    /// `t.a` and `t.g.b` lie under two siblings named `t`, which a walk by name cannot tell apart.
    @Test
    void filterUnderTwoSiblingsOfOneNameIsRefused() throws Exception {
        FilterPredicate both = FilterPredicate.and(FilterPredicate.eq("t.a", 1), FilterPredicate.eq("t.g.b", 5));
        String message = "Cannot filter on columns 't.a' and 't.g.b': they lie under two sibling fields named 't'";

        assertThatThrownBy(() -> rowIds(both))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
        assertThatThrownBy(() -> columnIds(both))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
    }

    private static List<Long> rowIds(FilterPredicate filter) throws Exception {
        List<Long> ids = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(TOP_LEVEL_STRUCTS));
             RowReader rows = reader.buildRowReader().filter(filter).build()) {
            while (rows.hasNext()) {
                rows.next();
                ids.add(rows.getLong("id"));
            }
        }
        return ids;
    }

    private static List<Long> columnIds(FilterPredicate filter) throws Exception {
        List<Long> ids = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(TOP_LEVEL_STRUCTS));
             ColumnReader column = reader.buildColumnReader("id").filter(filter).build()) {
            while (column.nextBatch()) {
                long[] values = column.getLongs();
                for (int i = 0; i < column.getRecordCount(); i++) {
                    ids.add(values[i]);
                }
            }
        }
        return ids;
    }

    @Test
    void filterOnAnotherTopLevelColumnApplies() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(TOP_LEVEL_STRUCTS));
             RowReader rows = reader.buildRowReader().filter(FilterPredicate.eq("id", 2L)).build()) {
            List<Long> ids = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                ids.add(rows.getLong("id"));
            }

            assertThat(ids).containsExactly(2L);
        }
    }

    /// Each file of a multi-file read is matched to the first by path; the `k`-th column of a
    /// shared path matches the `k`-th.
    @Test
    void multiFileReadMatchesASharedPathByPosition(@TempDir Path tempDir) throws Exception {
        Path copy = tempDir.resolve("copy.parquet");
        Files.copy(STRUCT_FIELDS, copy);

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(STRUCT_FIELDS, copy));
             RowReader rows = parquet.rowReader()) {
            List<String> values = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                PqStruct s = rows.getStruct("s");
                values.add(s.getInt(0) + "/" + s.getString(1));
            }

            assertThat(values).containsExactly("3/a", "4/null", "3/a", "4/null");
        }
    }

    /// A later file holding another number of columns at a shared path is incompatible, whichever
    /// of the two files holds more.
    @Test
    void multiFileReadRefusesAnotherCountOfASharedPath(@TempDir Path tempDir) throws Exception {
        Path single = tempDir.resolve("single.parquet");
        FileSchema schema = FileSchema.builder("m")
                .struct("s", RepetitionType.REQUIRED, s -> s.addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(single), schema)) {
            writer.rowWriter().writeRow(row -> row.setStruct("s", s -> s.setInt("v", 7)));
        }

        assertThatThrownBy(() -> readAll(STRUCT_FIELDS, single))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessage("[single.parquet] Columns at path 's.v': 2 in the first file, 1 in this file");
        assertThatThrownBy(() -> readAll(single, STRUCT_FIELDS))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessage("[duplicate_struct_field_names.parquet] Columns at path 's.v': 1 in the first file, 2 in this file");
    }

    private static void readAll(Path first, Path second) throws Exception {
        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(first, second));
             RowReader rows = parquet.buildRowReader().projection(ColumnProjection.columns("s")).build()) {
            while (rows.hasNext()) {
                rows.next();
            }
        }
    }

    @Test
    void columnarWriterRefusesAnAmbiguousStructPath() throws Exception {
        FileSchema schema = FileSchema.builder("m")
                .struct("t", RepetitionType.OPTIONAL, t -> t.addColumn("a", PhysicalType.INT32, RepetitionType.REQUIRED))
                .struct("t", RepetitionType.OPTIONAL, t -> t.addColumn("b", PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.inMemory(), schema)) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch.struct("t",
                    Validity.ofNulls(new boolean[] { false }))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Column name 't' is ambiguous: it is the dot-separated path of more than one field in the schema");
        }
    }
}
