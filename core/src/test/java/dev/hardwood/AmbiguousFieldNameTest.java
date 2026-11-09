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
