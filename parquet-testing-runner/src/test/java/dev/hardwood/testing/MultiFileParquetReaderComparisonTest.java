/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.MultiFileColumnReaders;
import dev.hardwood.reader.MultiFileParquetReader;
import dev.hardwood.reader.MultiFileRowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/// Comparison tests that validate Hardwood's output generated with MultiFileParquetReader
/// against the reference parquet-java implementation by comparing parsed results row-by-row, field-by-field.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MultiFileParquetReaderComparisonTest {

    private Path repoDir;

    @BeforeAll
    void setUp() throws IOException {
        repoDir = ParquetTestingRepoCloner.ensureCloned();
        createGoodCFile();
    }

    private void createGoodCFile() throws IOException {
        // create a good file with same schema as ARROW-RS-GH-6229-LEVELS
        Path output = repoDir.resolve("data/good_c.parquet");
        if (Files.exists(output)) {
            return;
        }
        MessageType schema = MessageTypeParser.parseMessageType(
                "message schema { required int32 c; }");
        Configuration conf = new Configuration();
        org.apache.hadoop.fs.Path hadoopPath =
                new org.apache.hadoop.fs.Path(output.toUri());
        try (ParquetWriter<Group> writer = ExampleParquetWriter
                .builder(hadoopPath)
                .withConf(conf)
                .withType(schema)
                .build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            writer.write(factory.newGroup().append("c", 1));
            writer.write(factory.newGroup().append("c", 2));
            writer.write(factory.newGroup().append("c", 3));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("dev.hardwood.testing.Utils#parquetTestFiles")
    void compareWithReference(Path testFile) throws IOException {
        String fileName = testFile.getFileName().toString();

        assumeFalse(Utils.SKIPPED_FILES.contains(fileName),
                "Skipping " + fileName + " (in skip list)");

        List<GenericRecord> reference = Utils.readWithParquetJava(testFile);

        int rowIndex = 0;
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             MultiFileParquetReader mfReader = new MultiFileParquetReader(
                     List.of(InputFile.of(testFile)), context);
             MultiFileRowReader rowReader = mfReader.createRowReader()) {

            while (rowReader.hasNext()) {
                rowReader.next();
                Utils.compareRow(rowIndex, reference.get(rowIndex), rowReader);
                rowIndex++;
            }
        }

        assertThat(rowIndex).isEqualTo(reference.size());
    }

    @Test
    void compareMultiFileWithReference() throws IOException {
        Path dataDir = repoDir.resolve("data");

        Path fileA = dataDir.resolve("alltypes_plain.parquet");
        Path fileB = dataDir.resolve("alltypes_plain.snappy.parquet");

        // Reference: parquet-java, one file at a time, concatenated
        List<GenericRecord> reference = new ArrayList<>();
        reference.addAll(Utils.readWithParquetJava(fileA));
        reference.addAll(Utils.readWithParquetJava(fileB));

        // Hardwood: multi-file reader over same files in same order
        List<InputFile> inputs = List.of(InputFile.of(fileA), InputFile.of(fileB));

        int rowIndex = 0;
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             MultiFileParquetReader mfReader = new MultiFileParquetReader(inputs, context);
             MultiFileRowReader rowReader = mfReader.createRowReader()) {

            while (rowReader.hasNext()) {
                rowReader.next();
                Utils.compareRow(rowIndex, reference.get(rowIndex), rowReader);
                rowIndex++;
            }
        }

        assertThat(rowIndex).isEqualTo(reference.size());
    }

    @Test
    void emptyFileListThrowsException() {
        assertThatThrownBy(() ->
                new MultiFileParquetReader(List.of(), HardwoodContextImpl.create()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("At least one file must be provided");
    }

    @Test
    void singleFileBehaviourIsSameAsSingleFileReader() throws IOException {
        Path file = repoDir.resolve("data/alltypes_plain.parquet");

        List<GenericRecord> reference = Utils.readWithParquetJava(file);

        int rowIndex = 0;
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             MultiFileParquetReader mfReader = new MultiFileParquetReader(
                     List.of(InputFile.of(file)), context);
             MultiFileRowReader rowReader = mfReader.createRowReader()) {

            while (rowReader.hasNext()) {
                rowReader.next();
                Utils.compareRow(rowIndex, reference.get(rowIndex), rowReader);
                rowIndex++;
            }
        }

        assertThat(rowIndex).isEqualTo(reference.size());
    }

    @ParameterizedTest(name = "column: {0}")
    @MethodSource("dev.hardwood.testing.Utils#parquetTestFiles")
    void compareColumnsWithReference(Path testFile) throws IOException {
        String fileName = testFile.getFileName().toString();

        assumeFalse(Utils.SKIPPED_FILES.contains(fileName),
                "Skipping " + fileName + " (in skip list)");
        assumeFalse(Utils.COLUMN_SKIPPED_FILES.contains(fileName),
                "Skipping " + fileName + " (in column skip list)");

        List<GenericRecord> reference = Utils.readWithParquetJava(testFile);

        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             MultiFileParquetReader mfReader = new MultiFileParquetReader(
                     List.of(InputFile.of(testFile)), context);
             MultiFileColumnReaders columns = mfReader.createColumnReaders(ColumnProjection.all())) {

            FileSchema schema = mfReader.getFileSchema();
            for (int colIdx = 0; colIdx < schema.getColumnCount(); colIdx++) {
                ColumnSchema colSchema = schema.getColumn(colIdx);
                if (colSchema.maxRepetitionLevel() > 0) {
                    continue;
                }
                ColumnReader columnReader = columns.getColumnReader(colIdx);
                Utils.compareColumnReader(colSchema.name(), columnReader, reference);
            }
        }
    }

    @Test
    void rejectsBadFileWhenEncountered() throws IOException {
        Path good = repoDir.resolve("data/alltypes_plain.parquet");
        Path bad  = repoDir.resolve("bad_data/PARQUET-1481.parquet");
        Utils.assertBadDataRejected("PARQUET-1481.parquet",
                "Invalid or corrupt physical type value: -7",
                multiFileReadAction(good, bad));
    }

    @Test
    void rejectDictheader() throws IOException {
        Path good = repoDir.resolve("data/alltypes_plain.parquet");
        Path bad  = repoDir.resolve("bad_data/ARROW-RS-GH-6229-DICTHEADER.parquet");
        Utils.assertBadDataRejected("ARROW-RS-GH-6229-DICTHEADER.parquet",
                multiFileReadAction(good, bad));
    }

    @Test
    void rejectLevels() throws IOException {
        Path good = repoDir.resolve("data/good_c.parquet");
        Path bad  = repoDir.resolve("bad_data/ARROW-RS-GH-6229-LEVELS.parquet");
        Utils.assertBadDataRejected("ARROW-RS-GH-6229-LEVELS.parquet",
                "Column 'c' not found in file: ARROW-RS-GH-6229-LEVELS.parquet",
                multiFileReadAction(good, bad));
    }

    private ThrowableAssert.ThrowingCallable multiFileReadAction(Path good, Path bad) {
        return () -> {
            try (HardwoodContextImpl context = HardwoodContextImpl.create();
                 MultiFileParquetReader mfReader = new MultiFileParquetReader(
                         List.of(InputFile.of(good), InputFile.of(bad)), context);
                 MultiFileRowReader rowReader = mfReader.createRowReader()) {
                while (rowReader.hasNext()) {
                    rowReader.next();
                }
            }
        };
    }
}
