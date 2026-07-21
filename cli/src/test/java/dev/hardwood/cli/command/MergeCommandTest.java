/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;

class MergeCommandTest {
    @Test
    void mergesLocalFiles(@TempDir Path directory) throws Exception {
        Path first = directory.resolve("first.parquet");
        Path second = directory.resolve("second.parquet");
        Path output = directory.resolve("merged.parquet");
        write(first, 1, 2);
        write(second, 3, 4, 5);

        Cli.Result result = Cli.launch("merge", "--output", output.toString(),
                first.toString(), second.toString());

        assertThat(result.exitCode()).isZero();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(output))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(5);
            assertThat(reader.getFileMetaData().rowGroups()).hasSize(2);
        }
    }

    @Test
    void rejectsOutputUsedAsInput(@TempDir Path directory) throws Exception {
        Path file = directory.resolve("input.parquet");
        write(file, 1);

        Cli.Result result = Cli.launch("merge", "-o", file.toString(), file.toString());

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("Output must not also be an input");
    }

    private static void write(Path path, int... values) throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(path), schema)) {
            writer.writeBatch(batch -> batch.ints(0, values));
        }
    }
}
