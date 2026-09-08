/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;

class InspectDictionaryCommandTest implements InspectDictionaryCommandContract {

    @Override
    public String plainFile() {
        return getClass().getResource("/plain_uncompressed.parquet").getPath();
    }

    @Override
    public String dictFile() {
        return getClass().getResource("/dictionary_uncompressed.parquet").getPath();
    }

    @Override
    public String longValueFile() {
        return getClass().getResource("/cli_long_value_test.parquet").getPath();
    }

    @Override
    public String nonexistentFile() {
        return "nonexistent.parquet";
    }

    @Test
    void requiresColumnOption() {
        Cli.Result result = Cli.launch("inspect", "dictionary", "-f", dictFile());

        assertThat(result.exitCode()).isNotZero();
    }

    @Test
    void rejectsNegativeLimit() {
        Cli.Result result = Cli.launch("inspect", "dictionary", "-f", dictFile(), "--column", "category",
                "--limit", "-1");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("--limit must be greater than or equal to 0");
    }

    /// This command reaches `DictionaryParser` directly rather than through the
    /// read path that names the file, row group and column, so a corrupt
    /// dictionary reported only what the parser knew — "CRC mismatch: expected
    /// … but computed …", with nothing saying where.
    @Test
    void aCorruptDictionaryNamesWhereItIs() throws Exception {
        byte[] bytes = Files.readAllBytes(Paths.get(dictFile()));
        long dictionaryStart;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(bytes)))) {
            ColumnMetaData meta = reader.getFileMetaData().rowGroups().getFirst().columns().stream()
                    .filter(c -> c.metaData().pathInSchema().toString().equals("category"))
                    .findFirst().orElseThrow().metaData();
            dictionaryStart = meta.dictionaryPageOffset();
        }
        // Make the dictionary page's header unparseable: 0x1f is a header for
        // field 1 declaring wire type 15, which is not a type. Corrupting the
        // page's data instead would go unnoticed — this dictionary is
        // uncompressed PLAIN and carries no CRC, so any bytes decode to
        // something.
        bytes[Math.toIntExact(dictionaryStart)] = 0x1f;
        Path corrupted = Files.createTempDirectory("hardwood-dict").resolve("corrupt.parquet");
        Files.write(corrupted, bytes);

        Cli.Result result = Cli.launch("inspect", "dictionary", "-f", corrupted.toString(),
                "--column", "category");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput())
                .contains("corrupt.parquet")
                .contains("row group 0")
                .contains("column category")
                .contains("dictionary page at byte " + dictionaryStart);
    }

    @Test
    void rejectsRemoteUri() {
        Cli.Result result = Cli.launch("inspect", "dictionary", "-f", "gs://bucket/data.parquet",
                "--column", "id");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("not implemented yet");
    }
}
