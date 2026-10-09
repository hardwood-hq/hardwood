/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.ToLongFunction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;

class InspectPagesCommandTest implements InspectPagesCommandContract {

    @Override
    public String plainFile() {
        return Cli.resourcePath("/plain_uncompressed.parquet");
    }

    @Override
    public String dictFile() {
        return Cli.resourcePath("/dictionary_uncompressed.parquet");
    }

    @Override
    public String pageIndexFile() {
        return Cli.resourcePath("/column_index_pushdown.parquet");
    }

    @Override
    public String longValueFile() {
        return Cli.resourcePath("/cli_long_value_test.parquet");
    }

    @Override
    public String nestedFile() {
        return Cli.resourcePath("/list_basic_test.parquet");
    }

    @Override
    public String nonexistentFile() {
        return "nonexistent.parquet";
    }

    /// Values, nulls and first-row indexes are counts, so they group in threes
    /// like every other count the CLI prints.
    @Test
    void groupsCountsWithThousandsSeparators() {
        Cli.Result result = Cli.launch("inspect", "pages", "-f",
                Cli.resourcePath("/misaligned_pages.parquet"), "-c", "narrow");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).contains(" 10,000 ").doesNotContain(" 10000 ");
    }

    /// An offset index the parser rejects is a damaged file, not a chunk without
    /// a page index: the command reports it, naming the chunk, rather than
    /// falling back to inline statistics.
    @Test
    void aDamagedOffsetIndexIsReported(@TempDir Path tempDir) throws IOException {
        Path damaged = damageFirstChunk(ColumnChunk::offsetIndexOffset, tempDir);

        Cli.Result result = Cli.launch("inspect", "pages", "-f", damaged.toString());

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).isEqualTo(
                "Error reading pages: [damaged.parquet: row group 0, column 'id'] OffsetIndex field 15 — Unknown field type: 15");
    }

    @Test
    void aDamagedColumnIndexIsReported(@TempDir Path tempDir) throws IOException {
        Path damaged = damageFirstChunk(ColumnChunk::columnIndexOffset, tempDir);

        Cli.Result result = Cli.launch("inspect", "pages", "-f", damaged.toString());

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).isEqualTo(
                "Error reading pages: [damaged.parquet: row group 0, column 'id'] ColumnIndex field 15 — Unknown field type: 15");
    }

    private Path damageFirstChunk(ToLongFunction<ColumnChunk> region, Path tempDir) throws IOException {
        Path source = Path.of(pageIndexFile());
        long offset;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(source))) {
            offset = region.applyAsLong(reader.getFileMetaData().rowGroups().get(0).columns().get(0));
        }
        return DamagedFiles.damage(source, offset, tempDir);
    }

    @Test
    void rejectsRemoteUri() {
        Cli.Result result = Cli.launch("inspect", "pages", "-f", "gs://bucket/data.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("not implemented yet");
    }
}
