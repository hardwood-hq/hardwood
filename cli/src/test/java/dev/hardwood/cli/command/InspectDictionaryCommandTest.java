/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
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

    @Test
    void rejectsRemoteUri() {
        Cli.Result result = Cli.launch("inspect", "dictionary", "-f", "gs://bucket/data.parquet",
                "--column", "id");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("not implemented yet");
    }

    /// A dictionary page header declaring a body longer than its chunk is a broken file, and is
    /// reported as one rather than escaping as whatever slicing the short buffer raised.
    @Test
    void reportsADictionaryPageRunningPastItsChunkAsABrokenFile(@TempDir Path tempDir) throws IOException {
        Path source = Path.of(dictFile());
        ColumnChunk chunk;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(source))) {
            FileMetaData metaData = reader.getFileMetaData();
            int column = reader.getFileSchema().getColumn("category").columnIndex();
            chunk = metaData.rowGroups().get(0).columns().get(column);
        }
        long offset = chunk.metaData().dictionaryPageOffset();
        byte[] bytes = Files.readAllBytes(source);
        int declared = inflateCompressedPageSize(bytes, Math.toIntExact(offset));
        Path damaged = tempDir.resolve("damaged.parquet");
        Files.write(damaged, bytes);
        int headerSize = headerSize(bytes, Math.toIntExact(offset));

        Cli.Result result = Cli.launch("inspect", "dictionary", "-f", damaged.toString(), "--column", "category");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).isEqualTo("Error reading dictionary: [damaged.parquet: row group 0,"
                + " column 'category'] Malformed Parquet metadata: the dictionary page header declares "
                + (headerSize + declared) + " bytes but only " + chunk.metaData().totalCompressedSize()
                + " bytes remain in the chunk");
    }

    /// Rewrites the page header's `compressed_page_size` (its third field, after `type` and
    /// `uncompressed_page_size`) in place as the largest value its varint's width holds, so
    /// nothing after it moves.
    ///
    /// @return the value written
    private static int inflateCompressedPageSize(byte[] bytes, int headerOffset) {
        int position = headerOffset;
        for (int field = 0; field < 2; field++) {
            position++; // field header
            while ((bytes[position++] & 0x80) != 0) {
                // skip varint
            }
        }
        position++; // field header of compressed_page_size
        int width = 0;
        while ((bytes[position + width] & 0x80) != 0) {
            width++;
        }
        width++;
        long zigzag = 0;
        for (int i = 0; i < width; i++) {
            // The lowest group even, so the zigzag value decodes as positive.
            int group = i == 0 ? 0x7e : 0x7f;
            bytes[position + i] = (byte) (i == width - 1 ? group : group | 0x80);
            zigzag |= ((long) group) << (7 * i);
        }
        return Math.toIntExact(zigzag >>> 1);
    }

    private static int headerSize(byte[] bytes, int headerOffset) {
        ThriftCompactReader reader = new ThriftCompactReader(ByteBuffer.wrap(bytes), headerOffset);
        PageHeaderReader.read(reader);
        return reader.getBytesRead();
    }
}
