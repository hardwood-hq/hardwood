/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.ParsedFooter;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A footer supplied by a [dev.hardwood.MetadataSource] passes the checks made on open when the
/// file was rewritten to the same length with a footer of the same length. Reading the rewritten
/// data then fails, and the failure says the footer was supplied.
class SuppliedFooterReadFailureTest {

    private static final Path FILE = Path.of("src/test/resources/plain_uncompressed.parquet");
    private static final Path PART_0 = Path.of("src/test/resources/multi_file_part0.parquet");
    private static final Path PART_1 = Path.of("src/test/resources/multi_file_part1.parquet");
    private static final String FAILURE =
            "[<memory>: row group 0, column 'id', page 0] PageHeader field 15 — Unknown field type: 15";
    private static final String HINT =
            " (footer supplied by the MetadataSource; the file may have changed since the footer was read)";

    @Test
    void aFileReadWithItsOwnFooterFailsAsItDoes() throws Exception {
        byte[] corrupt = corruptFirstPageHeader(Files.readAllBytes(FILE));

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(corrupt)))) {
            assertThatThrownBy(() -> readAllRows(reader))
                    .isExactlyInstanceOf(ParquetReadException.class)
                    .hasMessage(FAILURE);
        }
    }

    @Test
    void aRowReaderFailureOnAServedFileHintsAtTheSuppliedFooter() throws Exception {
        byte[] original = Files.readAllBytes(FILE);
        InputFile corrupt = InputFile.of(ByteBuffer.wrap(corruptFirstPageHeader(original)));
        ParsedFooter footer = footerOf(InputFile.of(ByteBuffer.wrap(original)));

        try (HardwoodContext context = contextServing(Map.of(corrupt, footer));
                ParquetFileReader reader = ParquetFileReader.open(corrupt, context)) {
            assertThatThrownBy(() -> readAllRows(reader))
                    .isExactlyInstanceOf(ParquetReadException.class)
                    .hasMessage(FAILURE + HINT);
        }
    }

    @Test
    void aColumnReaderFailureOnAServedFileHintsAtTheSuppliedFooter() throws Exception {
        byte[] original = Files.readAllBytes(FILE);
        InputFile corrupt = InputFile.of(ByteBuffer.wrap(corruptFirstPageHeader(original)));
        ParsedFooter footer = footerOf(InputFile.of(ByteBuffer.wrap(original)));

        try (HardwoodContext context = contextServing(Map.of(corrupt, footer));
                ParquetFileReader reader = ParquetFileReader.open(corrupt, context)) {
            assertThatThrownBy(() -> readFirstColumn(reader))
                    .isExactlyInstanceOf(ParquetReadException.class)
                    .hasMessage(FAILURE + HINT);
        }
    }

    @Test
    void aFailureOnALaterServedFileHintsAtTheSuppliedFooter() throws Exception {
        byte[] part1 = Files.readAllBytes(PART_1);
        InputFile first = InputFile.of(ByteBuffer.wrap(Files.readAllBytes(PART_0)));
        InputFile corrupt = InputFile.of(ByteBuffer.wrap(corruptFirstPageHeader(part1)));
        Map<InputFile, ParsedFooter> footers = new IdentityHashMap<>();
        footers.put(first, footerOf(InputFile.of(PART_0)));
        footers.put(corrupt, footerOf(InputFile.of(ByteBuffer.wrap(part1))));

        try (HardwoodContext context = contextServing(footers);
                ParquetFileReader reader = ParquetFileReader.openAll(List.of(first, corrupt), context)) {
            assertThatThrownBy(() -> readAllRows(reader))
                    .isExactlyInstanceOf(ParquetReadException.class)
                    .hasMessage(FAILURE + HINT);
            assertThatThrownBy(() -> readFirstColumn(reader))
                    .isExactlyInstanceOf(ParquetReadException.class)
                    .hasMessage(FAILURE + HINT);
        }
    }

    /// Overwrites the header of the first page of the first column chunk, leaving the file's
    /// length and its footer as they were.
    private static byte[] corruptFirstPageHeader(byte[] bytes) throws IOException {
        ColumnMetaData chunk = footerOf(InputFile.of(ByteBuffer.wrap(bytes))).metaData()
                .rowGroups().getFirst().columns().getFirst().metaData();
        long start = chunk.dictionaryPageOffset() != null ? chunk.dictionaryPageOffset() : chunk.dataPageOffset();
        byte[] corrupt = bytes.clone();
        Arrays.fill(corrupt, Math.toIntExact(start), Math.toIntExact(start + 16), (byte) 0xFF);
        return corrupt;
    }

    private static HardwoodContext contextServing(Map<InputFile, ParsedFooter> footers) {
        Map<InputFile, ParsedFooter> byInstance = new IdentityHashMap<>(footers);
        return HardwoodContext.builder().threads(2).metadataSource(byInstance::get).build();
    }

    private static ParsedFooter footerOf(InputFile file) throws IOException {
        try (file) {
            file.open();
            return ParsedFooter.readFrom(file);
        }
    }

    private static void readAllRows(ParquetFileReader reader) throws IOException {
        try (RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
            }
        }
    }

    private static void readFirstColumn(ParquetFileReader reader) throws IOException {
        String column = reader.getFileSchema().getColumn(0).name();
        try (ColumnReaders columns = reader.columnReaders(ColumnProjection.columns(column))) {
            while (columns.nextBatch()) {
                // reads to the end or to the failure
            }
        }
    }
}
