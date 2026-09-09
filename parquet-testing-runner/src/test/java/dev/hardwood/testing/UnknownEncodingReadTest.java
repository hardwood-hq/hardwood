/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// End-to-end coverage for an unrecognized encoding, reading the `alp_extended.zstd.parquet`
/// fixture from apache/parquet-testing. Its ALP columns carry Thrift `Encoding` value 10, which
/// this release does not implement (#581), so they exercise the [Encoding#UNKNOWN] placeholder on
/// real bytes: the metadata must stay readable, sibling columns in supported encodings must still
/// decode, and the pages Hardwood cannot decode must be rejected with a message that names which
/// encoding was met.
///
/// The fixture is skipped by [ParquetComparisonTest] because no released parquet-java can read it
/// — its generated `Encoding` enum stops at `BYTE_STREAM_SPLIT` — so this is the only coverage the
/// file gets.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UnknownEncodingReadTest {

    /// Thrift `Encoding` value of ALP, as assigned in parquet-format.
    private static final int ALP_THRIFT_VALUE = 10;

    private static final String PLAIN_COLUMN = "double_plain";
    private static final String ALP_COLUMN = "double_alp_1024";

    private Path file;

    @BeforeAll
    void locateFixture() throws IOException {
        file = ParquetTestingRepoCloner.getTestFile("data/alp_extended.zstd.parquet");
    }

    @Test
    void unrecognizedEncodingLeavesMetadataReadable() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            FileMetaData metaData = reader.getFileMetaData();
            assertThat(metaData.rowGroups()).isNotEmpty();

            assertThat(chunkEncodings(metaData, ALP_COLUMN))
                    .as("ALP chunk encodings")
                    .contains(Encoding.UNKNOWN);
            assertThat(chunkEncodings(metaData, PLAIN_COLUMN))
                    .as("PLAIN chunk encodings")
                    .contains(Encoding.PLAIN)
                    .doesNotContain(Encoding.UNKNOWN);
        }
    }

    @Test
    void supportedSiblingColumnStillDecodes() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader columnReader = reader.columnReader(PLAIN_COLUMN)) {

            long values = 0;
            while (columnReader.nextBatch()) {
                values += columnReader.getRecordCount();
            }
            assertThat(values).isEqualTo(reader.getFileMetaData().numRows());
        }
    }

    @Test
    void unrecognizedEncodingIsRejectedByValue() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader columnReader = reader.columnReader(ALP_COLUMN)) {

            assertThatThrownBy(columnReader::nextBatch)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage("[alp_extended.zstd.parquet] Encoding not yet supported: UNKNOWN (Thrift "
                             + "encoding value 10)")
                    ;
        }
    }

    /// Encodings recorded for the named leaf column, taken from the first row group.
    private static List<Encoding> chunkEncodings(FileMetaData metaData, String columnName) {
        RowGroup rowGroup = metaData.rowGroups().getFirst();
        return rowGroup.columns().stream()
                .map(ColumnChunk::metaData)
                .filter(cmd -> cmd.pathInSchema().matchesDottedName(columnName))
                .map(ColumnMetaData::encodings)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No column chunk for " + columnName));
    }
}
