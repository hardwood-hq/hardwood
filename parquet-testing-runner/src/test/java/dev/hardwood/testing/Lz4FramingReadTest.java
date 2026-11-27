/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// `hadoop_lz4_compressed.parquet` and `non_hadoop_lz4_compressed.parquet`: the same four rows,
/// every column chunk declaring the deprecated `LZ4` codec. The first frames each page the way
/// Hadoop's `Lz4Codec` does; the second holds each page as one raw LZ4 block, as Arrow C++ wrote
/// it before `LZ4_RAW` existed. A reader of `LZ4` meets both in files in the wild.
///
/// parquet-java's Avro reader cannot read either file (an empty field name in the schema of the
/// first, a decoding failure on the second), so [Utils] skips them in the comparison tests, and
/// this holds Hardwood to the values pyarrow reads from them.
class Lz4FramingReadTest {

    private static final List<String> EXPECTED = List.of(
            "1593604800 abc 42.0",
            "1593604800 def 7.7",
            "1593604801 abc 42.125",
            "1593604801 def 7.7");

    @ParameterizedTest
    @ValueSource(strings = { "hadoop_lz4_compressed.parquet", "non_hadoop_lz4_compressed.parquet" })
    void readsEitherLz4Framing(String fileName) throws IOException {
        List<String> rows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(ParquetTestingRepoCloner.getTestFile("data/" + fileName)))) {
            for (ColumnChunk chunk : reader.getFileMetaData().rowGroups().getFirst().columns()) {
                assertThat(chunk.metaData().codec()).isEqualTo(CompressionCodec.LZ4);
            }
            try (RowReader row = reader.rowReader()) {
                while (row.hasNext()) {
                    row.next();
                    rows.add(row.getLong("c0") + " "
                            + new String(row.getBinary("c1"), StandardCharsets.UTF_8) + " "
                            + row.getDouble("v11"));
                }
            }
        }

        assertThat(rows).containsExactlyElementsOf(EXPECTED);
    }
}
