/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end read of a Parquet file larger than 2 GB, proving the per-region
/// memory mapping (#75) works through the full read stack.
///
/// Opt-in: generating a multi-GB file is slow and disk-heavy, so this runs only
/// with `-Dhardwood.largeFileTests=true`.
@Tag("large")
@EnabledIfSystemProperty(named = "hardwood.largeFileTests", matches = "true")
class LargeFileReadTest {

    private static final long TWO_GB = (long) Integer.MAX_VALUE + 1;

    @Test
    void readsFileLargerThanTwoGigabytes(@TempDir Path tmpDir) throws Exception {
        Path output = tmpDir.resolve("large.parquet");
        MessageType schema = MessageTypeParser.parseMessageType("message m { required int64 id; }");

        long written = 0;
        try (ParquetWriter<Group> writer = ExampleParquetWriter
                .builder(new org.apache.hadoop.fs.Path(output.toUri()))
                .withConf(new Configuration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(false) // PLAIN: ~8 bytes/row, so the file reliably exceeds 2 GB
                .build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            while (writer.getDataSize() <= TWO_GB + (64L << 20)) {
                for (int i = 0; i < 1_000_000; i++) {
                    writer.write(factory.newGroup().append("id", written));
                    written++;
                }
            }
        }

        long fileSize = Files.size(output);
        System.out.printf("LargeFileReadTest: wrote %,d rows, on-disk size %,d bytes (%.2f GB)%n",
                written, fileSize, fileSize / (double) (1L << 30));
        assertThat(fileSize)
                .as("on-disk file size must exceed 2 GB (%,d bytes), was %,d", TWO_GB, fileSize)
                .isGreaterThan(TWO_GB);

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(output))) {
            // Reading the footer alone exercises a read at an offset beyond 2 GB.
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(written);

            // The final rows live in the last row group, located past the 2 GB mark,
            // so reading them fetches a column-chunk region beyond Integer.MAX_VALUE.
            int tail = 5;
            try (RowReader rowReader = reader.buildRowReader().tail(tail).build()) {
                long expected = written - tail;
                int seen = 0;
                while (rowReader.hasNext()) {
                    rowReader.next();
                    assertThat(rowReader.getLong("id")).isEqualTo(expected);
                    expected++;
                    seen++;
                }
                assertThat(seen).isEqualTo(tail);
            }
        }
    }
}
