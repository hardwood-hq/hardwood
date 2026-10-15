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
import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SizeStatistics;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.OutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;

import static org.assertj.core.api.Assertions.assertThat;

/// Hardwood and parquet-java, writing the same records, produce the same `SizeStatistics` for
/// every column chunk: the same fields present and the same counts in them. Readers that prune or
/// size their buffers from these statistics therefore decide the same way on either file.
/// parquet-java writes an empty struct for a chunk with nothing to count, where Hardwood writes
/// none; the two are compared as equal.
///
/// The per-page counts are held to the decoded pages by [ParquetJavaReader#assertSizeStatistics]
/// across the whole write gate; page boundaries differ between the two writers, so only the chunk
/// totals are compared here.
class WriterSizeStatisticsInteropTest {

    private static final int ROWS = 1_000;

    private static final FileSchema HARDWOOD_SCHEMA = FileSchema.builder("schema")
            .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
            .struct("address", RepetitionType.OPTIONAL, address -> address
                    .addColumn("city", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, LogicalType.string())
                    .addColumn("zip", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.string()))
            .list("scores", RepetitionType.OPTIONAL,
                    element -> element.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
            .build();

    private static final MessageType PARQUET_JAVA_SCHEMA = MessageTypeParser.parseMessageType("""
            message schema {
              required int32 id;
              optional group address {
                required binary city (STRING);
                optional binary zip (STRING);
              }
              optional group scores (LIST) {
                repeated group list {
                  optional int32 element;
                }
              }
            }""");

    @Test
    void matchesParquetJavaOnEveryColumnChunk(@TempDir Path dir) throws IOException {
        Path hardwood = dir.resolve("hardwood.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(hardwood), HARDWOOD_SCHEMA)) {
            RowWriter rows = writer.rowWriter();
            for (int r = 0; r < ROWS; r++) {
                int row = r;
                rows.writeRow(record -> {
                    record.setInt("id", row);
                    if (hasAddress(row)) {
                        record.setStruct("address", address -> address.setString("city", city(row))
                                .setString("zip", zip(row)));
                    }
                    if (hasScores(row)) {
                        record.setList("scores", scores -> {
                            for (int i = 0; i < scoreCount(row); i++) {
                                if (scoreIsNull(row, i)) {
                                    scores.addNull();
                                }
                                else {
                                    scores.addInt(row + i);
                                }
                            }
                        });
                    }
                });
            }
        }

        Path parquetJava = dir.resolve("parquet-java.parquet");
        SimpleGroupFactory groups = new SimpleGroupFactory(PARQUET_JAVA_SCHEMA);
        try (ParquetWriter<Group> writer = ExampleParquetWriter
                .builder(new org.apache.hadoop.fs.Path(parquetJava.toUri()))
                .withConf(HadoopConf.DEFAULTS)
                .withType(PARQUET_JAVA_SCHEMA)
                .build()) {
            for (int row = 0; row < ROWS; row++) {
                Group record = groups.newGroup().append("id", row);
                if (hasAddress(row)) {
                    Group address = record.addGroup("address").append("city", city(row));
                    if (zip(row) != null) {
                        address.append("zip", zip(row));
                    }
                }
                if (hasScores(row)) {
                    Group scores = record.addGroup("scores");
                    for (int i = 0; i < scoreCount(row); i++) {
                        Group entry = scores.addGroup("list");
                        if (!scoreIsNull(row, i)) {
                            entry.append("element", row + i);
                        }
                    }
                }
                writer.write(record);
            }
        }

        List<SizeStatistics> expected = sizeStatistics(parquetJava);
        assertThat(expected).as("parquet-java chunks").hasSize(4);
        assertThat(sizeStatistics(hardwood)).isEqualTo(expected);
    }

    /// Each column chunk's `SizeStatistics` as the raw footer carries it, `null` where it carries
    /// none or one with no field set.
    private static List<SizeStatistics> sizeStatistics(Path file) throws IOException {
        List<SizeStatistics> statistics = new ArrayList<>();
        for (RowGroup rowGroup : ParquetJavaReader.readFormatFooter(file).getRow_groups()) {
            for (ColumnChunk chunk : rowGroup.getColumns()) {
                SizeStatistics chunkStatistics = chunk.getMeta_data().getSize_statistics();
                statistics.add(chunkStatistics == null || chunkStatistics.equals(new SizeStatistics())
                        ? null
                        : chunkStatistics);
            }
        }
        return statistics;
    }

    private static boolean hasAddress(int row) {
        return row % 5 != 0;
    }

    private static String city(int row) {
        return "city-" + row;
    }

    /// `null` for a present address without a zip.
    private static String zip(int row) {
        return row % 3 == 0 ? null : "z" + (row % 17);
    }

    private static boolean hasScores(int row) {
        return row % 7 != 0;
    }

    /// Zero for an empty list.
    private static int scoreCount(int row) {
        return row % 7 == 1 ? 0 : row % 4;
    }

    private static boolean scoreIsNull(int row, int index) {
        return (row + index) % 5 == 0;
    }
}
